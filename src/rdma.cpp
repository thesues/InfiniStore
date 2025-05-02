#include "rdma.h"

#include <string>

#include "log.h"

int close_rdma_device(struct rdma_device *rdma_dev) {
    if (rdma_dev->pd) {
        ibv_dealloc_pd(rdma_dev->pd);
    }
    if (rdma_dev->ib_ctx) {
        ibv_close_device(rdma_dev->ib_ctx);
    }
    return 0;
}

int destroy_rdma_context(struct rdma_context *ctx) {
    assert(ctx->rdma_dev != NULL, "destroy_rdma_context should be called before close_rdma_device");

    if (ctx->qp) {
        struct ibv_qp_attr attr;
        memset(&attr, 0, sizeof(attr));
        attr.qp_state = IBV_QPS_RESET;
        ibv_modify_qp(ctx->qp, &attr, IBV_QP_STATE);
        ibv_destroy_qp(ctx->qp);
    }

    if (ctx->cq) {
        ibv_destroy_cq(ctx->cq);
    }

    if (ctx->comp_channel) {
        ibv_destroy_comp_channel(ctx->comp_channel);
    }
    return 0;
}

int open_rdma_device(std::string dev_name, int ib_port, std::string link_type,
                     struct rdma_device *rdma_dev) {
    assert(link_type == "IB" || link_type == "Ethernet");
    assert(rdma_dev != NULL);

    rdma_dev->link_type = link_type;

    struct ibv_device **dev_list;
    struct ibv_device *ib_dev;
    int num_devices;
    dev_list = ibv_get_device_list(&num_devices);
    if (!dev_list) {
        ERROR("Failed to get RDMA devices list");
        return -1;
    }

    for (int i = 0; i < num_devices; ++i) {
        char *dev_name_from_list = (char *)ibv_get_device_name(dev_list[i]);
        if (strcmp(dev_name_from_list, dev_name.c_str()) == 0) {
            INFO("found device {}", dev_name_from_list);
            ib_dev = dev_list[i];
            rdma_dev->ib_ctx = ibv_open_device(ib_dev);
            break;
        }
    }

    if (!rdma_dev->ib_ctx) {
        INFO(
            "Can't find or failed to open the specified device, try to open "
            "the default device {}",
            (char *)ibv_get_device_name(dev_list[0]));
        rdma_dev->ib_ctx = ibv_open_device(dev_list[0]);
        if (!rdma_dev->ib_ctx) {
            ERROR("Failed to open the default device");
            return -1;
        }
    }

    struct ibv_port_attr port_attr;
    rdma_dev->ib_port = ib_port;
    if (ibv_query_port(rdma_dev->ib_ctx, rdma_dev->ib_port, &port_attr)) {
        ERROR("Unable to query port {} attributes\n", rdma_dev->ib_port);
        return -1;
    }
    if ((port_attr.link_layer == IBV_LINK_LAYER_INFINIBAND && link_type == "Ethernet") ||
        (port_attr.link_layer == IBV_LINK_LAYER_ETHERNET && link_type == "IB")) {
        ERROR("port link layer and config link type don't match");
        return -1;
    }
    if (port_attr.link_layer == IBV_LINK_LAYER_INFINIBAND) {
        rdma_dev->gid_index = -1;
    }
    else {
        rdma_dev->gid_index =
            ibv_find_sgid_type(rdma_dev->ib_ctx, rdma_dev->ib_port, IBV_GID_TYPE_ROCE_V2, AF_INET);
        if (rdma_dev->gid_index < 0) {
            ERROR("Failed to find GID");
            return -1;
        }
    }

    rdma_dev->lid = port_attr.lid;
    rdma_dev->active_mtu = port_attr.active_mtu;

    union ibv_gid gid;
    // get gid
    if (rdma_dev->gid_index != -1 &&
        ibv_query_gid(rdma_dev->ib_ctx, 1, rdma_dev->gid_index, &gid)) {
        ERROR("Failed to get GID");
        return -1;
    }

    rdma_dev->pd = ibv_alloc_pd(rdma_dev->ib_ctx);
    if (!rdma_dev->pd) {
        ERROR("Failed to allocate PD");
        return -1;
    }
}

int init_rdma_context(struct rdma_context *ctx, struct rdma_device *rdma_dev) {
    assert(ctx != NULL);
    assert(rdma_dev != NULL);

    // work like a weak_ptr
    ctx->rdma_dev = rdma_dev;

    ctx->comp_channel = ibv_create_comp_channel(rdma_dev->ib_ctx);
    if (!ctx->comp_channel) {
        ERROR("Failed to create completion channel");
        return -1;
    }

    // Create Completion Queue
    ctx->cq =
        ibv_create_cq(rdma_dev->ib_ctx, MAX_SEND_WR + MAX_RECV_WR, NULL, ctx->comp_channel, 0);
    if (!ctx->cq) {
        ERROR("Failed to create CQ");
        return -1;
    }

    if (ibv_req_notify_cq(ctx->cq, 0)) {
        ERROR("Failed to request CQ notification");
        return -1;
    }

    // Create Queue Pair
    struct ibv_qp_init_attr qp_init_attr = {};
    qp_init_attr.send_cq = ctx->cq;
    qp_init_attr.recv_cq = ctx->cq;
    qp_init_attr.qp_type = IBV_QPT_RC;  // Reliable Connection
    qp_init_attr.cap.max_send_wr = MAX_SEND_WR;
    qp_init_attr.cap.max_recv_wr = MAX_RECV_WR;
    qp_init_attr.cap.max_send_sge = 1;
    qp_init_attr.cap.max_recv_sge = 1;

    ctx->qp = ibv_create_qp(rdma_dev->pd, &qp_init_attr);
    if (!ctx->qp) {
        ERROR("Failed to create QP, {}", strerror(errno));
        return -1;
    }

    // Modify QP to INIT state
    if (modify_qp_to_init(ctx)) {
        ERROR("Failed to modify QP to INIT, {}", strerror(errno));
        return -1;
    }

    // save information to local_info for exchange data
    ctx->local_info.qpn = ctx->qp->qp_num;
    ctx->local_info.psn = lrand48() & 0xffffff;
    if (rdma_dev->gid_index != -1) {
        ctx->local_info.gid = gid;
    }

    ctx->local_info.lid = rdma_dev->lid;
    ctx->local_info.mtu = (uint32_t)rdma_dev->active_mtu;
    return 0;
}

int modify_qp_to_init(struct rdma_context *ctx) {
    struct ibv_qp_attr attr = {};
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = ctx->ib_port;
    attr.pkey_index = 0;
    attr.qp_access_flags =
        IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_LOCAL_WRITE;

    int flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;

    int ret = ibv_modify_qp(ctx->qp, &attr, flags);
    if (ret) {
        ERROR("Failed to modify QP to INIT");
        return ret;
    }
    return 0;
}

int modify_qp_to_rts(struct rdma_context *ctx) {
    struct ibv_qp_attr attr = {};
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 14;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    attr.sq_psn = ctx->local_info.psn;  // Use 0 or match with local PSN
    attr.max_rd_atomic = 16;

    int flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
                IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;

    int ret = ibv_modify_qp(ctx->qp, &attr, flags);
    if (ret) {
        ERROR("Failed to modify QP to RTS");
        return ret;
    }
    return 0;
}

int modify_qp_to_rtr(struct rdma_context *ctx) {
    struct ibv_qp_attr attr = {};
    attr.qp_state = IBV_QPS_RTR;

    // update MTU
    if (ctx->remote_info.mtu != (uint32_t)ctx->active_mtu) {
        INFO("remote MTU: {}, local MTU: {} is not the same, update to minimal MTU",
             1 << ((uint32_t)ctx->remote_info.mtu + 7), 1 << ((uint32_t)ctx->active_mtu + 7));
    }

    attr.path_mtu =
        (enum ibv_mtu)std::min((uint32_t)ctx->active_mtu, (uint32_t)ctx->remote_info.mtu);

    attr.dest_qp_num = ctx->remote_info.qpn;
    attr.rq_psn = ctx->remote_info.psn;
    attr.max_dest_rd_atomic = 16;
    attr.min_rnr_timer = 12;
    attr.ah_attr.dlid = 0;
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num = ctx->ib_port;

    if (ctx->gid_index == -1) {
        // IB
        attr.ah_attr.dlid = ctx->remote_info.lid;
        attr.ah_attr.is_global = 0;
    }
    else {
        // RoCE v2
        attr.ah_attr.is_global = 1;
        attr.ah_attr.grh.dgid = ctx->remote_info.gid;
        attr.ah_attr.grh.sgid_index = ctx->gid_index;  // local gid
        attr.ah_attr.grh.hop_limit = 1;
    }

    int flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;

    int ret = ibv_modify_qp(ctx->qp, &attr, flags);
    if (ret) {
        ERROR("Failed to modify QP to RTR");
        return ret;
    }
    return 0;
}
