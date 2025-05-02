#ifndef RDMA_H
#define RDMA_H
#include <infiniband/verbs.h>

#include <string>

#include "ibv_helper.h"
#include "protocol.h"

struct rdma_device {
    struct ibv_context *ib_ctx;
    struct ibv_pd *pd;
    int ib_port;
    int gid_index;
    int lid;
    std::string link_type;  // IB or Ethernet
    ibv_mtu active_mtu;
};

struct rdma_context {
    struct rdma_device *rdma_dev;

    struct ibv_comp_channel *comp_channel;
    struct ibv_cq *cq;
    struct ibv_qp *qp;

    struct rdma_conn_info_t local_info;
    struct rdma_conn_info_t remote_info;
};

// int init_rdma_context(std::string dev_name, int ib_port, std::string link_type,
//                                                 struct rdma_context *ctx);

int open_rdma_device(std::string dev_name, int ib_port, std::string link_type,
                     struct rdma_device *rdma_dev);
int init_rdma_context(std::string dev_name, int ib_port, std::string link_type,
                      struct rdma_device *rdma_dev, struct rdma_context *ctx);
int modify_qp_to_init(struct rdma_context *ctx);
int modify_qp_to_rts(struct rdma_context *ctx);
int modify_qp_to_rtr(struct rdma_context *ctx);

int destroy_rdma_context(struct rdma_context *ctx);
int close_rdma_device(struct rdma_device *rdma_dev);

#endif
