#include "rdma_conn.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "log.h"
#include "utils.h"

// how long the destructor waits for the completion handler when the owner forgot
// to call stop(). see the comment in ~RdmaConnection().
static const std::chrono::seconds CQ_THREAD_EXIT_TIMEOUT(1);

SendBuffer::SendBuffer(struct ibv_pd *pd, size_t size) {
    if (posix_memalign(&buffer_, 4096, size) != 0) {
        assert(false);
    }
    mr_ = ibv_reg_mr(pd, buffer_, size, IBV_ACCESS_LOCAL_WRITE);
    assert(mr_ != NULL);
}

SendBuffer::~SendBuffer() {
    DEBUG("destroying send buffer");
    if (mr_) {
        ibv_dereg_mr(mr_);
        mr_ = nullptr;
    }
    if (buffer_) {
        free(buffer_);
        buffer_ = nullptr;
    }
}

int RdmaConnection::open(const std::string &dev_name, int ib_port, const std::string &link_type,
                         int hint_gid_index) {
    if (open_rdma_device(dev_name, ib_port, link_type, hint_gid_index, &rdma_dev_) < 0) {
        ERROR("Failed to open RDMA device");
        return -1;
    }

    if (init_rdma_context(&ctx_, &rdma_dev_) < 0) {
        ERROR("Failed to initialize RDMA context");
        return -1;
    }

    local_info_ = get_rdma_conn_info(&ctx_, &rdma_dev_);
    return 0;
}

rdma_conn_info_t RdmaConnection::local_info() { return local_info_; }

int RdmaConnection::connect(const rdma_conn_info_t &remote_info) {
    if (cq_thread_.joinable()) {
        ERROR("RDMA connection is already established");
        return -1;
    }

    remote_info_ = remote_info;

    print_rdma_conn_info(&remote_info_, true);
    print_rdma_conn_info(&local_info_, false);

    if (modify_qp_to_rtr(&ctx_, &rdma_dev_, &remote_info_)) {
        ERROR("Failed to modify QP to RTR");
        return -1;
    }

    if (modify_qp_to_rts(&ctx_)) {
        ERROR("Failed to modify QP to RTS");
        return -1;
    }

    /*
    This is MAX_RECV_WR not MAX_SEND_WR,
    because server also has the same number of buffers
    */
    for (int i = 0; i < MAX_RECV_WR; i++) {
        send_buffers_.push(new SendBuffer(rdma_dev_.pd, PROTOCOL_BUFFER_SIZE));
    }

    stop_ = false;
    cq_exited_ = std::promise<void>();
    cq_exited_future_ = cq_exited_.get_future();
    cq_thread_ = std::thread([this]() { cq_handler(); });
    return 0;
}

int RdmaConnection::wake_cq_thread() {
    if (ibv_req_notify_cq(ctx_.cq, 0)) {
        ERROR("Failed to request CQ notification");
        return -1;
    }

    // the buffer is not registered on purpose: the work request only has to fail
    // and produce a completion, that is what wakes the handler up.
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)this;
    sge.length = sizeof(*this);
    sge.lkey = 0;

    struct ibv_send_wr send_wr;
    memset(&send_wr, 0, sizeof(send_wr));
    send_wr.wr_id = (uintptr_t)this;
    send_wr.sg_list = &sge;
    send_wr.num_sge = 1;
    send_wr.opcode = IBV_WR_SEND;
    send_wr.send_flags = IBV_SEND_SIGNALED;

    struct ibv_send_wr *bad_send_wr;
    int ret = ibv_post_send(ctx_.qp, &send_wr, &bad_send_wr);
    if (ret) {
        // without a completion the handler stays blocked in ibv_get_cq_event
        ERROR("Failed to wake up cq thread: {}", strerror(ret));
        return -1;
    }
    return 0;
}

void RdmaConnection::stop() {
    if (!cq_thread_.joinable()) {
        return;
    }

    if (!stop_.exchange(true) && !cq_thread_exited()) {
        wake_cq_thread();
    }
    cq_thread_.join();
}

bool RdmaConnection::cq_thread_exited() {
    return cq_exited_future_.valid() &&
           cq_exited_future_.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
}

RdmaConnection::~RdmaConnection() {
    if (cq_thread_.joinable()) {
        /*
        stop() should have been called by now. Give the thread a bounded chance to
        come back anyway, and if it does not, leak the rdma resources instead of
        destroying them while the thread is still using them: a leak is bounded,
        a use after free is not.
        */
        WARN("stop() was not called before destroying the rdma connection");
        if (!stop_.exchange(true) && !cq_thread_exited()) {
            wake_cq_thread();
        }
        if (cq_exited_future_.valid() &&
            cq_exited_future_.wait_for(CQ_THREAD_EXIT_TIMEOUT) != std::future_status::ready) {
            ERROR("cq thread did not exit, leaking the rdma resources of this connection");
            cq_thread_.detach();
            return;
        }
        cq_thread_.join();
    }

    SendBuffer *buffer;
    while (send_buffers_.pop(buffer)) {
        delete buffer;
    }

    for (auto it = local_mr_.begin(); it != local_mr_.end(); it++) {
        ibv_dereg_mr(it->second);
    }
    local_mr_.clear();

    destroy_rdma_context(&ctx_);
    close_rdma_device(&rdma_dev_);
}

SendBuffer *RdmaConnection::get_send_buffer() {
    /*
    if the send buffer list is empty we just report the error and return NULL,
    normal users should not have that many inflight requests.
    */
    SendBuffer *buffer = NULL;
    if (!send_buffers_.pop(buffer)) {
        ERROR("No send buffer available, too many inflight requests");
        return NULL;
    }
    return buffer;
}

void RdmaConnection::release_send_buffer(SendBuffer *buffer) { send_buffers_.push(buffer); }

void RdmaConnection::post_recv_ack(rdma_info_base *info) {
    struct ibv_recv_wr recv_wr = {};
    struct ibv_recv_wr *bad_recv_wr = NULL;

    recv_wr.wr_id = (uintptr_t)info;

    recv_wr.next = NULL;
    recv_wr.sg_list = NULL;
    recv_wr.num_sge = 0;

    int ret = ibv_post_recv(ctx_.qp, &recv_wr, &bad_recv_wr);
    if (ret) {
        ERROR("Failed to post recv wr :{}", strerror(ret));
    }
}

int RdmaConnection::post_meta_request(const std::vector<std::string> &keys,
                                      const std::vector<size_t> &offsets, int block_size,
                                      void *base_ptr, char op, rdma_info_base *info) {
    assert(base_ptr != NULL);
    assert(offsets.size() == keys.size());

    auto mr_it = local_mr_.find((uintptr_t)base_ptr);
    if (mr_it == local_mr_.end()) {
        ERROR("Please register memory first {}", (uintptr_t)base_ptr);
        delete info;
        return -1;
    }
    struct ibv_mr *mr = mr_it->second;

    SendBuffer *send_buffer = get_send_buffer();
    if (send_buffer == NULL) {
        delete info;
        return -1;
    }

    std::vector<unsigned long> remote_addrs;
    remote_addrs.reserve(offsets.size());
    for (size_t i = 0; i < offsets.size(); i++) {
        remote_addrs.push_back((unsigned long)base_ptr + offsets[i]);
    }

    FixedBufferAllocator allocator(send_buffer->buffer_, PROTOCOL_BUFFER_SIZE);
    FlatBufferBuilder builder(64 << 10, &allocator);
    auto keys_offset = builder.CreateVectorOfStrings(keys);
    auto remote_addrs_offset = builder.CreateVector(remote_addrs);
    auto req = CreateRemoteMetaRequest(builder, keys_offset, block_size, mr->rkey,
                                       remote_addrs_offset, op);
    builder.Finish(req);

    // the server acks through this recv, so it has to be posted before the request
    post_recv_ack(info);

    struct ibv_sge sge = {};
    sge.addr = (uintptr_t)builder.GetBufferPointer();
    sge.length = builder.GetSize();
    sge.lkey = send_buffer->mr_->lkey;

    struct ibv_send_wr wr = {};
    struct ibv_send_wr *bad_wr = NULL;
    wr.wr_id = (uintptr_t)send_buffer;
    wr.opcode = IBV_WR_SEND;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;

    int ret = ibv_post_send(ctx_.qp, &wr, &bad_wr);
    if (ret) {
        ERROR("Failed to post RDMA send :{}", strerror(ret));
        release_send_buffer(send_buffer);
        // info stays posted as a recv wr, the completion handler owns it now
        return -1;
    }

    return 0;
}

int RdmaConnection::post_write(const std::vector<std::string> &keys,
                               const std::vector<size_t> &offsets, int block_size, void *base_ptr,
                               std::function<void(int)> callback) {
    return post_meta_request(keys, offsets, block_size, base_ptr, OP_RDMA_WRITE,
                             new rdma_write_info(callback));
}

int RdmaConnection::post_read(const std::vector<std::string> &keys,
                              const std::vector<size_t> &offsets, int block_size, void *base_ptr,
                              std::function<void(unsigned int)> callback) {
    return post_meta_request(keys, offsets, block_size, base_ptr, OP_RDMA_READ,
                             new rdma_read_info(callback));
}

int RdmaConnection::register_mr(void *base_ptr, size_t ptr_region_size) {
    assert(base_ptr != NULL);
    if (local_mr_.count((uintptr_t)base_ptr)) {
        WARN("this memory address is already registered!");
        ibv_dereg_mr(local_mr_[(uintptr_t)base_ptr]);
    }
    struct ibv_mr *mr;
    mr = ibv_reg_mr(rdma_dev_.pd, base_ptr, ptr_region_size,
                    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    if (!mr) {
        ERROR("Failed to register memory regions, size: {}", ptr_region_size);
        return -1;
    }
    INFO("register mr done for base_ptr: {}, size: {}", (uintptr_t)base_ptr, ptr_region_size);
    local_mr_[(uintptr_t)base_ptr] = mr;
    return 0;
}

void RdmaConnection::cq_handler() {
    assert(ctx_.comp_channel != NULL);

    while (!stop_) {
        struct ibv_cq *ev_cq;
        void *ev_ctx;
        int ret = ibv_get_cq_event(ctx_.comp_channel, &ev_cq, &ev_ctx);
        if (ret != 0) {
            // TODO: graceful shutdown
            if (errno != EINTR) {
                WARN("Failed to get CQ event {}", strerror(errno));
                break;
            }
            continue;
        }

        ibv_ack_cq_events(ev_cq, 1);
        if (ibv_req_notify_cq(ev_cq, 0)) {
            ERROR("Failed to request CQ notification");
            break;
        }

        struct ibv_wc wc[10] = {};
        int num_completions;
        bool done = false;
        while (!done && (num_completions = ibv_poll_cq(ctx_.cq, 10, wc)) && num_completions > 0) {
            for (int i = 0; i < num_completions; i++) {
                if (wc[i].status != IBV_WC_SUCCESS) {
                    // only the wake up wr uses IBV_WC_SEND, see wake_cq_thread()
                    if (wc[i].opcode == IBV_WC_SEND) {
                        INFO("cq thread exit");
                    }
                    else {
                        ERROR("Failed status: {}", ibv_wc_status_str(wc[i].status));
                    }
                    done = true;
                    break;
                }

                if (wc[i].opcode == IBV_WC_SEND) {
                    // read cache/allocate msg/commit msg: request sent
                    DEBUG("read cache/allocated/commit msg request send {}, ",
                          (uintptr_t)wc[i].wr_id);
                    release_send_buffer((SendBuffer *)wc[i].wr_id);
                }
                else if (wc[i].opcode == IBV_WC_RECV) {  // allocate msg recved.
                    rdma_info_base *ptr = reinterpret_cast<rdma_info_base *>(wc[i].wr_id);
                    switch (ptr->get_wr_type()) {
                        case WrType::RDMA_READ_ACK: {
                            DEBUG("read cache done: Received IMM, imm_data: {}", wc[i].imm_data);
                            auto *info = reinterpret_cast<rdma_read_info *>(ptr);
                            info->callback(wc[i].imm_data);
                            delete info;
                            break;
                        }
                        case WrType::RDMA_WRITE_ACK: {
                            DEBUG("RDMA write cache done: Received IMM, imm_data: {}",
                                  wc[i].imm_data);
                            auto *info = reinterpret_cast<rdma_write_info *>(ptr);
                            info->callback(wc[i].imm_data);
                            DEBUG("RDMA_WRITE_ACK callback done");
                            delete info;
                            break;
                        }
                        default:
                            ERROR("Unexpected wr type: {}", (int)ptr->get_wr_type());
                            done = true;
                            break;
                    }
                }
                else {
                    ERROR("Unexpected opcode: {}", (int)wc[i].opcode);
                    done = true;
                    break;
                }
            }
        }

        if (done) {
            break;
        }
    }

    // let the destructor know the thread is on its way out
    cq_exited_.set_value();
}
