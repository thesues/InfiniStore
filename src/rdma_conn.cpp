#include "rdma_conn.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "log.h"
#include "utils.h"

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

int RdmaConnection::connect(uv_loop_t *loop, const rdma_conn_info_t &remote_info) {
    if (poll_handle_ != NULL) {
        ERROR("RDMA connection is already established");
        return -1;
    }
    if (loop == NULL) {
        ERROR("No event loop given");
        return -1;
    }

    remote_info_ = remote_info;
    loop_thread_ = uv_thread_self();

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
        send_buffers_.push_back(new SendBuffer(rdma_dev_.pd, PROTOCOL_BUFFER_SIZE));
    }

    if (ibv_req_notify_cq(ctx_.cq, 0)) {
        ERROR("Failed to request CQ notification");
        return -1;
    }

    poll_handle_ = (uv_poll_t *)malloc(sizeof(uv_poll_t));
    if (poll_handle_ == NULL) {
        ERROR("Failed to allocate poll handle");
        return -1;
    }

    if (uv_poll_init(loop, poll_handle_, ctx_.comp_channel->fd) < 0) {
        ERROR("Failed to init poll handle");
        free(poll_handle_);
        poll_handle_ = NULL;
        return -1;
    }
    poll_handle_->data = this;
    uv_poll_start(poll_handle_, UV_READABLE, poll_cb);
    return 0;
}

void RdmaConnection::stop() {
    if (poll_handle_ == NULL) {
        return;
    }

    /*
    uv_close stops the watcher and unregisters the fd synchronously, so the
    completion channel can be destroyed right after. The handle itself is freed
    by the loop later, in the close callback.
    */
    poll_handle_->data = NULL;
    uv_close((uv_handle_t *)poll_handle_, [](uv_handle_t *handle) { free(handle); });
    poll_handle_ = NULL;
}

RdmaConnection::~RdmaConnection() {
    stop();

    for (auto *buffer : send_buffers_) {
        delete buffer;
    }
    send_buffers_.clear();

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
    if (send_buffers_.empty()) {
        ERROR("No send buffer available, too many inflight requests");
        return NULL;
    }
    SendBuffer *buffer = send_buffers_.front();
    send_buffers_.pop_front();
    return buffer;
}

void RdmaConnection::release_send_buffer(SendBuffer *buffer) {
    send_buffers_.push_back(buffer);
}

bool RdmaConnection::on_loop_thread() const {
    uv_thread_t self = uv_thread_self();
    return uv_thread_equal(&loop_thread_, &self) != 0;
}

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

    if (poll_handle_ == NULL) {
        ERROR("the rdma connection is not established");
        delete info;
        return -1;
    }

    /*
    The send buffers and the completion handling are single threaded by design,
    everything runs on the loop. Reject the call instead of corrupting them.
    */
    if (!on_loop_thread()) {
        ERROR("the connection is used from a thread other than the one running its loop");
        delete info;
        return -1;
    }

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

void RdmaConnection::poll_cb(uv_poll_t *handle, int status, int events) {
    (void)events;

    RdmaConnection *self = (RdmaConnection *)handle->data;
    if (self == NULL) {
        return;
    }
    if (status < 0) {
        ERROR("Poll error: {}", uv_strerror(status));
        return;
    }
    self->poll_cq();
}

void RdmaConnection::poll_cq() {
    struct ibv_cq *ev_cq;
    void *ev_ctx;

    if (ibv_get_cq_event(ctx_.comp_channel, &ev_cq, &ev_ctx) != 0) {
        ERROR("Failed to get CQ event");
        return;
    }
    ibv_ack_cq_events(ev_cq, 1);

    if (ibv_req_notify_cq(ev_cq, 0) != 0) {
        ERROR("Failed to request CQ notification");
        return;
    }

    struct ibv_wc wc[10] = {};
    int num_completions;
    while ((num_completions = ibv_poll_cq(ctx_.cq, 10, wc)) > 0) {
        for (int i = 0; i < num_completions; i++) {
            if (wc[i].status != IBV_WC_SUCCESS) {
                ERROR("Failed status: {}", ibv_wc_status_str(wc[i].status));
                return;
            }

            if (wc[i].opcode == IBV_WC_SEND) {
                // read cache/allocate msg/commit msg: request sent
                DEBUG("read cache/allocated/commit msg request send {}, ", (uintptr_t)wc[i].wr_id);
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
                        DEBUG("RDMA write cache done: Received IMM, imm_data: {}", wc[i].imm_data);
                        auto *info = reinterpret_cast<rdma_write_info *>(ptr);
                        info->callback(wc[i].imm_data);
                        DEBUG("RDMA_WRITE_ACK callback done");
                        delete info;
                        break;
                    }
                    default:
                        ERROR("Unexpected wr type: {}", (int)ptr->get_wr_type());
                        return;
                }
            }
            else {
                ERROR("Unexpected opcode: {}", (int)wc[i].opcode);
                return;
            }
        }
    }
}
