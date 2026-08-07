#ifndef RDMA_CONN_H
#define RDMA_CONN_H

#include <uv.h>

#include <deque>
#include <functional>
#include <string>
#include <unordered_map>
#include <vector>

#include "config.h"
#include "protocol.h"
#include "rdma.h"

// RDMA send buffer
// because write_cache will be invoked asynchronously,
// so each request will have a standalone send buffer.
struct SendBuffer {
    void *buffer_ = NULL;
    struct ibv_mr *mr_ = NULL;

    SendBuffer(struct ibv_pd *pd, size_t size);
    SendBuffer(const SendBuffer &) = delete;
    ~SendBuffer();
};

enum class WrType {
    BASE,
    RDMA_READ_ACK,
    RDMA_WRITE_ACK,
};

struct rdma_info_base {
   protected:
    WrType wr_type;

   public:
    rdma_info_base(WrType wr_type) : wr_type(wr_type) {}
    virtual ~rdma_info_base() = default;
    WrType get_wr_type() const { return wr_type; }
};

struct rdma_write_info : rdma_info_base {
    std::function<void(int)> callback;
    rdma_write_info(std::function<void(int)> callback)
        : rdma_info_base(WrType::RDMA_WRITE_ACK), callback(callback) {}
};

struct rdma_read_info : rdma_info_base {
    // call back function.
    std::function<void(unsigned int)> callback;
    rdma_read_info(std::function<void(unsigned int)> callback)
        : rdma_info_base(WrType::RDMA_READ_ACK), callback(callback) {}
};

/*
The client side RDMA connection: device, QP, memory regions and send buffers. It
does not do any TCP/protocol work, the owner(Connection) carries the connection
info of both sides over its own socket and hands it over through
local_info()/connect().

The completion queue is watched by the caller's libuv loop, the same way the
server does it, so everything here runs on the loop thread and no state is
shared across threads.
*/
class RdmaConnection {
   public:
    RdmaConnection() = default;
    RdmaConnection(const RdmaConnection &) = delete;
    ~RdmaConnection();

    // open the device and create the QP. local_info() is valid afterwards.
    int open(const std::string &dev_name, int ib_port, const std::string &link_type,
             int hint_gid_index);

    rdma_conn_info_t local_info();

    // bring the QP up against the remote side and start watching the completion
    // channel on the given loop
    int connect(uv_loop_t *loop, const rdma_conn_info_t &remote_info);

    // stop watching the completion queue, idempotent
    void stop();

    /*
    Register a memory region. This pins the pages, it takes a while for a large
    region and it is synchronous, so it is the one call which is meant to be made
    off the loop thread. It must not overlap with in flight requests on this
    connection though, they read the same map from the loop.
    */
    int register_mr(void *base_ptr, size_t ptr_region_size);

    int post_read(const std::vector<std::string> &keys, const std::vector<size_t> &offsets,
                  int block_size, void *base_ptr, std::function<void(unsigned int)> callback);
    int post_write(const std::vector<std::string> &keys, const std::vector<size_t> &offsets,
                   int block_size, void *base_ptr, std::function<void(int)> callback);

   private:
    // build a RemoteMetaRequest and send it over the QP. Takes ownership of info:
    // it is deleted here on failure, and by the completion handler on success.
    int post_meta_request(const std::vector<std::string> &keys,
                          const std::vector<size_t> &offsets, int block_size, void *base_ptr,
                          char op, rdma_info_base *info);
    void post_recv_ack(rdma_info_base *info);
    // true when the caller is on the loop this connection runs on
    bool on_loop_thread() const;
    // one completion channel event: ack it, rearm and drain the CQ
    void poll_cq();
    static void poll_cb(uv_poll_t *handle, int status, int events);

    SendBuffer *get_send_buffer();
    void release_send_buffer(SendBuffer *buffer);

    struct rdma_device rdma_dev_;
    struct rdma_context ctx_;

    rdma_conn_info_t local_info_ = {};
    rdma_conn_info_t remote_info_ = {};

    std::unordered_map<uintptr_t, struct ibv_mr *> local_mr_;

    /*
    This is MAX_RECV_WR not MAX_SEND_WR,
    because server also has the same number of buffers
    */
    std::deque<SendBuffer *> send_buffers_;

    /*
    Watches the completion channel. Heap allocated: libuv needs the memory of a
    handle to stay valid until its close callback has run, which is after this
    object may be gone.
    */
    uv_poll_t *poll_handle_ = NULL;
    // the thread which runs the loop, recorded when the connection is set up.
    // Only read to reject calls from elsewhere, no synchronization involved.
    uv_thread_t loop_thread_ = {};
};

#endif  // RDMA_CONN_H
