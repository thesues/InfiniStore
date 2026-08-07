#ifndef RDMA_CONN_H
#define RDMA_CONN_H

#include <atomic>
#include <boost/lockfree/spsc_queue.hpp>
#include <chrono>
#include <functional>
#include <future>
#include <string>
#include <thread>
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
The client side RDMA connection: device, QP, memory regions, send buffers and the
completion queue handler thread. It does not do any TCP/protocol work, the owner
(Connection) carries the connection info of both sides over its own socket and
hands it over through local_info()/connect().

Threading: every method is meant to be called from a single thread, the only other
thread involved is the completion handler started by connect(). send_buffers_ is a
single producer(that thread) single consumer(the caller) queue, so post_read and
post_write must not be called concurrently from several threads.
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

    // bring the QP up against the remote side and start the completion handler
    int connect(const rdma_conn_info_t &remote_info);

    /*
    Stop the completion handler thread. Idempotent.

    This can not be done in the destructor: the completion callbacks call back into
    python, so they need the GIL, while the destructor is usually called from python
    with the GIL held. Waiting for the thread there would deadlock, which is why the
    owner has to call this explicitly first.
    */
    void stop();

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
    void cq_handler();
    // post a work request which is only there to wake the completion handler up
    int wake_cq_thread();
    // true once the completion handler has returned
    bool cq_thread_exited();

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
    boost::lockfree::spsc_queue<SendBuffer *> send_buffers_{MAX_RECV_WR};

    std::thread cq_thread_;
    // set by the completion handler right before it returns, so the destructor can
    // wait for it with a timeout instead of blocking forever
    std::promise<void> cq_exited_;
    std::future<void> cq_exited_future_;

    std::atomic<bool> stop_{false};
};

#endif  // RDMA_CONN_H
