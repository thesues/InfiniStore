#ifndef LIBINFINISTORE_H
#define LIBINFINISTORE_H

#include <assert.h>
#include <time.h>
#include <unistd.h>

#include <functional>
#include <string>
#include <vector>

#include "config.h"
#include "log.h"
#include "protocol.h"
#include "rdma.h"
#include "rdma_conn.h"
#include "tcp_conn.h"

/*
The client side connection: speaks the protocol on a TcpConnection and owns a
RdmaConnection for the data path.

Everything here is asynchronous and runs on the caller's libuv loop, which is
handed over in init_connection(). Each call queues the request and returns
immediately, the callback runs later on the loop thread. A negative value handed
to a callback means the request failed.
*/
class Connection {
    TcpConnection tcp_;
    RdmaConnection rdma_;

   public:
    // result of a request, negative on error
    using ResultCallback = std::function<void(int)>;
    // return code and payload of a tcp read
    using ReadCallback = std::function<void(int, std::vector<unsigned char>)>;

    Connection() = default;

    Connection(const Connection &) = delete;
    // destroy the connection
    ~Connection();
    // stop the rdma completion handler and close the socket, safe to call twice
    void close_conn();

    // loop_ptr is the uv_loop_t the caller runs, the connection lives on it
    int init_connection(client_config_t config, unsigned long loop_ptr, ResultCallback cb);
    int setup_rdma(client_config_t config, ResultCallback cb);

    // cb gets 1 if the key exists, 0 if it does not
    int check_exist(const std::string &key, ResultCallback cb);
    // cb gets the last index of the key list which is in the store
    int get_match_last_index(const std::vector<std::string> &keys, ResultCallback cb);
    // cb gets the number of keys deleted
    int delete_keys(const std::vector<std::string> &keys, ResultCallback cb);
    // ptr has to stay alive until cb has run
    int w_tcp(const std::string &key, void *ptr, size_t size, ResultCallback cb);
    int r_tcp(const std::string &key, ReadCallback cb);

    int r_rdma_async(const std::vector<std::string> &keys, const std::vector<size_t> offsets,
                     int block_size, void *base_ptr, std::function<void(unsigned int)> callback);
    int w_rdma_async(const std::vector<std::string> &keys, const std::vector<size_t> offsets,
                     int block_size, void *base_ptr, std::function<void(int)> callback);
    int register_mr(void *base_ptr, size_t ptr_region_size);
};

#endif  // LIBINFINISTORE_H
