#ifndef LIBINFINISTORE_H
#define LIBINFINISTORE_H

#include <arpa/inet.h>
#include <assert.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include <deque>
#include <map>
#include <stdexcept>

#include "config.h"
#include "log.h"
#include "protocol.h"
#include "rdma.h"
#include "rdma_conn.h"

/*
The client side connection: owns the tcp socket and speaks the protocol on it.
Everything RDMA lives in RdmaConnection, this class only carries the connection
info of both sides over the socket during setup_rdma().
*/
class Connection {
    // tcp socket
    int sock_ = 0;

    RdmaConnection rdma_;

   public:
    Connection() = default;

    Connection(const Connection &) = delete;
    // destroy the connection
    ~Connection();
    // stop the rdma completion handler and close the socket, safe to call twice
    void close_conn();
    int init_connection(client_config_t config);
    int setup_rdma(client_config_t config);
    int r_rdma_async(const std::vector<std::string> &keys, const std::vector<size_t> offsets,
                     int block_size, void *base_ptr, std::function<void(unsigned int)> callback);
    int w_rdma_async(const std::vector<std::string> &keys, const std::vector<size_t> offsets,
                     int block_size, void *base_ptr, std::function<void(int)> callback);
    int w_tcp(const std::string &key, void *ptr, size_t size);
    std::vector<unsigned char> *r_tcp(const std::string &key);

    int check_exist(std::string key);
    int get_match_last_index(std::vector<std::string> &keys);
    int delete_keys(const std::vector<std::string> &keys);
    int register_mr(void *base_ptr, size_t ptr_region_size);

    int exchange_conn_info(rdma_conn_info_t *remote_info);
};

#endif  // LIBINFINISTORE_H
