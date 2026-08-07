#include "libinfinistore.h"

#include <arpa/inet.h>
#include <assert.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include <vector>

#include "config.h"
#include "log.h"
#include "protocol.h"
#include "rdma.h"
#include "utils.h"

/*
because python will always hold GIL when doing ~Connection(), which could lead to deadlock,
so we have to explicitly call close() to stop the rdma completion handler.
*/
void Connection::close_conn() {
    rdma_.stop();

    if (sock_ > 0) {
        close(sock_);
        // closing the same fd twice can take down an unrelated fd which has
        // reused the number in the meantime
        sock_ = 0;
    }
}

Connection::~Connection() {
    INFO("destroying connection");

    /*
    rdma_.stop() is deliberately not called here: it waits for the completion
    handler, whose callbacks need the GIL, and this destructor usually runs with the
    GIL held. ~RdmaConnection() falls back to a bounded wait and leaks rather than
    destroying resources the thread is still using.
    */
    if (sock_ > 0) {
        close(sock_);
        sock_ = 0;
    }
}

int Connection::setup_rdma(client_config_t config) {
    if (rdma_.open(config.dev_name, config.ib_port, config.link_type, config.hint_gid_index) < 0) {
        return -1;
    }

    // exchange the connection information with the server over the tcp socket
    rdma_conn_info_t remote_info = {};
    if (exchange_conn_info(&remote_info) < 0) {
        return -1;
    }

    return rdma_.connect(remote_info);
}

int Connection::init_connection(client_config_t config) {
    signal(SIGSEGV, signal_handler);
    signal(SIGABRT, signal_handler);
    signal(SIGBUS, signal_handler);
    signal(SIGFPE, signal_handler);
    signal(SIGILL, signal_handler);

    struct sockaddr_in serv_addr;
    // create socket
    if ((sock_ = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        ERROR("Failed to create socket");
        return -1;
    }

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(config.service_port);

    // always connect to localhost
    if (inet_pton(AF_INET, config.host_addr.data(), &serv_addr.sin_addr) <= 0) {
        ERROR("Invalid address/ Address not supported {}", config.host_addr);
        return -1;
    }

    INFO("Connecting to {}:{}", config.host_addr, config.service_port);
    if (connect(sock_, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        ERROR("Failed to connect to server");
        return -1;
    }
    return 0;
}

int Connection::exchange_conn_info(rdma_conn_info_t *remote_info) {
    header_t header = {
        .magic = MAGIC,
        .op = OP_RDMA_EXCHANGE,
        .body_size = sizeof(rdma_conn_info_t),
    };

    struct iovec iov[2];
    struct msghdr msg;

    rdma_conn_info_t local_info = rdma_.local_info();

    iov[0].iov_base = &header;
    iov[0].iov_len = FIXED_HEADER_SIZE;
    iov[1].iov_base = &local_info;
    iov[1].iov_len = sizeof(rdma_conn_info_t);

    memset(&msg, 0, sizeof(msg));
    msg.msg_iov = iov;
    msg.msg_iovlen = 2;

    if (sendmsg(sock_, &msg, 0) < 0) {
        ERROR("Failed to send local connection information");
        return -1;
    }

    int return_code = -1;
    if (recv(sock_, &return_code, RETURN_CODE_SIZE, MSG_WAITALL) != RETURN_CODE_SIZE) {
        ERROR("Failed to receive return code");
        return -1;
    }

    if (return_code != FINISH) {
        ERROR("Failed to exchange connection information, return code: {}", return_code);
        return -1;
    }

    if (recv(sock_, remote_info, sizeof(rdma_conn_info_t), MSG_WAITALL) !=
        sizeof(rdma_conn_info_t)) {
        ERROR("Failed to receive remote connection information");
        return -1;
    }
    return 0;
}

int Connection::check_exist(std::string key) {
    header_t header;
    header = {
        .magic = MAGIC, .op = OP_CHECK_EXIST, .body_size = static_cast<unsigned int>(key.size())};

    struct iovec iov[2];
    struct msghdr msg;
    memset(&msg, 0, sizeof(msg));

    iov[0].iov_base = &header;
    iov[0].iov_len = FIXED_HEADER_SIZE;
    iov[1].iov_base = const_cast<void *>(static_cast<const void *>(key.data()));
    iov[1].iov_len = key.size();
    msg.msg_iov = iov;
    msg.msg_iovlen = 2;

    if (sendmsg(sock_, &msg, 0) < 0) {
        ERROR("Failed to send header and body");
        return -1;
    }

    int return_code = 0;
    if (recv(sock_, &return_code, RETURN_CODE_SIZE, MSG_WAITALL) != RETURN_CODE_SIZE) {
        ERROR("Failed to receive return code");
        return -1;
    }
    if (return_code != FINISH) {
        ERROR("Failed to check exist");
        return -1;
    }

    int exist = 0;
    if (recv(sock_, &exist, sizeof(int), MSG_WAITALL) != sizeof(int)) {
        ERROR("Failed to receive exist");
        return -1;
    }
    return exist;
}

int Connection::get_match_last_index(std::vector<std::string> &keys) {
    INFO("get_match_last_index");

    FlatBufferBuilder builder(64 << 10);

    auto keys_offset = builder.CreateVectorOfStrings(keys);
    auto req = CreateGetMatchLastIndexRequest(builder, keys_offset);
    builder.Finish(req);

    header_t header = {
        .magic = MAGIC,
        .op = OP_GET_MATCH_LAST_IDX,
        .body_size = builder.GetSize(),
    };

    struct iovec iov[2];
    struct msghdr msg;
    memset(&msg, 0, sizeof(msg));

    iov[0].iov_base = &header;
    iov[0].iov_len = FIXED_HEADER_SIZE;
    iov[1].iov_base = builder.GetBufferPointer();
    iov[1].iov_len = builder.GetSize();

    msg.msg_iov = iov;
    msg.msg_iovlen = 2;

    if (sendmsg(sock_, &msg, 0) < 0) {
        ERROR("Failed to send header and body");
        return -1;
    }

    int return_code = 0;
    if (recv(sock_, &return_code, RETURN_CODE_SIZE, MSG_WAITALL) != RETURN_CODE_SIZE) {
        ERROR("Failed to receive return code");
        return -1;
    }
    if (return_code != FINISH) {
        ERROR("Failed to get match last index");
        return -1;
    }

    int last_index = -1;
    if (recv(sock_, &last_index, RETURN_CODE_SIZE, MSG_WAITALL) != RETURN_CODE_SIZE) {
        ERROR("Failed to receive return code");
        return -1;
    }

    return last_index;
}

/**
 *  The function sends the request to delete a list of keys from the store
 *
 *  Input:
 *    keys: the list of the keys to delete
 *
 *  Return:
 *    The count of the keys deleted, -1 if there is an error
 */
int Connection::delete_keys(const std::vector<std::string> &keys) {
    INFO("delete_keys");

    FlatBufferBuilder builder(64 << 10);

    auto keys_offset = builder.CreateVectorOfStrings(keys);
    auto req = CreateDeleteKeysRequest(builder, keys_offset);
    builder.Finish(req);

    header_t header = {
        .magic = MAGIC,
        .op = OP_DELETE_KEYS,
        .body_size = builder.GetSize(),
    };

    struct iovec iov[2];
    struct msghdr msg;
    memset(&msg, 0, sizeof(msg));

    iov[0].iov_base = &header;
    iov[0].iov_len = FIXED_HEADER_SIZE;
    iov[1].iov_base = builder.GetBufferPointer();
    iov[1].iov_len = builder.GetSize();

    msg.msg_iov = iov;
    msg.msg_iovlen = 2;

    if (sendmsg(sock_, &msg, 0) < 0) {
        ERROR("Failed to send header and body for delete_keys message");
        return -1;
    }

    // TODO: Merge the two recv's into one?
    int return_code = 0;
    if (recv(sock_, &return_code, RETURN_CODE_SIZE, MSG_WAITALL) != RETURN_CODE_SIZE) {
        ERROR("Failed to receive return code for delete_keys");
        return -1;
    }
    if (return_code != FINISH) {
        ERROR("Failed to delete keys, error: {}", return_code);
        return -1;
    }

    int count = -1;
    if (recv(sock_, &count, sizeof(count), MSG_WAITALL) != sizeof(count)) {
        ERROR("Failed to receive count of the keys deleted");
        return -1;
    }

    return count;
}

std::vector<unsigned char> *Connection::r_tcp(const std::string &key) {
    FlatBufferBuilder builder(64 << 10);
    auto req = CreateTCPPayloadRequestDirect(builder, key.c_str(), 0, OP_TCP_GET);
    builder.Finish(req);

    header_t header = {
        .magic = MAGIC,
        .op = OP_TCP_PAYLOAD,
        .body_size = builder.GetSize(),
    };

    struct iovec iov[2];
    struct msghdr msg;
    memset(&msg, 0, sizeof(msg));

    iov[0].iov_base = &header;
    iov[0].iov_len = FIXED_HEADER_SIZE;
    iov[1].iov_base = builder.GetBufferPointer();
    iov[1].iov_len = builder.GetSize();

    msg.msg_iov = iov;
    msg.msg_iovlen = 2;

    if (sendmsg(sock_, &msg, 0) < 0) {
        ERROR("r_tcp: Failed to send header");
        return nullptr;
    }

    uint32_t buf[2];
    if (recv(sock_, &buf, RETURN_CODE_SIZE * 2, MSG_WAITALL) != RETURN_CODE_SIZE * 2) {
        ERROR("r_tcp: Failed to receive return code");
        return nullptr;
    }

    int return_code = buf[0];
    int size = buf[1];

    if (return_code != FINISH) {
        ERROR("r_tcp: Failed to get value, return code: {}", key, return_code);
        return nullptr;
    }

    if (size == 0) {
        ERROR("r_tcp: size is 0");
        return nullptr;
    }

    auto ret_buf = new std::vector<unsigned char>(size);

    if (recv(sock_, ret_buf->data(), size, MSG_WAITALL) != size) {
        ERROR("r_tcp: Failed to receive payload");
        return nullptr;
    }
    return ret_buf;
}

int Connection::w_tcp(const std::string &key, void *ptr, size_t size) {
    assert(ptr != NULL);

    FlatBufferBuilder builder(64 << 10);
    auto req = CreateTCPPayloadRequestDirect(builder, key.c_str(), size, OP_TCP_PUT);
    builder.Finish(req);

    header_t header = {
        .magic = MAGIC,
        .op = OP_TCP_PAYLOAD,
        .body_size = builder.GetSize(),
    };

    struct iovec iov[2];
    struct msghdr msg;
    memset(&msg, 0, sizeof(msg));

    iov[0].iov_base = &header;
    iov[0].iov_len = FIXED_HEADER_SIZE;
    iov[1].iov_base = builder.GetBufferPointer();
    iov[1].iov_len = builder.GetSize();

    msg.msg_iov = iov;
    msg.msg_iovlen = 2;

    if (sendmsg(sock_, &msg, MSG_MORE) < 0) {
        ERROR("w_tcp: Failed to send header");
        return -1;
    }

    // reuse iov[0] and msghdr
    iov[0].iov_base = ptr;
    iov[0].iov_len = size;
    msg.msg_iov = iov;
    msg.msg_iovlen = 1;
    if (sendmsg(sock_, &msg, 0) < 0) {
        ERROR("w_tcp: Failed to send payload");
        return -1;
    }

    int return_code = 0;
    if (recv(sock_, &return_code, RETURN_CODE_SIZE, MSG_WAITALL) != RETURN_CODE_SIZE) {
        ERROR("w_tcp: Failed to receive return code");
        return -1;
    }
    if (return_code != FINISH) {
        ERROR("w_tcp: Failed to put key: {}, return code: {}", key, return_code);
        return -1;
    }

    return 0;
}

int Connection::w_rdma_async(const std::vector<std::string> &keys,
                             const std::vector<size_t> offsets, int block_size, void *base_ptr,
                             std::function<void(int)> callback) {
    return rdma_.post_write(keys, offsets, block_size, base_ptr, callback);
}

int Connection::r_rdma_async(const std::vector<std::string> &keys,
                             const std::vector<size_t> offsets, int block_size, void *base_ptr,
                             std::function<void(unsigned int)> callback) {
    return rdma_.post_read(keys, offsets, block_size, base_ptr, callback);
}

int Connection::register_mr(void *base_ptr, size_t ptr_region_size) {
    return rdma_.register_mr(base_ptr, ptr_region_size);
}
