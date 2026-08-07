#include "libinfinistore.h"

#include <assert.h>
#include <signal.h>
#include <string.h>

#include <vector>

#include "config.h"
#include "log.h"
#include "protocol.h"
#include "utils.h"

namespace {

// fixed header followed by the request body, this is what goes on the wire first
std::vector<char> make_head(char op, const void *body, size_t body_size) {
    header_t header = {
        .magic = MAGIC,
        .op = op,
        .body_size = static_cast<unsigned int>(body_size),
    };

    std::vector<char> head(FIXED_HEADER_SIZE + body_size);
    memcpy(head.data(), &header, FIXED_HEADER_SIZE);
    if (body_size > 0) {
        memcpy(head.data() + FIXED_HEADER_SIZE, body, body_size);
    }
    return head;
}

// read an int out of a response body, returns false when the body is too short
bool body_to_int(const std::vector<unsigned char> &body, int *out) {
    if (body.size() < sizeof(int)) {
        return false;
    }
    memcpy(out, body.data(), sizeof(int));
    return true;
}

}  // namespace

void Connection::close_conn() {
    rdma_.stop();
    tcp_.close();
}

Connection::~Connection() {
    INFO("destroying connection");
    close_conn();
}

int Connection::init_connection(client_config_t config, unsigned long loop_ptr, ResultCallback cb) {
    signal(SIGSEGV, signal_handler);
    signal(SIGABRT, signal_handler);
    signal(SIGBUS, signal_handler);
    signal(SIGFPE, signal_handler);
    signal(SIGILL, signal_handler);

    loop_ = (uv_loop_t *)loop_ptr;
    if (loop_ == NULL) {
        ERROR("No event loop given");
        return -1;
    }

    return tcp_.connect(loop_, config.host_addr, config.service_port, cb);
}

int Connection::setup_rdma(client_config_t config, ResultCallback cb) {
    if (rdma_.open(config.dev_name, config.ib_port, config.link_type, config.hint_gid_index) < 0) {
        return -1;
    }

    // exchange the connection information with the server over the tcp connection
    rdma_conn_info_t local_info = rdma_.local_info();
    std::vector<char> head = make_head(OP_RDMA_EXCHANGE, &local_info, sizeof(rdma_conn_info_t));

    return tcp_.request(std::move(head), NULL, 0, RespSpec::fixed(sizeof(rdma_conn_info_t)),
                        [this, cb](int return_code, std::vector<unsigned char> body) {
                            if (return_code != FINISH) {
                                ERROR("Failed to exchange connection information, return code: {}",
                                      return_code);
                                cb(-1);
                                return;
                            }
                            if (body.size() != sizeof(rdma_conn_info_t)) {
                                ERROR("Failed to receive remote connection information");
                                cb(-1);
                                return;
                            }

                            rdma_conn_info_t remote_info;
                            memcpy(&remote_info, body.data(), sizeof(rdma_conn_info_t));
                            cb(rdma_.connect(loop_, remote_info));
                        });
}

int Connection::check_exist(const std::string &key, ResultCallback cb) {
    std::vector<char> head = make_head(OP_CHECK_EXIST, key.data(), key.size());

    return tcp_.request(std::move(head), NULL, 0, RespSpec::fixed(sizeof(int)),
                        [cb](int return_code, std::vector<unsigned char> body) {
                            int exist = 0;
                            if (return_code != FINISH || !body_to_int(body, &exist)) {
                                ERROR("Failed to check exist, return code: {}", return_code);
                                cb(-1);
                                return;
                            }
                            cb(exist);
                        });
}

int Connection::get_match_last_index(const std::vector<std::string> &keys, ResultCallback cb) {
    INFO("get_match_last_index");

    FlatBufferBuilder builder(64 << 10);
    auto keys_offset = builder.CreateVectorOfStrings(keys);
    auto req = CreateGetMatchLastIndexRequest(builder, keys_offset);
    builder.Finish(req);

    std::vector<char> head =
        make_head(OP_GET_MATCH_LAST_IDX, builder.GetBufferPointer(), builder.GetSize());

    return tcp_.request(std::move(head), NULL, 0, RespSpec::fixed(sizeof(int)),
                        [cb](int return_code, std::vector<unsigned char> body) {
                            int last_index = -1;
                            if (return_code != FINISH || !body_to_int(body, &last_index)) {
                                ERROR("Failed to get match last index, return code: {}",
                                      return_code);
                                cb(-1);
                                return;
                            }
                            cb(last_index);
                        });
}

/**
 *  Delete a list of keys from the store, the callback gets the number of keys
 *  deleted, or a negative value on error.
 */
int Connection::delete_keys(const std::vector<std::string> &keys, ResultCallback cb) {
    INFO("delete_keys");

    FlatBufferBuilder builder(64 << 10);
    auto keys_offset = builder.CreateVectorOfStrings(keys);
    auto req = CreateDeleteKeysRequest(builder, keys_offset);
    builder.Finish(req);

    std::vector<char> head =
        make_head(OP_DELETE_KEYS, builder.GetBufferPointer(), builder.GetSize());

    return tcp_.request(std::move(head), NULL, 0, RespSpec::fixed(sizeof(int)),
                        [cb](int return_code, std::vector<unsigned char> body) {
                            int count = -1;
                            if (return_code != FINISH || !body_to_int(body, &count)) {
                                ERROR("Failed to delete keys, return code: {}", return_code);
                                cb(-1);
                                return;
                            }
                            cb(count);
                        });
}

int Connection::r_tcp(const std::string &key, ReadCallback cb) {
    FlatBufferBuilder builder(64 << 10);
    auto req = CreateTCPPayloadRequestDirect(builder, key.c_str(), 0, OP_TCP_GET);
    builder.Finish(req);

    std::vector<char> head =
        make_head(OP_TCP_PAYLOAD, builder.GetBufferPointer(), builder.GetSize());

    return tcp_.request(std::move(head), NULL, 0, RespSpec::sized(),
                        [cb](int return_code, std::vector<unsigned char> body) {
                            if (return_code != FINISH) {
                                ERROR("Failed to get value, return code: {}", return_code);
                            }
                            cb(return_code, std::move(body));
                        });
}

int Connection::w_tcp(const std::string &key, void *ptr, size_t size, ResultCallback cb) {
    assert(ptr != NULL);

    FlatBufferBuilder builder(64 << 10);
    auto req = CreateTCPPayloadRequestDirect(builder, key.c_str(), size, OP_TCP_PUT);
    builder.Finish(req);

    std::vector<char> head =
        make_head(OP_TCP_PAYLOAD, builder.GetBufferPointer(), builder.GetSize());

    // ptr is not copied, the caller keeps it alive until the callback has run
    return tcp_.request(std::move(head), ptr, size, RespSpec::code_only(),
                        [cb, key](int return_code, std::vector<unsigned char>) {
                            if (return_code != FINISH) {
                                ERROR("Failed to put key: {}, return code: {}", key, return_code);
                                cb(-1);
                                return;
                            }
                            cb(0);
                        });
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
