#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <cuda.h>
#include <cuda_runtime.h>
#include <infiniband/verbs.h>

#include <msgpack.hpp>
#include <string>
#include <vector>

/*
Protocol:

REQUEST:
+-------------------+
| MAGIC(4 bytes)    |
+-------------------+
| OP(1 byte)        |
+-------------------+

and then

+-------------------+
|Fixed Size Payload |
+-------------------+
OR
+---------------------+
|Variable Size Payload|
+---------------------+



RESPONSE:

Error code:
+--------------------+
| ERROR_CODE(4 bytes)|
+--------------------+

and then

+-------------------+
|Fixed Size Payload |
+-------------------+
OR
+---------------------+
|Variable Size Payload|
+---------------------+
*/

#define MAX_WR 8192

#define MAGIC 0xdeadbeef
#define MAGIC_SIZE 4

#define OP_R 'R'
#define OP_W 'W'
#define OP_SYNC 'S'
#define OP_RDMA_EXCHANGE 'E'
#define OP_RDMA_WRITE 'D'
#define OP_RDMA_READ 'A'
#define OP_CHECK_EXIST 'C'
#define OP_GET_MATCH_LAST_IDX 'M'
#define OP_SIZE 1
// please add op name in protocol.cpp

std::string op_name(char op);

// error code: int
#define INVALID_REQ 400
#define FINISH 200
#define TASK_ACCEPTED 202
#define INTERNAL_ERROR 500
#define KEY_NOT_FOUND 404
#define RETRY 408
#define SYSTEM_ERROR 503

#define RETURN_CODE_SIZE sizeof(int)

typedef struct __attribute__((packed)) {
    unsigned int magic;
    char op;
    unsigned int body_size;
} header_t;

typedef struct {
    std::string key;
    unsigned long offset;
    MSGPACK_DEFINE(key, offset)
} block_t;

typedef struct {
    std::vector<std::string> keys;
    MSGPACK_DEFINE(keys)
} keys_t;

// implement pack for ipcHandler
namespace msgpack {
MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
    namespace adaptor {

    template <>
    struct pack<cudaIpcMemHandle_t> {
        template <typename Stream>
        packer<Stream>& operator()(msgpack::packer<Stream>& o, const cudaIpcMemHandle_t& v) const {
            o.pack_bin(sizeof(cudaIpcMemHandle_t));
            o.pack_bin_body(reinterpret_cast<const char*>(&v), sizeof(cudaIpcMemHandle_t));
            return o;
        }
    };

    template <>
    struct convert<cudaIpcMemHandle_t> {
        msgpack::object const& operator()(msgpack::object const& o, cudaIpcMemHandle_t& v) const {
            if (o.type != msgpack::type::BIN || o.via.bin.size != sizeof(cudaIpcMemHandle_t)) {
                throw msgpack::type_error();
            }
            std::memcpy(&v, o.via.bin.ptr, sizeof(cudaIpcMemHandle_t));
            return o;
        }
    };

    }  // namespace adaptor
}  // MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
}  // namespace msgpack

typedef struct {
    cudaIpcMemHandle_t ipc_handle;
    int block_size;
    std::vector<block_t> blocks;
    MSGPACK_DEFINE(ipc_handle, block_size, blocks)

} local_meta_t;

typedef struct {
    std::vector<std::string> keys;
    int block_size;
    uint32_t rkey;                        // client's rkey
    std::vector<uintptr_t> remote_addrs;  // client's GPU address
    char op;
    MSGPACK_DEFINE(keys, block_size, rkey, remote_addrs, op)
} remote_meta_request;  // rdma read/write request

typedef struct __attribute__((packed)) rdma_conn_info_t {
    uint32_t qpn;
    uint32_t psn;
    union ibv_gid gid;  // RoCE v2
    uint16_t lid;       // IB
} rdma_conn_info_t;

template <typename T>
bool serialize(const T& data, std::string& out) {
    try {
        msgpack::sbuffer sbuf;
        msgpack::pack(sbuf, data);
        out.assign(sbuf.data(), sbuf.size());
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

template <typename T>
bool serialize_to_fixed(const T& data, char* buffer, size_t buffer_size, size_t& packed_size) {
    try {
        msgpack::sbuffer sbuffer;
        msgpack::packer<msgpack::sbuffer> packer(sbuffer);
        packer.pack(data);
        packed_size = sbuffer.size();
        if (packed_size > buffer_size) {
            return false;
        }
        std::memcpy(buffer, sbuffer.data(), packed_size);
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

template <typename T>
bool deserialize(const char* data, size_t size, T& out) {
    try {
        msgpack::object_handle oh = msgpack::unpack(data, size);
        oh.get().convert(out);
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

template bool serialize<keys_t>(const keys_t& data, std::string& out);
template bool deserialize<keys_t>(const char* data, size_t size, keys_t& out);
template bool serialize<local_meta_t>(const local_meta_t& data, std::string& out);
template bool deserialize<local_meta_t>(const char* data, size_t size, local_meta_t& out);
template bool serialize<remote_meta_request>(const remote_meta_request& data, std::string& out);
template bool deserialize<remote_meta_request>(const char* data, size_t size,
                                               remote_meta_request& out);

#define FIXED_HEADER_SIZE sizeof(header_t)

#endif
