#ifndef TCP_CONN_H
#define TCP_CONN_H

#include <uv.h>

#include <deque>
#include <functional>
#include <string>
#include <vector>

#include "protocol.h"

/*
Shape of the response the client expects for a request. The protocol is not self
describing: every response starts with a return code, what follows depends on the
request, so the caller has to say what it is waiting for.

A return code other than FINISH is always the whole response, the server does not
send a body in that case.
*/
struct RespSpec {
    enum Type {
        CODE_ONLY,  // return code only
        FIXED,      // return code + `size` bytes
        SIZED,      // return code + 4 byte length + that many bytes
    };

    Type type = CODE_ONLY;
    size_t size = 0;

    static RespSpec code_only() { return RespSpec{CODE_ONLY, 0}; }
    static RespSpec fixed(size_t size) { return RespSpec{FIXED, size}; }
    static RespSpec sized() { return RespSpec{SIZED, 0}; }
};

/*
The client side TCP connection, running on a libuv loop owned by the caller.

Nothing here blocks: a request is queued, the loop sends it and the callback runs
when the response is complete. Every method has to be called from the loop thread.

Requests are serialized, one in flight at a time. The server is not safe to
pipeline: a OP_TCP_GET response is written in chunks from a write callback, so a
response produced for a following request would be interleaved in between.
*/
class TcpConnection {
   public:
    // return code, body(empty when the return code is not FINISH)
    using ResponseCallback = std::function<void(int, std::vector<unsigned char>)>;
    using ConnectCallback = std::function<void(int)>;

    TcpConnection() = default;
    TcpConnection(const TcpConnection &) = delete;
    ~TcpConnection();

    // start connecting, cb gets 0 on success and a negative value otherwise
    int connect(uv_loop_t *loop, const std::string &host, int port, ConnectCallback cb);

    /*
    Queue one request. head(fixed header + body) is copied, payload is not: the
    caller has to keep it alive until cb has run. cb always runs exactly once,
    with a negative return code if the connection went away.
    */
    int request(std::vector<char> head, const void *payload, size_t payload_size,
                const RespSpec &resp, ResponseCallback cb);

    // fail everything still in flight and close the handle, safe to call twice
    void close();

    bool connected() const { return connected_; }

   private:
    struct Request {
        std::vector<char> head;
        const void *payload = NULL;
        size_t payload_size = 0;
        RespSpec resp;
        ResponseCallback cb;
    };

    void on_connected(int status);
    void on_data(const char *data, size_t len);
    void on_eof(int status);
    // send the request at the front of the queue if nothing is in flight
    void flush_queue();
    // hand the response to the in flight request and move on to the next one
    void finish_inflight(int return_code, std::vector<unsigned char> body);
    void fail_all(int return_code);

    static void alloc_buffer_cb(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf);
    static void read_cb(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf);
    static void connect_cb(uv_connect_t *req, int status);
    static void write_cb(uv_write_t *req, int status);

    enum ReadState {
        READ_RETURN_CODE,
        READ_BODY_SIZE,
        READ_BODY,
    };

    uv_loop_t *loop_ = NULL;
    // heap allocated: libuv needs the memory to stay valid until the close
    // callback has run, which is after this object may be gone
    uv_tcp_t *handle_ = NULL;

    bool connected_ = false;
    bool closing_ = false;

    ConnectCallback connect_cb_;

    std::deque<Request> queue_;
    bool inflight_ = false;

    ReadState state_ = READ_RETURN_CODE;
    char scratch_[RETURN_CODE_SIZE] = {};
    size_t scratch_read_ = 0;
    int return_code_ = 0;
    std::vector<unsigned char> body_;
    size_t body_read_ = 0;
};

#endif  // TCP_CONN_H
