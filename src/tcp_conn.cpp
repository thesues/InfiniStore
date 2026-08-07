#include "tcp_conn.h"

#include <arpa/inet.h>
#include <string.h>

#include <algorithm>

#include "log.h"

namespace {

// every write carries the request head, it has to stay alive until the write is done
struct WriteReq {
    uv_write_t req;
    std::vector<char> head;
};

}  // namespace

TcpConnection::~TcpConnection() { close(); }

void TcpConnection::alloc_buffer_cb(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
    (void)handle;
    buf->base = (char *)malloc(suggested_size);
    buf->len = buf->base ? suggested_size : 0;
}

int TcpConnection::connect(uv_loop_t *loop, const std::string &host, int port,
                           ConnectCallback cb) {
    if (handle_ != NULL) {
        ERROR("connection is already in use");
        return -1;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
        ERROR("Invalid address/ Address not supported {}", host);
        return -1;
    }

    loop_ = loop;
    handle_ = (uv_tcp_t *)malloc(sizeof(uv_tcp_t));
    if (handle_ == NULL) {
        ERROR("Failed to allocate tcp handle");
        return -1;
    }

    int ret = uv_tcp_init(loop_, handle_);
    if (ret < 0) {
        ERROR("Failed to init tcp handle: {}", uv_strerror(ret));
        free(handle_);
        handle_ = NULL;
        return -1;
    }
    handle_->data = this;
    closing_ = false;
    connect_cb_ = cb;

    INFO("Connecting to {}:{}", host, port);
    uv_connect_t *req = (uv_connect_t *)malloc(sizeof(uv_connect_t));
    ret = uv_tcp_connect(req, handle_, (const struct sockaddr *)&addr, connect_cb);
    if (ret < 0) {
        ERROR("Failed to connect to server: {}", uv_strerror(ret));
        free(req);
        connect_cb_ = NULL;
        close();
        return -1;
    }
    return 0;
}

void TcpConnection::connect_cb(uv_connect_t *req, int status) {
    uv_tcp_t *handle = (uv_tcp_t *)req->handle;
    free(req);

    // close() clears data, the connect callback was already reported from there
    TcpConnection *self = (TcpConnection *)handle->data;
    if (self == NULL) {
        return;
    }
    self->on_connected(status);
}

void TcpConnection::on_connected(int status) {
    ConnectCallback cb = connect_cb_;
    connect_cb_ = NULL;

    if (status < 0) {
        ERROR("Failed to connect to server: {}", uv_strerror(status));
        close();
        if (cb) {
            cb(status);
        }
        return;
    }

    int ret = uv_read_start((uv_stream_t *)handle_, alloc_buffer_cb, read_cb);
    if (ret < 0) {
        ERROR("Failed to start reading: {}", uv_strerror(ret));
        close();
        if (cb) {
            cb(ret);
        }
        return;
    }

    connected_ = true;
    if (cb) {
        cb(0);
    }
    flush_queue();
}

int TcpConnection::request(std::vector<char> head, const void *payload, size_t payload_size,
                           const RespSpec &resp, ResponseCallback cb) {
    if (closing_ || handle_ == NULL) {
        ERROR("connection is closed");
        return -1;
    }

    Request req;
    req.head = std::move(head);
    req.payload = payload;
    req.payload_size = payload_size;
    req.resp = resp;
    req.cb = cb;
    queue_.push_back(std::move(req));

    flush_queue();
    return 0;
}

void TcpConnection::flush_queue() {
    if (inflight_ || !connected_ || closing_ || queue_.empty()) {
        return;
    }

    Request &req = queue_.front();

    WriteReq *wr = new WriteReq();
    wr->head = std::move(req.head);
    wr->req.data = wr;

    uv_buf_t bufs[2];
    int nbufs = 1;
    bufs[0] = uv_buf_init(wr->head.data(), wr->head.size());
    if (req.payload != NULL && req.payload_size > 0) {
        bufs[1] = uv_buf_init((char *)req.payload, req.payload_size);
        nbufs = 2;
    }

    int ret = uv_write(&wr->req, (uv_stream_t *)handle_, bufs, nbufs, write_cb);
    if (ret < 0) {
        ERROR("Failed to write request: {}", uv_strerror(ret));
        delete wr;
        fail_all(SYSTEM_ERROR);
        close();
        return;
    }
    inflight_ = true;
}

void TcpConnection::write_cb(uv_write_t *req, int status) {
    WriteReq *wr = (WriteReq *)req->data;
    uv_handle_t *handle = (uv_handle_t *)req->handle;
    delete wr;

    if (status < 0) {
        // the response callbacks are reported from close()/on_eof
        TcpConnection *self = (TcpConnection *)handle->data;
        if (self == NULL) {
            return;
        }
        ERROR("Write error {}", uv_strerror(status));
        self->on_eof(status);
    }
}

void TcpConnection::read_cb(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf) {
    TcpConnection *self = (TcpConnection *)stream->data;

    if (self != NULL) {
        if (nread < 0) {
            self->on_eof(nread);
        }
        else if (nread > 0) {
            self->on_data(buf->base, nread);
        }
    }

    if (buf->base) {
        free(buf->base);
    }
}

void TcpConnection::on_data(const char *data, size_t len) {
    size_t offset = 0;

    while (offset < len && !closing_) {
        switch (state_) {
            case READ_RETURN_CODE: {
                size_t to_copy = std::min(len - offset, RETURN_CODE_SIZE - scratch_read_);
                memcpy(scratch_ + scratch_read_, data + offset, to_copy);
                scratch_read_ += to_copy;
                offset += to_copy;
                if (scratch_read_ < RETURN_CODE_SIZE) {
                    break;
                }
                memcpy(&return_code_, scratch_, RETURN_CODE_SIZE);
                scratch_read_ = 0;

                if (!inflight_ || queue_.empty()) {
                    ERROR("Received a response without a pending request");
                    fail_all(SYSTEM_ERROR);
                    close();
                    return;
                }

                if (return_code_ != FINISH) {
                    // an error response never carries a body
                    finish_inflight(return_code_, {});
                    break;
                }

                const RespSpec &resp = queue_.front().resp;
                if (resp.type == RespSpec::CODE_ONLY) {
                    finish_inflight(return_code_, {});
                }
                else if (resp.type == RespSpec::FIXED) {
                    if (resp.size == 0) {
                        finish_inflight(return_code_, {});
                    }
                    else {
                        body_.assign(resp.size, 0);
                        body_read_ = 0;
                        state_ = READ_BODY;
                    }
                }
                else {
                    state_ = READ_BODY_SIZE;
                }
                break;
            }

            case READ_BODY_SIZE: {
                size_t to_copy = std::min(len - offset, RETURN_CODE_SIZE - scratch_read_);
                memcpy(scratch_ + scratch_read_, data + offset, to_copy);
                scratch_read_ += to_copy;
                offset += to_copy;
                if (scratch_read_ < RETURN_CODE_SIZE) {
                    break;
                }
                unsigned int size = 0;
                memcpy(&size, scratch_, RETURN_CODE_SIZE);
                scratch_read_ = 0;

                if (size == 0) {
                    finish_inflight(return_code_, {});
                    break;
                }
                body_.assign(size, 0);
                body_read_ = 0;
                state_ = READ_BODY;
                break;
            }

            case READ_BODY: {
                size_t to_copy = std::min(len - offset, body_.size() - body_read_);
                memcpy(body_.data() + body_read_, data + offset, to_copy);
                body_read_ += to_copy;
                offset += to_copy;
                if (body_read_ == body_.size()) {
                    finish_inflight(return_code_, std::move(body_));
                    body_ = std::vector<unsigned char>();
                    body_read_ = 0;
                }
                break;
            }
        }
    }
}

void TcpConnection::finish_inflight(int return_code, std::vector<unsigned char> body) {
    state_ = READ_RETURN_CODE;
    scratch_read_ = 0;

    ResponseCallback cb = queue_.front().cb;
    queue_.pop_front();
    inflight_ = false;

    if (cb) {
        // the callback may queue another request or close the connection
        cb(return_code, std::move(body));
    }

    flush_queue();
}

void TcpConnection::on_eof(int status) {
    if (closing_) {
        return;
    }
    if (status != UV_EOF) {
        ERROR("Read error {}", uv_err_name(status));
    }
    close();
}

void TcpConnection::fail_all(int return_code) {
    // the callbacks may touch the queue, so drain it first
    std::deque<Request> queue;
    queue.swap(queue_);
    inflight_ = false;

    for (auto &req : queue) {
        if (req.cb) {
            req.cb(return_code, {});
        }
    }
}

void TcpConnection::close() {
    if (closing_) {
        return;
    }
    closing_ = true;
    connected_ = false;

    ConnectCallback connect_cb = connect_cb_;
    connect_cb_ = NULL;

    if (handle_ != NULL) {
        /*
        The handle outlives this object: its memory may only be released once the
        close callback has run, which is a loop iteration later. Cut the back
        pointer so the pending callbacks do not reach a destroyed connection.
        */
        handle_->data = NULL;
        uv_close((uv_handle_t *)handle_, [](uv_handle_t *handle) { free(handle); });
        handle_ = NULL;
    }

    fail_all(SYSTEM_ERROR);

    if (connect_cb) {
        connect_cb(-1);
    }
}
