"""
Blocking client: no event loop anywhere in the application.

infinistore runs its io on a background thread of its own, so the calls below
just block until they are done, and they can be made from any thread, including
several at the same time.
"""

import ctypes
import threading
import uuid

import infinistore


config = infinistore.ClientConfig(
    host_addr="127.0.0.1",
    service_port=12345,
    connection_type=infinistore.TYPE_TCP,
    log_level="info",
)


def get_ptr(buf):
    return ctypes.addressof(ctypes.c_char.from_buffer(buf))


def main():
    conn = infinistore.InfinityConnection(config)
    # no event loop needed, this starts one on a background thread
    conn.connect()

    try:
        # ---- plain blocking calls ----
        key = str(uuid.uuid4())
        src = bytearray(128 * 1024)
        for i in range(len(src)):
            src[i] = i % 256

        conn.tcp_write_cache(key, get_ptr(src), len(src))
        assert conn.check_exist(key)

        dst = conn.tcp_read_cache(key)
        assert bytes(dst) == bytes(src)
        print(f"round trip of {len(src)} bytes done")

        # ---- the same connection, from several worker threads ----
        # requests are serialized inside the connection, no locking needed here
        errors = []

        def worker(n):
            try:
                buf = bytearray(64 * 1024)
                for i in range(len(buf)):
                    buf[i] = (i + n) % 256

                for i in range(10):
                    k = f"worker-{n}-{i}"
                    conn.tcp_write_cache(k, get_ptr(buf), len(buf))
                    got = conn.tcp_read_cache(k)
                    assert bytes(got) == bytes(buf)
            except Exception as e:  # noqa: BLE001
                errors.append(f"worker {n}: {e!r}")

        threads = [threading.Thread(target=worker, args=(n,)) for n in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, errors
        print("8 threads x 10 round trips done")

        deleted = conn.delete_keys([f"worker-{n}-0" for n in range(8)])
        print(f"deleted {deleted} keys")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
