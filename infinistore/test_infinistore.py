import torch
import pytest
import infinistore
import time
import os
import signal
import subprocess
import random
import string
import asyncio
import json
import ctypes
from multiprocessing import Process
import requests # For making HTTP requests to the metrics endpoint
import re       # For parsing metrics text


RDMA_DEV = []


# Fixture to set RDMA_DEV before running tests
@pytest.fixture(scope="session", autouse=True)
def set_rdma_dev():
    global RDMA_DEV
    RDMA_DEV = get_active_rdma_devices()
    assert len(RDMA_DEV) > 0  # At least one RDMA device should be active
    print(f"Active RDMA devices: {RDMA_DEV}")


# Fixture to start the TCzpserver before running tests
@pytest.fixture(scope="module")
def server():
    server_process = subprocess.Popen(
        [
            "python",
            "-m",
            "infinistore.server",
            "--dev-name",
            f"{RDMA_DEV[0]}",
            "--link-type",
            "Ethernet",
            "--service-port",
            "92345",
            "--manage-port",
            "98080",
        ]
    )
    time.sleep(4)
    if server_process.poll() is None:
        print("Test Server process is running.")
    else:
        print("Server process failed to start or has already exited.")
        assert False
    yield
    os.kill(server_process.pid, signal.SIGINT)
    server_process.wait()


@pytest.fixture(scope="module")
def server_with_metrics():
    manage_port = 98080 # Management port for FastAPI
    service_port = 92345 # Data plane port
    metrics_endpoint_url = f"http://127.0.0.1:{manage_port}/metrics" 
    
    server_cmd = [
        "python",
        "-m",
        "infinistore.server",
        "--dev-name",
        f"{RDMA_DEV[0]}", 
        "--link-type",
        "Ethernet",
        "--service-port",
        str(service_port),
        "--manage-port",
        str(manage_port)
        # --metrics-port is REMOVED
    ]
    server_process = subprocess.Popen(server_cmd)
    
    # Robust check: poll the new /metrics endpoint to ensure server is ready
    max_retries = 20 # Increased retries
    retry_interval = 1 # seconds
    server_ready = False
    for i in range(max_retries):
        try:
            response = requests.get(metrics_endpoint_url, timeout=1) # Increased timeout slightly
            if response.status_code == 200:
                print(f"Test Server process with metrics is running (PID: {server_process.pid}). Metrics URL: {metrics_endpoint_url}")
                server_ready = True
                break
        except requests.ConnectionError:
            # print(f"Metrics endpoint not yet available (attempt {i+1}/{max_retries})...")
            pass 
        except requests.ReadTimeout:
            print(f"Read timeout connecting to metrics endpoint (attempt {i+1}/{max_retries})...")
            pass 
        time.sleep(retry_interval)

    if not server_ready:
        if server_process.poll() is not None:
             print(f"Server process failed to start or has already exited. Return code: {server_process.returncode}")
        assert False, f"Server process for metrics test failed to become ready at {metrics_endpoint_url}"
   
    yield server_process, metrics_endpoint_url
    
    print(f"Stopping server process (PID: {server_process.pid})")
    os.kill(server_process.pid, signal.SIGINT)
    server_process.wait()


# add a flat to whether the same connection.


def generate_random_string(length):
    letters_and_digits = string.ascii_letters + string.digits
    random_string = "".join(random.choice(letters_and_digits) for i in range(length))
    return random_string


def get_active_rdma_devices():
    try:
        result = subprocess.run(
            ["rdma", "link", "-j"], capture_output=True, text=True, check=True
        )
        output = result.stdout

        active_devices = []
        devices = json.loads(output)
        for device in devices:
            if device["state"] == "ACTIVE":
                ifname = device["ifname"].split("/")[0]  # Extract mlx5_X part only
                if ifname not in active_devices:
                    active_devices.append(ifname)

        return active_devices
    except subprocess.CalledProcessError as e:
        print(f"Error while executing command: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"Error while parsing JSON: {e}")
        return []


def get_gpu_count():
    if torch.cuda.is_available():
        gpu_count = torch.cuda.device_count()
        return gpu_count
    else:
        return 0


@pytest.mark.parametrize("dtype", [torch.float16, torch.float32])
def test_basic_read_write_cache(server, dtype):
    config = infinistore.ClientConfig(
        host_addr="127.0.0.1",
        service_port=92345,
        link_type=infinistore.LINK_ETHERNET,
        dev_name=f"{RDMA_DEV[0]}",
    )

    config.connection_type = infinistore.TYPE_RDMA

    conn = infinistore.InfinityConnection(config)
    conn.connect()

    # key is random string
    key = generate_random_string(10)
    src = [i for i in range(4096)]

    src_tensor = torch.tensor(src, device="cuda:0", dtype=dtype)

    torch.cuda.synchronize(src_tensor.device)

    conn.register_mr(
        src_tensor.data_ptr(), src_tensor.numel() * src_tensor.element_size()
    )
    element_size = torch._utils._element_size(dtype)

    async def run_write():
        await conn.rdma_write_cache_async(
            [(key, 0)], len(src) * element_size, src_tensor.data_ptr()
        )

    asyncio.run(run_write())
    conn.close()

    conn = infinistore.InfinityConnection(config)
    conn.connect()

    dst = torch.zeros(4096, device="cuda:0", dtype=dtype)

    conn.register_mr(dst.data_ptr(), dst.numel() * dst.element_size())

    async def run_read():
        await conn.rdma_read_cache_async(
            [(key, 0)], len(dst) * element_size, dst.data_ptr()
        )

    asyncio.run(run_read())
    assert torch.equal(src_tensor, dst)
    conn.close()


@pytest.mark.parametrize("separated_gpu", [False, True])
def test_batch_read_write_cache(server, separated_gpu):
    config = infinistore.ClientConfig(
        host_addr="127.0.0.1",
        service_port=92345,
        link_type=infinistore.LINK_ETHERNET,
        dev_name="RDMA_DEV[0]",
    )

    config.connection_type = infinistore.TYPE_RDMA

    # test if we have multiple GPUs
    if separated_gpu:
        if get_gpu_count() >= 2:
            src_device = "cuda:0"
            dst_device = "cuda:1"
        else:
            # skip if we don't have enough GPUs
            return
    else:
        src_device = "cuda:0"
        dst_device = "cuda:0"

    conn = infinistore.InfinityConnection(config)
    conn.connect()

    num_of_blocks = 10
    block_size = 4096
    src = [i for i in range(num_of_blocks * block_size)]

    src_tensor = torch.tensor(src, device=src_device, dtype=torch.float32)
    torch.cuda.synchronize(src_tensor.device)

    async def run():
        # write/read 3 times
        for i in range(3):
            keys = [generate_random_string(num_of_blocks) for i in range(10)]
            await asyncio.to_thread(
                conn.register_mr,
                src_tensor.data_ptr(),
                src_tensor.numel() * src_tensor.element_size(),
            )

            blocks_offsets = [
                (keys[i], i * block_size * 4) for i in range(num_of_blocks)
            ]

            await conn.rdma_write_cache_async(
                blocks_offsets, block_size * 4, src_tensor.data_ptr()
            )

            dst = torch.zeros(
                num_of_blocks * block_size, device=dst_device, dtype=torch.float32
            )
            await asyncio.to_thread(
                conn.register_mr, dst.data_ptr(), dst.numel() * dst.element_size()
            )

            await conn.rdma_read_cache_async(
                blocks_offsets, block_size * 4, dst.data_ptr()
            )
            assert torch.equal(src_tensor.cpu(), dst.cpu())

    asyncio.run(run())
    conn.close()


@pytest.mark.parametrize("num_clients", [2])
def test_multiple_clients(num_clients):
    def run():
        config = infinistore.ClientConfig(
            host_addr="127.0.0.1",
            service_port=92345,
            link_type=infinistore.LINK_ETHERNET,
            dev_name=f"{RDMA_DEV[0]}",
        )

        config.connection_type = infinistore.TYPE_RDMA

        conn = infinistore.InfinityConnection(config)
        conn.connect()

        # key is random string
        key = generate_random_string(10)
        src = [i for i in range(4096)]

        src_tensor = torch.tensor(src, device="cuda:0", dtype=torch.float32)

        torch.cuda.synchronize(src_tensor.device)
        conn.register_mr(
            src_tensor.data_ptr(), src_tensor.numel() * src_tensor.element_size()
        )
        element_size = torch._utils._element_size(torch.float32)

        asyncio.run(
            conn.rdma_write_cache_async(
                [(key, 0)], 4096 * element_size, src_tensor.data_ptr()
            )
        )
        conn.close()

        conn = infinistore.InfinityConnection(config)
        conn.connect()

        dst = torch.zeros(4096, device="cuda:0", dtype=torch.float32)
        conn.register_mr(dst.data_ptr(), dst.numel() * dst.element_size())
        asyncio.run(
            conn.rdma_read_cache_async([(key, 0)], 4096 * element_size, dst.data_ptr())
        )
        assert torch.equal(src_tensor, dst)
        conn.close()

    processes = []
    for _ in range(num_clients):
        p = Process(target=run)
        p.start()
        processes.append(p)
    for i in range(num_clients):
        processes[i].join()


def test_key_check(server):
    config = infinistore.ClientConfig(
        host_addr="127.0.0.1",
        service_port=92345,
        link_type=infinistore.LINK_ETHERNET,
        dev_name="RDMA_DEV[0]",
        connection_type=infinistore.TYPE_RDMA,
    )
    conn = infinistore.InfinityConnection(config)
    conn.connect()
    key = generate_random_string(5)
    src = torch.randn(4096, device="cuda", dtype=torch.float32)
    conn.register_mr(src.data_ptr(), src.numel() * src.element_size())
    torch.cuda.synchronize(src.device)

    asyncio.run(conn.rdma_write_cache_async([(key, 0)], 4096 * 4, src.data_ptr()))
    assert conn.check_exist(key)
    conn.close()


def test_get_match_last_index(server):
    config = infinistore.ClientConfig(
        host_addr="127.0.0.1",
        service_port=92345,
        link_type=infinistore.LINK_ETHERNET,
        dev_name="RDMA_DEV[0]",
        connection_type=infinistore.TYPE_RDMA,
    )
    conn = infinistore.InfinityConnection(config)
    conn.connect()
    src = torch.randn(4096, device="cuda", dtype=torch.float32)
    torch.cuda.synchronize(src.device)

    conn.register_mr(src.data_ptr(), src.numel() * src.element_size())
    asyncio.run(
        conn.rdma_write_cache_async(
            [("key1", 0), ("key2", 1024), ("key3", 2048)], 1024 * 4, src.data_ptr()
        )
    )
    assert conn.get_match_last_index(["A", "B", "C", "key1", "D", "E"]) == 3
    conn.close()


def test_key_not_found(server):
    config = infinistore.ClientConfig(
        host_addr="127.0.0.1",
        service_port=92345,
        link_type=infinistore.LINK_ETHERNET,
        dev_name=f"{RDMA_DEV[0]}",
        connection_type=infinistore.TYPE_RDMA,
    )
    conn = infinistore.InfinityConnection(config)

    async def run():
        try:
            await conn.connect_async()
            key = "not_exist_key"
            dst = torch.randn(4096, device="cuda", dtype=torch.float32)
            conn.register_mr(dst.data_ptr(), dst.numel() * dst.element_size())
            # expect raise exception
            with pytest.raises(Exception):
                await conn.rdma_read_cache_async([(key, 0)], 4096 * 4, dst.data_ptr())
        finally:
            conn.close()

    asyncio.run(run())


def test_upload_cpu_download_gpu(server):
    src_config = infinistore.ClientConfig(
        host_addr="127.0.0.1",
        service_port=92345,
        link_type=infinistore.LINK_ETHERNET,
        dev_name=f"{RDMA_DEV[0]}",
        connection_type=infinistore.TYPE_RDMA,
    )
    dst_config = infinistore.ClientConfig(
        host_addr="127.0.0.1",
        service_port=92345,
        link_type=infinistore.LINK_ETHERNET,
        dev_name=f"{RDMA_DEV[0]}",
        connection_type=infinistore.TYPE_RDMA,
    )
    src_conn = infinistore.InfinityConnection(src_config)
    src_conn.connect()

    dst_conn = infinistore.InfinityConnection(dst_config)
    dst_conn.connect()

    key = generate_random_string(5)
    src = torch.randn(4096, dtype=torch.float32, device="cpu")
    # NOTE: not orch.cuda.synchronize required for CPU tensor
    src_conn.register_mr(src.data_ptr(), src.numel() * src.element_size())

    dst = torch.zeros(4096, dtype=torch.float32, device="cuda:0")
    dst_conn.register_mr(dst.data_ptr(), dst.numel() * dst.element_size())

    async def run():
        await src_conn.rdma_write_cache_async([(key, 0)], 4096 * 4, src.data_ptr())
        await dst_conn.rdma_read_cache_async([(key, 0)], 4096 * 4, dst.data_ptr())
        assert torch.equal(src, dst.cpu())

    asyncio.run(run())
    src_conn.close()
    dst_conn.close()


def test_async_api(server):
    config = infinistore.ClientConfig(
        host_addr="127.0.0.1",
        service_port=92345,
        link_type=infinistore.LINK_ETHERNET,
        dev_name=f"{RDMA_DEV[0]}",
        connection_type=infinistore.TYPE_RDMA,
    )
    conn = infinistore.InfinityConnection(config)

    # use asyncio
    async def run():
        await conn.connect_async()
        key = generate_random_string(5)
        src = torch.randn(4096, device="cuda", dtype=torch.float32)
        dst = torch.zeros(4096, device="cuda", dtype=torch.float32)

        def register_mr():
            conn.register_mr(src.data_ptr(), src.numel() * src.element_size())
            conn.register_mr(dst.data_ptr(), dst.numel() * dst.element_size())

        await asyncio.to_thread(register_mr)
        await conn.rdma_write_cache_async([(key, 0)], 4096 * 4, src.data_ptr())
        await conn.rdma_read_cache_async([(key, 0)], 4096 * 4, dst.data_ptr())

        assert torch.equal(src, dst)
        conn.close()

    asyncio.run(run())


def test_read_non_exist_key(server):
    config = infinistore.ClientConfig(
        host_addr="127.0.0.1",
        service_port=92345,
        link_type=infinistore.LINK_ETHERNET,
        dev_name=f"{RDMA_DEV[0]}",
        connection_type=infinistore.TYPE_RDMA,
    )

    async def run():
        conn = infinistore.InfinityConnection(config)
        try:
            await conn.connect_async()
            dst = torch.zeros(4096, device="cuda", dtype=torch.float32)
            await asyncio.to_thread(
                conn.register_mr, dst.data_ptr(), dst.numel() * dst.element_size()
            )
            with pytest.raises(infinistore.InfiniStoreKeyNotFound):
                await conn.rdma_read_cache_async(
                    [("non_exist_key", 0)], 4096 * 4, dst.data_ptr()
                )
        finally:
            conn.close()

    asyncio.run(run())


@pytest.mark.benchmark
def test_benchmark(server):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    result = subprocess.run(
        [
            "python",
            f"{current_dir}/benchmark.py",
            "--service-port",
            "92345",
            "--dev-name",
            f"{RDMA_DEV[0]}",
            "--link-type",
            "Ethernet",
            "--size",
            "100",
            "--block-size",
            "32",
            "--iteration",
            "10",
            "--rdma",
        ],
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    assert result.returncode == 0


@pytest.mark.parametrize("test_dtype", [torch.float32])
def test_delete_keys(server, test_dtype):
    BLOCK_SIZE = 4096
    # The count of elements in a key indexed region
    BLOB_SIZE = 1024
    KEY_COUNT = 3

    config = infinistore.ClientConfig(
        host_addr="127.0.0.1",
        service_port=92345,
        link_type=infinistore.LINK_ETHERNET,
        dev_name=f"{RDMA_DEV[0]}",
        connection_type=infinistore.TYPE_RDMA,
    )
    conn = infinistore.InfinityConnection(config)
    conn.connect()

    src_tensor = torch.randn(BLOCK_SIZE, device="cuda", dtype=test_dtype)
    keys = [generate_random_string(10) for i in range(KEY_COUNT)]

    conn.register_mr(
        src_tensor.data_ptr(), src_tensor.numel() * src_tensor.element_size()
    )
    element_size = torch._utils._element_size(test_dtype)

    async def run():
        block_offsets = [
            (keys[i], i * BLOB_SIZE * element_size) for i in range(KEY_COUNT)
        ]
        await conn.rdma_write_cache_async(
            block_offsets, BLOB_SIZE * element_size, src_tensor.data_ptr()
        )

    asyncio.run(run())

    # Check all the keys exist
    for i in range(KEY_COUNT):
        assert conn.check_exist(keys[i])

    # Delete the keys at index 0 and 2
    assert conn.delete_keys([keys[0], keys[2]]) == 2

    # Verify the correctness
    assert conn.check_exist(keys[1])
    assert not conn.check_exist(keys[0])
    assert not conn.check_exist(keys[2])
    conn.close()


def get_ptr(mv: memoryview):
    return ctypes.addressof(ctypes.c_char.from_buffer(mv))


def parse_prometheus_metrics(text_content):
    metrics = {}
    for line in text_content.splitlines():
        if line.startswith('#') or not line.strip():
            continue
        # Example line: infinistore_items_total 0.0
        # More complex example: http_requests_total{method="post",code="200"} 1027
        # For now, we assume simple metrics without labels for simplicity in parsing
        match = re.match(r'(\w+)(\{[^\}]+\})?\s+([\d\.\+\-eE]+)', line)
        if match:
            metric_name = match.group(1)
            # labels = match.group(2) # Ignoring labels for now
            value = float(match.group(3))
            metrics[metric_name] = value
    return metrics


def test_metrics_functionality(server_with_metrics):
    _, metrics_endpoint_url = server_with_metrics 

    # a. Initial check of the /metrics endpoint
    try:
        response = requests.get(metrics_endpoint_url, timeout=5)
    except requests.exceptions.ConnectionError as e:
        assert False, f"Failed to connect to metrics endpoint: {metrics_endpoint_url}. Error: {e}"
    
    assert response.status_code == 200
    assert "text/plain" in response.headers["Content-Type"]
    
    initial_metrics = parse_prometheus_metrics(response.text)
    print("Initial metrics:", initial_metrics) # For debugging
    
    assert "infinistore_items_total" in initial_metrics
    assert "infinistore_memory_allocated_bytes" in initial_metrics # Assuming MM is initialized
    # Initial value of items should be 0 if store is empty
    assert initial_metrics.get("infinistore_items_total", -1) == 0.0

    # b. Test metrics after some operations (e.g., TCP write and read)
    client_config = infinistore.ClientConfig(
        host_addr="127.0.0.1",
        service_port=92345, # Matches server fixture (service port for client)
        connection_type=infinistore.TYPE_TCP,
    )
    conn = None
    try:
        conn = infinistore.InfinityConnection(client_config)
        conn.connect()
        
        key1 = "metrics_test_key1"
        size1 = 1024
        data1 = bytearray(random.getrandbits(8) for _ in range(size1))
        
        # Perform a write
        conn.tcp_write_cache(key1, get_ptr(data1), size1)

        response_after_write = requests.get(metrics_endpoint_url, timeout=5)
        assert response_after_write.status_code == 200
        metrics_after_write = parse_prometheus_metrics(response_after_write.text)
        print("Metrics after write:", metrics_after_write)

        assert metrics_after_write.get("infinistore_items_total", 0) == initial_metrics.get("infinistore_items_total", 0) + 1
        assert metrics_after_write.get("infinistore_tcp_put_requests_total", 0) >= initial_metrics.get("infinistore_tcp_put_requests_total", 0) + 1
        # memory_used_bytes should also increase
        assert metrics_after_write.get("infinistore_memory_used_bytes", 0) > initial_metrics.get("infinistore_memory_used_bytes", -1)


        # Perform a read (hit)
        read_data1 = conn.tcp_read_cache(key1)
        assert read_data1 == data1 # Verify data integrity as a sanity check

        response_after_read_hit = requests.get(metrics_endpoint_url, timeout=5)
        assert response_after_read_hit.status_code == 200
        metrics_after_read_hit = parse_prometheus_metrics(response_after_read_hit.text)
        print("Metrics after read hit:", metrics_after_read_hit)

        assert metrics_after_read_hit.get("infinistore_tcp_get_requests_total", 0) >= metrics_after_write.get("infinistore_tcp_get_requests_total", 0) + 1
        assert metrics_after_read_hit.get("infinistore_tcp_get_hits_total", 0) >= metrics_after_write.get("infinistore_tcp_get_hits_total", 0) + 1

        # Perform a read for a non-existent key (miss)
        key_non_existent = "metrics_test_key_non_existent"
        try:
            conn.tcp_read_cache(key_non_existent)
        except infinistore.InfiniStoreKeyNotFound: 
            pass # Expected

        response_after_read_miss = requests.get(metrics_endpoint_url, timeout=5)
        assert response_after_read_miss.status_code == 200
        metrics_after_read_miss = parse_prometheus_metrics(response_after_read_miss.text)
        print("Metrics after read miss:", metrics_after_read_miss)
        
        assert metrics_after_read_miss.get("infinistore_tcp_get_requests_total", 0) >= metrics_after_read_hit.get("infinistore_tcp_get_requests_total", 0) + 1
        assert metrics_after_read_miss.get("infinistore_tcp_get_misses_total", 0) >= metrics_after_read_hit.get("infinistore_tcp_get_misses_total", 0) + 1

    finally:
        if conn:
            conn.close()


def test_simple_tcp_read_write(server):
    config = infinistore.ClientConfig(
        host_addr="127.0.0.1",
        service_port=92345,
        connection_type=infinistore.TYPE_TCP,
    )

    try:
        conn = infinistore.InfinityConnection(config)
        conn.connect()
        key = generate_random_string(10)
        size = 256 * 1024
        src = bytearray(size)
        for i in range(size):
            src[i] = i % 200
        conn.tcp_write_cache(key, get_ptr(src), len(src))

        dst = conn.tcp_read_cache(key)
        assert len(dst) == len(src)
        for i in range(len(src)):
            assert dst[i] == src[i]
    finally:
        conn.close()


def test_overwrite_tcp(server):
    config = infinistore.ClientConfig(
        host_addr="127.0.0.1",
        service_port=92345,
        connection_type=infinistore.TYPE_TCP,
    )

    try:
        conn = infinistore.InfinityConnection(config)
        conn.connect()
        key = generate_random_string(10)
        size = 256 * 1024
        src = bytearray(size)
        for i in range(size):
            src[i] = i % 200
        conn.tcp_write_cache(key, get_ptr(src), len(src))
        dst = conn.tcp_read_cache(key)
        assert len(dst) == len(src)
        for i in range(len(src)):
            assert dst[i] == src[i]

        # overwrite the key
        src = bytearray(size)
        for i in range(size):
            src[i] = i % 100
        conn.tcp_write_cache(key, get_ptr(src), len(src))
        dst = conn.tcp_read_cache(key)
        assert len(dst) == len(src)
    finally:
        conn.close()
