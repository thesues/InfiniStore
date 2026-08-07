import infinistore._infinistore as _infinistore

# sphinx-doc will mock infinistore._infinistore, it has to be written like this

import os
import subprocess
import asyncio
from functools import singledispatchmethod
from typing import Optional, Union, List, Tuple
import socket
import threading

os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["NUMEXPR_NUM_THREADS"] = "1"
os.environ["OMP_NUM_THREADS"] = "1"
import numpy as np


def _uv_loop_ptr(loop):
    """
    Return the uv_loop_t* behind a uvloop event loop, the C++ side runs its
    connections on it.
    """
    try:
        from uvloop.loop import libuv_get_loop_t_ptr
    except ImportError:
        raise Exception("infinistore requires uvloop")

    import ctypes
    from ctypes import pythonapi, c_void_p, py_object

    try:
        capsule = libuv_get_loop_t_ptr(loop)
    except (TypeError, AttributeError):
        raise Exception(f"infinistore requires a uvloop event loop, got {type(loop)}")

    PyCapsule_GetPointer = pythonapi.PyCapsule_GetPointer
    PyCapsule_GetPointer.restype = c_void_p
    PyCapsule_GetPointer.argtypes = [py_object, ctypes.c_char_p]
    return PyCapsule_GetPointer(capsule, None)


_background_loop = None
_background_lock = threading.Lock()


def _get_background_loop():
    """
    A uvloop event loop running in a daemon thread of its own.

    Connections established with the synchronous API live on it, so blocking
    callers do not need an event loop of their own. It is process wide on
    purpose: a coroutine may use several connections.
    """
    global _background_loop
    with _background_lock:
        if _background_loop is not None:
            return _background_loop

        import uvloop

        loop = uvloop.new_event_loop()
        ready = threading.Event()

        def _run():
            asyncio.set_event_loop(loop)
            loop.call_soon(ready.set)
            loop.run_forever()

        thread = threading.Thread(target=_run, name="infinistore-io", daemon=True)
        thread.start()
        ready.wait()

        _background_loop = loop
        return _background_loop


def _submit(loop, coro):
    """
    Run a coroutine on `loop` from any thread and wait for it.

    The coroutine body runs on the loop thread, which is what the connections
    require, the calling thread only blocks on the result.
    """
    try:
        running = asyncio.get_running_loop()
    except RuntimeError:
        running = None

    if running is loop:
        coro.close()
        raise Exception(
            "this would block the event loop the connection runs on, "
            "use the *_async variant here"
        )
    return asyncio.run_coroutine_threadsafe(coro, loop).result()


def run(coro):
    """
    Run a coroutine from blocking code, on the loop the connections established
    with the synchronous connect() live on.

    Use it instead of asyncio.run(): a connection is bound to one loop, and
    asyncio.run() creates a new one every time.
    """
    return _submit(_get_background_loop(), coro)


def _settle(future, ret, message):
    """Resolve a future from a C++ callback: negative means failure."""
    if future.done():
        return
    if ret < 0:
        future.set_exception(Exception(f"{message}, ret = {ret}"))
    else:
        future.set_result(ret)


# connection type: default is RDMA
TYPE_RDMA = "RDMA"
TYPE_TCP = "TCP"
# rdma link type
LINK_ETHERNET = "Ethernet"
LINK_IB = "IB"


# Define exceptions which can be caught by the client such as KeyNotFound


class InfiniStoreException(Exception):
    pass


class InfiniStoreKeyNotFound(InfiniStoreException):
    pass


class ClientConfig(_infinistore.ClientConfig):
    """
    ClientConfig is a configuration class for the Infinistore client.

    Attributes:
        connection_type (str): The type of connection to use (e.g. TYPE_RDMA).
        host_addr (str): The address of the host.
        dev_name (str): The name of the device (default is "mlx5_1").
        ib_port (int): The port number of the InfiniBand device (default is 1).
        link_type (str): The type of link (default is "IB").
        service_port (int): The port number of the service.
        log_level (str): The logging level (default is "warning").
    """

    def __init__(self, **kwargs):
        super().__init__()
        self.connection_type = kwargs.get("connection_type", None)
        self.host_addr = kwargs.get("host_addr", None)
        self.dev_name = kwargs.get("dev_name", "mlx5_1")
        self.ib_port = kwargs.get("ib_port", 1)
        self.link_type = kwargs.get("link_type", "IB")
        self.service_port = kwargs.get("service_port", None)
        # get log from system env
        # if log level is not set in Config and system env is not set either, use warning as default
        if "INFINISTORE_LOG_LEVEL" in os.environ:
            self.log_level = os.environ["INFINISTORE_LOG_LEVEL"]
        else:
            self.log_level = kwargs.get("log_level", "warning")
        self.hint_gid_index = kwargs.get("hint_gid_index", -1)

    def __repr__(self):
        return (
            f"ServerConfig(service_port={self.service_port}, "
            f"log_level='{self.log_level}', host_addr='{self.host_addr}', "
            f"connection_type='{self.connection_type}')"
            f"dev_name='{self.dev_name}', ib_port={self.ib_port}, link_type='{self.link_type}'"
        )

    def verify(self):
        if self.connection_type not in [TYPE_RDMA, TYPE_TCP]:
            raise Exception("Invalid connection type")
        if self.host_addr == "":
            raise Exception("Host address is empty")
        if self.service_port == 0:
            raise Exception("Service port is 0")
        if self.log_level not in ["error", "debug", "info", "warning"]:
            raise Exception("log level should be error, debug, info or warning")
        if self.ib_port < 1:
            raise Exception("ib port of device should be greater than 0")
        if self.connection_type == TYPE_RDMA and self.link_type not in [
            "IB",
            "Ethernet",
        ]:
            raise Exception("link type should be IB or Ethernet for RDMA connection")


class ServerConfig(_infinistore.ServerConfig):
    class ServerConfig:
        """
        ServerConfig is a configuration class for the server settings.

        Attributes:
            manage_port (int): The port used for management. Defaults to 0.
            service_port (int): The port used for service. Defaults to 0.
            log_level (str): The logging level. Defaults to "warning".
            dev_name (str): The device name. Defaults to "mlx5_1".
            ib_port (int): The InfiniBand port number. Defaults to 1.
            link_type (str): The type of link. Defaults to "IB".
            prealloc_size (int): The preallocation size. Defaults to 16.
            minimal_allocate_size (int): The minimal allocation size. Defaults to 64.
            auto_increase (bool): indicate if infinistore will be automatically increased. 10GB each time. Default False.
            hint_gid_index (int): The hint GID index. Defaults to -1.

        """

    def __init__(self, **kwargs):
        super().__init__()
        self.manage_port = kwargs.get("manage_port", 0)
        self.service_port = kwargs.get("service_port", 0)
        self.log_level = kwargs.get("log_level", "warning")
        self.dev_name = kwargs.get("dev_name", "mlx5_1")
        self.ib_port = kwargs.get("ib_port", 1)
        self.link_type = kwargs.get("link_type", "IB")
        self.prealloc_size = kwargs.get("prealloc_size", 16)
        self.minimal_allocate_size = kwargs.get("minimal_allocate_size", 64)
        self.auto_increase = kwargs.get("auto_increase", False)
        self.evict_min_threshold = kwargs.get("evict_min_threshold", 0.6)
        self.evict_max_threshold = kwargs.get("evict_max_threshold", 0.8)
        self.evict_interval = kwargs.get("evict_interval", 5)
        self.hint_gid_index = kwargs.get("hint_gid_index", -1)

    def __repr__(self):
        return (
            f"ServerConfig: service_port={self.service_port}, manage_port={self.manage_port}, "
            f"log_level='{self.log_level}', "
            f"dev_name='{self.dev_name}', ib_port={self.ib_port}, link_type='{self.link_type}', "
            f"prealloc_size={self.prealloc_size}, minimal_allocate_size={self.minimal_allocate_size}, "
            f"auto_increase={self.auto_increase}, evict_min_threshold={self.evict_min_threshold}, "
            f"evict_max_threshold={self.evict_max_threshold}, evict_interval={self.evict_interval}, "
            f"hint_gid_index={self.hint_gid_index}"
        )

    def verify(self):
        if self.service_port == 0:
            raise Exception("Service port is 0")
        if self.manage_port == 0:
            raise Exception("Manage port is 0")
        if self.log_level not in ["error", "debug", "info", "warning"]:
            raise Exception("log level should be error, debug, info or warning")
        if self.ib_port < 1:
            raise Exception("ib port of device should be greater than 0")
        if self.link_type not in ["IB", "Ethernet"]:
            raise Exception("link type should be IB or Ethernet")
        if self.minimal_allocate_size < 16:
            raise Exception("minimal allocate size should be greater than 16")


class Logger:
    @staticmethod
    def info(msg):
        _infinistore.log_msg("info", str(msg))

    @staticmethod
    def debug(msg):
        _infinistore.log_msg("debug", str(msg))

    @staticmethod
    def error(msg):
        _infinistore.log_msg("error", str(msg))

    @staticmethod
    def warn(msg):
        _infinistore.log_msg("warning", str(msg))

    @staticmethod
    def set_log_level(level):
        _infinistore.set_log_level(level)


def get_kvmap_len():
    """
    Returns the length of the key-value map in the infinistore.

    This function calls the underlying _infinistore.get_kvmap_len() method to
    get the length of the key-value map.

    Returns:
        The result of the _infinistore.get_kvmap_len() method call.
    """
    return _infinistore.get_kvmap_len()


def purge_kv_map():
    """
    Purges the key-value map in the infinistore.

    This function calls the underlying _infinistore.purge_kv_map() method to
    clear all entries in the key-value map, effectively resetting it.

    Returns:
        The result of the _infinistore.purge_kv_map() method call.
    """
    return _infinistore.purge_kv_map()


def register_server(loop, config: ServerConfig):
    """
    Registers a server with the given event loop and configuration.

    This function is intended to be used internally and should not be called by clients directly.

    Args:
        loop: The event loop to register the server with.
        config (ServerConfig): The configuration for the server.

    Raises:
        Exception: If the server registration fails.
    """
    # client does not need to call this function
    if _infinistore.register_server(_uv_loop_ptr(loop), config) < 0:
        raise Exception("Failed to register server")


def evict_cache(min_threshold: float, max_threshold: float):
    """
    Evicts the cache in the infinistore.

    This function calls the underlying _infinistore.evict_cache() method to
    clear all entries in the cache, effectively resetting it.

    Returns:
        The result of the _infinistore.evict_cache() method call.
    """
    if min_threshold >= max_threshold:
        raise Exception("min_threshold should be less than max_threshold")
    if min_threshold > 1 or min_threshold < 0:
        raise Exception("min_threshold should be in (0, 1)")
    if max_threshold > 1 or max_threshold < 0:
        raise Exception("max_threshold should be in (0, 1)")

    return _infinistore.evict_cache(min_threshold, max_threshold)


def _kernel_modules():
    modules = set()
    try:
        with open("/proc/modules", "r") as f:
            for line in f:
                sep = line.find(" ")
                if sep != -1:
                    modules.add(line[:sep])
    except IOError as e:
        raise Exception(f"can not read /proc/modules: {e}")
    return modules


def _check_rdma_devices_ibv():
    try:
        result = subprocess.run(
            ["ibv_devinfo"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        if result.returncode != 0:
            return
        output = result.stdout
        devices = output.split("\n\n")
        port_active = False
        for device_info in devices:
            if "hca_id" in device_info:
                if "PORT_ACTIVE" in device_info:
                    port_active = True
                    break
        if port_active is False:
            raise Exception("No active RDMA device found")
    except FileNotFoundError:
        raise Exception(
            "command ibv_devinfo not found, make sure RDMA tools are installed; for ubuntu, run apt install ibv_devinfo"
        )


class InfinityConnection:
    """
    A class to manage connections and data transfers with an Infinistore instance using RDMA connections.

    Attributes:
        conn (_infinistore.Connection): The connection object to the Infinistore instance.
        rdma_connected (bool): Indicates if connected to a remote instance via RDMA.
        config (ClientConfig): Configuration object for the connection.
    """

    OP_RDMA_READ = "A"

    def __init__(self, config: ClientConfig):
        config.verify()
        self.conn = _infinistore.Connection()
        self.rdma_connected = False
        self.config = config

        # the connection lives on this uv loop. It is the caller's loop when there
        # is a running one, otherwise the process wide fallback loop.
        self._loop = None

        # used for async io
        self.semaphore = asyncio.BoundedSemaphore(128)
        Logger.set_log_level(config.log_level)

    def _ensure_loop(self):
        """
        Pick the loop this connection runs on: the caller's if one is running,
        otherwise a private one which the sync API drives with run_until_complete.
        """
        if self._loop is not None:
            return self._loop

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is None:
            loop = _get_background_loop()

        self._loop = loop
        return loop

    def _bound_loop(self):
        """The loop the connection is on, checked against the running one."""
        if self._loop is None:
            raise Exception("not connected")
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is not None and running is not self._loop:
            raise Exception(
                "this connection is bound to another event loop, connect it from "
                "the loop you are using it on"
            )
        return self._loop

    def _is_loop_thread(self):
        try:
            return asyncio.get_running_loop() is self._loop
        except RuntimeError:
            return False

    def _call_on_loop(self, fn):
        """
        Run a plain function on the connection's loop thread.

        Only for calls which complete right away, like closing the handles or
        registering a memory region. Inline when the caller is already on the
        loop, otherwise handed over to it.
        """
        if self._loop is None or self._is_loop_thread():
            return fn()

        async def _wrapper():
            return fn()

        return _submit(self._loop, _wrapper())

    def _run_sync(self, coro):
        """
        Run a coroutine on this connection's loop and wait for it.

        Callable from any thread except the loop's own: the work happens on the
        loop thread, the caller only blocks on the result.
        """
        return _submit(self._ensure_loop(), coro)

    async def connect_async(self):
        """
        Asynchronously establishes a connection based on the configuration.

        The connection runs on the caller's event loop, which has to be a uvloop
        one, and stays bound to it.

        Raises:
            Exception: If the initialization of the remote connection fails.
            Exception: If the setup of the RDMA connection fails.
        """
        loop = self._ensure_loop()
        self.config.host_addr = self.resolve_hostname(self.config.host_addr)

        future = loop.create_future()

        def _callback(ret):
            loop.call_soon_threadsafe(
                _settle, future, ret, "Failed to initialize remote connection"
            )

        if self.conn.init_connection(self.config, _uv_loop_ptr(loop), _callback) < 0:
            raise Exception("Failed to initialize remote connection")
        await future

        if self.config.connection_type == TYPE_RDMA:
            rdma_future = loop.create_future()

            def _rdma_callback(ret):
                loop.call_soon_threadsafe(
                    _settle, rdma_future, ret, "Failed to setup RDMA connection"
                )

            if self.conn.setup_rdma(self.config, _rdma_callback) < 0:
                raise Exception("Failed to setup RDMA connection")
            await rdma_future
            self.rdma_connected = True

    @staticmethod
    def resolve_hostname(hostname: str) -> str:
        try:
            socket.inet_aton(hostname)
            return hostname
        except socket.error:
            pass

        # If the hostname is not an IP address, resolve it
        Logger.info(f"Resolving hostname: {hostname}")
        try:
            infos = socket.getaddrinfo(
                hostname, None, socket.AF_INET, socket.SOCK_STREAM
            )
            # Return the first resolved IPv4 address
            return infos[0][4][0]
        except socket.gaierror as e:
            raise Exception(f"Failed to resolve hostname '{hostname}': {e}")

    def connect(self):
        """
        Establishes a connection to the Infinistore instance based on the configuration.

        This drives a private event loop, so it can not be called while an event
        loop is running, use connect_async() there.

        Raises:
            Exception: If already connected to a remote instance.
            Exception: If failed to initialize remote connection.
            Exception: If failed to setup RDMA connection.
        """
        if self.rdma_connected:
            raise Exception("Already connected to remote instance")

        self._run_sync(self.connect_async())

    def close(self):
        """
        Closes the connection to the Infinistore instance.

        Callable from any thread and from inside a coroutine: the handles are
        always closed on the loop thread.
        """
        self._call_on_loop(self.conn.close)

    async def tcp_read_cache_async(self, key: str, **kwargs) -> np.ndarray:
        """
        Retrieve a single cached item over the TCP connection.

        Parameters:
        key (str): The key associated with the cached item.

        Returns:
        np.ndarray: The cached item retrieved from the TCP connection.

        Raises:
            InfiniStoreKeyNotFound: If the key is not in the store.
            Exception: If the read fails.
        """
        loop = self._bound_loop()
        future = loop.create_future()

        def _callback(code, array):
            if future.done():
                return
            if code == 404:
                loop.call_soon_threadsafe(
                    future.set_exception, InfiniStoreKeyNotFound(f"key not found: {key}")
                )
            elif code != 200:
                loop.call_soon_threadsafe(
                    future.set_exception,
                    Exception(f"Failed to read from infinistore, ret = {code}"),
                )
            else:
                loop.call_soon_threadsafe(future.set_result, array)

        if self.conn.r_tcp(key, _callback) < 0:
            raise Exception("Failed to read from infinistore")
        return await future

    async def tcp_write_cache_async(self, key: str, ptr: int, size: int, **kwargs):
        """
        Write a single cache entry to the remote memory over TCP.

        Args:
            key (str): The key of the cache entry to write.
            ptr (int): Pointer to the data. It has to stay alive until this
                coroutine returns, the data is sent from the event loop.
            size (int): The size of the data to write.

        Raises:
            Exception: If the key is empty, the size is 0, the pointer is 0, or
                the write operation fails.
        """
        if key == "":
            raise Exception("key is empty")
        if size == 0:
            raise Exception("size is 0")
        if ptr == 0:
            raise Exception("ptr is 0")

        loop = self._bound_loop()
        future = loop.create_future()

        def _callback(ret):
            loop.call_soon_threadsafe(
                _settle, future, ret, "Failed to write to infinistore"
            )

        if self.conn.w_tcp(key, ptr, size, _callback) < 0:
            raise Exception("Failed to write to infinistore")
        await future

    def tcp_read_cache(self, key: str, **kwargs) -> np.ndarray:
        """
        Retrieve a single cached item from the TCP connection.

        Parameters:
        key (str): The key associated with the cached item.
        ``**kwargs``: Additional keyword arguments.

        Returns:
        np.ndarray: The cached item retrieved from the TCP connection.
        """
        return self._run_sync(self.tcp_read_cache_async(key, **kwargs))

    def tcp_write_cache(self, key: str, ptr: int, size: int, **kwargs):
        """
        Writes a single cache entry to the remote memory using TCP.

        Args:
            key (str): The key of the cache entry to write.
            ptr (int): The pointer to the memory location holding the data.
            size (int): The size of the data to write.

        Raises:
            Exception: If the key is empty, the size is 0, the pointer is 0, or
                the write operation fails.
        """
        return self._run_sync(self.tcp_write_cache_async(key, ptr, size, **kwargs))

    async def rdma_write_cache_async(
        self, blocks: List[Tuple[str, int]], block_size: int, ptr: int
    ):
        """
        Asynchronously writes data to the infinistore cache using RDMA.

        This function performs an RDMA write operation to the infinistore cache.
        It requires an active RDMA connection and uses a semaphore to limit
        concurrent writes. The operation is completed asynchronously.

        Args:
            blocks (List[Tuple[str, int]]): A list of tuples where each tuple
                contains a key (str) and an offset (int) representing the data
                blocks to be written.
            block_size (int): The size of each block to be written, in bytes.
            ptr (int): A pointer to the memory location containing the data to
                be written.

        Raises:
            Exception: If RDMA is not connected or if the write operation fails.
        Returns:
            int: The result code of the write operation if successful.

        Notes:
            - If the RDMA connection is not established, an exception is raised.
            - The semaphore ensures that the number of concurrent writes is
              limited.
        """
        if not self.rdma_connected:
            raise Exception("this function is only valid for connected rdma")

        loop = self._bound_loop()
        await self.semaphore.acquire()
        future = loop.create_future()

        keys, offsets = zip(*blocks)

        def _callback(code):
            if code != 200:
                loop.call_soon_threadsafe(
                    future.set_exception,
                    Exception(f"Failed to write to infinistore, ret = {code}"),
                )
            else:
                loop.call_soon_threadsafe(future.set_result, code)
            self.semaphore.release()

        ret = self.conn.w_rdma_async(
            keys,
            offsets,
            block_size,
            ptr,
            _callback,
        )
        if ret < 0:
            raise Exception(f"Failed to write to infinistore, ret = {ret}")
        return await future

    async def rdma_read_cache_async(
        self, blocks: List[Tuple[str, int]], block_size: int, ptr: int
    ):
        """
        Asynchronously reads data from the RDMA cache.

        This function performs an asynchronous RDMA read operation for the specified
        blocks of data. It requires an active RDMA connection and uses a semaphore
        to limit concurrent operations.

        Args:
            blocks (List[Tuple[str, int]]): A list of tuples where each tuple contains
                a key (str) and an offset (int) specifying the data to be read.
            block_size (int): The size of each block to be read.
            ptr (int): A pointer to the memory location where the data will be stored.

        Raises:
            Exception: If RDMA is not connected or if the RDMA read operation fails.
            InfiniStoreKeyNotFound: If some keys are not found in the RDMA cache.

        Returns:
            int: The result code of the RDMA read operation (e.g., 200 for success).

        Note:
            This function uses a callback mechanism to handle the result of the RDMA
            read operation. The semaphore is released after the operation completes.
        """
        if not self.rdma_connected:
            raise Exception("this function is only valid for connected rdma")
        pass

        loop = self._bound_loop()
        await self.semaphore.acquire()
        future = loop.create_future()

        def _callback(code):
            if code == 404:
                loop.call_soon_threadsafe(
                    future.set_exception, InfiniStoreKeyNotFound("some keys not found")
                )
            elif code != 200:
                loop.call_soon_threadsafe(
                    future.set_exception,
                    Exception(f"Failed to read to infinistore, ret = {code}"),
                )
            else:
                loop.call_soon_threadsafe(future.set_result, code)
            self.semaphore.release()

        keys, offsets = zip(*blocks)
        ret = self.conn.r_rdma_async(
            keys,
            offsets,
            block_size,
            ptr,
            _callback,
        )
        if ret < 0:
            raise Exception(f"Failed to read to infinistore, ret = {ret}")
        return await future

    async def check_exist_async(self, key: str):
        """
        Check if a given key exists in the store.

        Returns:
            bool: True if the key exists, False otherwise.
        """
        loop = self._bound_loop()
        future = loop.create_future()

        def _callback(ret):
            loop.call_soon_threadsafe(
                _settle, future, ret, "Failed to check if this key exists"
            )

        if self.conn.check_exist(key, _callback) < 0:
            raise Exception("Failed to check if this key exists")
        # the server answers 0 when the key is there
        ret = await future
        return ret == 0

    def check_exist(self, key: str):
        """
        Check if a given key exists in the store.

        Args:
            key (str): The key to check for existence.

        Returns:
            bool: True if the key exists, False otherwise.

        Raises:
            Exception: If there is an error checking the key's existence.
        """
        return self._run_sync(self.check_exist_async(key))

    async def get_match_last_index_async(self, keys: List[str]):
        """
        Retrieve the last index of a match for the given keys.

        Returns:
            int: The last index of a match.
        """
        loop = self._bound_loop()
        future = loop.create_future()

        def _callback(ret):
            loop.call_soon_threadsafe(_settle, future, ret, "can't find a match")

        if self.conn.get_match_last_index(keys, _callback) < 0:
            raise Exception("can't find a match")
        return await future

    def get_match_last_index(self, keys: List[str]):
        """
        Retrieve the last index of a match for the given keys.

        Args:
            keys (List[str]): A list of string keys to search for matches.

        Returns:
            int: The last index of a match.

        Raises:
            Exception: If no match is found (i.e., if the return value is negative).
        """
        return self._run_sync(self.get_match_last_index_async(keys))

    @singledispatchmethod
    def register_mr(self, arg: Union[int], size: Optional[int] = None):
        """
        Registers a memory region (MR) for the given argument.

        Args:
            arg (Union[int]): The argument for which the memory region is to be registered.
            size (Optional[int], optional): The size of the memory region to be registered. Defaults to None.

        Raises:
            NotImplementedError: If the type of the argument is not supported.
        """
        raise NotImplementedError(f"not supported: {type(arg)}")

    @register_mr.register
    def _(self, ptr: int, size):
        """
        Registers a memory region (MR) for an integer pointer.

        Args:
            ptr (int): The pointer to the memory region.
            size (int): The size of the memory region.

        Raises:
            Exception: If the RDMA connection is not established.
            Exception: If the memory region registration fails.

        Returns:
            int: The result of the memory region registration.
        """
        if not self.rdma_connected:
            raise Exception("this function is only valid for connected rdma")

        # NOTE: this pins the pages, so it holds up the loop for as long as that
        # takes. It is a setup time call, and it has to happen on the loop
        # because the map it writes is read there when requests are posted.
        ret = self._call_on_loop(lambda: self.conn.register_mr(ptr, size))
        if ret < 0:
            raise Exception("register memory region failed")
        return ret

    async def delete_keys_async(self, keys: List[str]):
        """
        Delete a list of keys.

        Returns:
            int: The count of the deleted keys
        """
        loop = self._bound_loop()
        future = loop.create_future()

        def _callback(ret):
            loop.call_soon_threadsafe(
                _settle,
                future,
                ret,
                "somethings are wrong, not all the specified keys were deleted",
            )

        if self.conn.delete_keys(keys, _callback) < 0:
            raise Exception(
                "somethings are wrong, not all the specified keys were deleted"
            )
        return await future

    def delete_keys(self, keys: List[str]):
        """
        Delete a list of keys

        Args:
            keys (List[str]): The list of string keys to delete

        Returns:
            int: The count of the deleted keys

        Raises:
            Exception: If there is something wrong(return value is -1)
        """
        return self._run_sync(self.delete_keys_async(keys))
