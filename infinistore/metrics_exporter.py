from prometheus_client import Gauge
from prometheus_client.core import CollectorRegistry # Changed from REGISTRY

# Import the C++ bindings
try:
    import _infinistore
except ImportError:
    print("Warning: _infinistore module not found. Using MockInfiniStore for metrics_exporter.")
    class MockInfiniStore:
        def __init__(self):
            self._mock_data = {
                'items_total': 0, 'memory_allocated_bytes': 1024 * 1024 * 100, 'memory_used_bytes': 0,
                'lru_queue_size_items': 0, 'tcp_put_requests_total': 0,
                'tcp_get_requests_total': 0, 'tcp_get_hits_total': 0, 'tcp_get_misses_total': 0,
                'rdma_write_requests_total': 0, 'rdma_read_requests_total': 0,
                'rdma_read_hits_total': 0, 'rdma_read_misses_total': 0, 'evictions_total': 0
            }
            # Simulate some initial activity or state for mock
            self._mock_data['items_total'] = 5 # Example
            self._mock_data['memory_used_bytes'] = 5 * 1024 # Example

        def __getattr__(self, name):
            if name.startswith("get_"):
                metric_name = name[4:]
                # Ensure all expected metrics have a default value in _mock_data
                return lambda: self._mock_data.get(metric_name, 0)
            raise AttributeError(f"MockInfiniStore has no attribute '{name}'")
    _infinistore = MockInfiniStore()


class InfiniStoreCollector:
    def __init__(self):
        # Define Prometheus metrics. All are Gauges as they represent current totals from C++.
        self.metrics = {
            'items_total': Gauge('infinistore_items_total', 'Total number of items currently in the store'),
            'memory_allocated_bytes': Gauge('infinistore_memory_allocated_bytes', 'Total memory allocated by the memory manager'),
            'memory_used_bytes': Gauge('infinistore_memory_used_bytes', 'Total memory used by items in the store'),
            'lru_queue_size_items': Gauge('infinistore_lru_queue_size_items', 'Current number of items in the LRU queue'),
            'tcp_put_requests_total': Gauge('infinistore_tcp_put_requests_total', 'Total number of TCP PUT requests'),
            'tcp_get_requests_total': Gauge('infinistore_tcp_get_requests_total', 'Total number of TCP GET requests'),
            'tcp_get_hits_total': Gauge('infinistore_tcp_get_hits_total', 'Total number of successful TCP GET requests (hits)'),
            'tcp_get_misses_total': Gauge('infinistore_tcp_get_misses_total', 'Total number of unsuccessful TCP GET requests (misses)'),
            'rdma_write_requests_total': Gauge('infinistore_rdma_write_requests_total', 'Total number of RDMA write requests (batches)'),
            'rdma_read_requests_total': Gauge('infinistore_rdma_read_requests_total', 'Total number of RDMA read requests (batches)'),
            'rdma_read_hits_total': Gauge('infinistore_rdma_read_hits_total', 'Total number of RDMA read key hits'),
            'rdma_read_misses_total': Gauge('infinistore_rdma_read_misses_total', 'Total number of RDMA read key misses'),
            'evictions_total': Gauge('infinistore_evictions_total', 'Total number of items evicted from the cache')
        }

    def collect(self):
        # Fetch values from C++ bindings and update Prometheus Gauges
        # Using a loop to avoid repetitive getattr calls and handle missing getters gracefully
        for metric_name, gauge_metric in self.metrics.items():
            getter_name = f"get_{metric_name}"
            try:
                value = getattr(_infinistore, getter_name)()
                gauge_metric.set(value)
            except AttributeError:
                # This might happen if _infinistore (especially Mock) doesn't have a getter
                print(f"Warning: Metric getter {getter_name} not found in _infinistore module. Setting to 0.")
                gauge_metric.set(0) 
            except Exception as e:
                print(f"Error calling {getter_name}: {e}. Setting to 0.")
                gauge_metric.set(0)
        
        for metric in self.metrics.values():
            yield metric

# Create a global registry and register the collector
metrics_registry = CollectorRegistry()
metrics_registry.register(InfiniStoreCollector())

# Removed start_metrics_server function
# Removed if __name__ == '__main__' block
