from dubbo import server
from dubbo.client import DubboClient
from dubbo.server import DubboService
from dubbo.errors import DubboError


_paths = []


def setup_module(module):
    server.KazooClient = _MockKazooClient
    server.get_timestamp = _mock_get_timestamp
    server.get_pub_ip = _mock_get_pub_ip
    _MockKazooClient.instances = []
    _paths.clear()


class _MockKazooClient(object):
    instances = []

    def __init__(self, zk):
        self._zk = zk
        self.created = []  # [(path, ephemeral)]
        self.closed = False
        _MockKazooClient.instances.append(self)

    def start(self):
        pass

    def ensure_path(self, path):
        # kazoo ensure_path 是幂等的：已存在时不抛错，仅记录
        if path not in _paths:
            _paths.append(path)

    def create(self, path, ephemeral=False):
        self.created.append((path, ephemeral))

    def close(self):
        self.closed = True


def _mock_get_timestamp():
    return 1234567890


def _mock_get_pub_ip():
    return '10.0.1.120'


def test_dubbo_register():
    service = DubboService(12345, 'unit-test')
    service.register('zk-1.test.corp:2181')
    # 尚未 add_method，无服务可注册
    assert _MockKazooClient.instances == []
    service.add_method('a.service', 'doGet', 'void')
    service.register('zk-1.test.corp:2181')
    # A6: 多次 register 复用同一个 KazooClient，不再每次新建导致连接泄漏
    assert len(_MockKazooClient.instances) == 1
    client = _MockKazooClient.instances[0]
    assert _paths == ['/dubbo/a.service/providers']
    expected_url = (r'dubbo%3A%2F%2F10.0.1.120%3A12345%2Fa.service'
                    r'%3Fanyhost%3Dtrue%26application%3Dunit-test%26dubbo%3D2.5.3'
                    r'%26interface%3Da.service%26methods%3DdoGet%26pid%3D1'
                    r'%26revision%3D1.0.0%26side%3Dprovider%26timestamp%3D1234567890'
                    r'%26version%3D1.0.0')
    node = f'/dubbo/a.service/providers/{expected_url}'
    # A6: provider 节点必须是 ephemeral（服务下线自动清理）
    assert (node, True) in client.created

    _paths.clear()
    service.register('zk-1.test.corp:2181', '1.1')
    # 仍是同一个 client，注册第二版节点
    assert len(_MockKazooClient.instances) == 1
    expected_url_v2 = expected_url.replace('version%3D1.0.0', 'version%3D1.1').replace('pid%3D1', 'pid%3D2')
    assert (f'/dubbo/a.service/providers/{expected_url_v2}', True) in client.created


def test_dubbo_stop_closes_zk_client():
    _MockKazooClient.instances = []
    service = DubboService(12359, 'unit-test')
    service.add_method('a.service', 'doGet', 'void')
    service.register('zk-1.test.corp:2181')
    client = _MockKazooClient.instances[-1]
    assert not client.closed
    # 未 start 的服务也应能安全 stop，且释放 zk 连接
    service.stop()
    assert client.closed


def test_dubbo_handler():
    service = DubboService(12358, 'unittest')

    def _multi_2_handler(num):
        return num * 2

    def _add_handler(a, b):
        return a + b

    def _exp_handler(num):
        return num ** 2

    def _divide_handler(a, b):
        if b == 0:
            raise DubboError(40, 'divide by zero')
        return a / b

    service.add_method('calc', 'multi2', _multi_2_handler)
    service.add_method('calc', 'add', _add_handler)
    service.add_method('calc', 'exp', _exp_handler)
    service.add_method('calc', 'divide', _divide_handler)
    service.start()
    client = DubboClient('127.0.0.1', 12358)
    assert client.send_request_and_return_response(service_name='calc', method_name='exp', service_version='1.0', args=[4], attachment={}).data == 16
    assert client.send_request_and_return_response(service_name='calc', method_name='multi2', service_version='1.0', args=[4], attachment={}).data == 8
    assert client.send_request_and_return_response(service_name='calc', method_name='divide', args=[3, 2]).data == 1.5
    assert client.send_request_and_return_response(service_name='calc', method_name='$invoke', args=['add', ['int', 'int'], [3, 2]], attachment={'generic': 'true'}).data == 5
    error_resp = client.send_request_and_return_response(service_name='calc', method_name='divide', args=[3, 0])
    assert error_resp.status == 40
    assert error_resp.data is None
    assert error_resp.error == 'divide by zero'