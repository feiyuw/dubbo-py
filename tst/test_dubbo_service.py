from kazoo.exceptions import NodeExistsError
from dubbo import server
from dubbo.client import DubboClient
from dubbo.server import DubboService
from dubbo.errors import DubboError


_paths = []


def setup_module(module):
    server.KazooClient = _MockKazooClient
    server.get_timestamp = _mock_get_timestamp
    server.get_pub_ip = _mock_get_pub_ip


class _MockKazooClient(object):
    def __init__(self, zk):
        self._zk = zk

    def start(self):
        pass

    def create(self, path):
        if path in _paths:
            raise NodeExistsError
        _paths.append(path)


def _mock_get_timestamp():
    return 1234567890


def _mock_get_pub_ip():
    return '10.0.1.120'


def test_dubbo_register():
    service = DubboService(12345, 'unit-test')
    service.register('zk-1.test.corp:2181')
    assert _paths == []
    service.add_method('a.service', 'doGet', 'void')
    service.register('zk-1.test.corp:2181')
    assert _paths == ['/dubbo', '/dubbo/a.service', '/dubbo/a.service/providers', '/dubbo/a.service/providers/dubbo%3A%2F%2F10.0.1.120%3A12345%2Fa.service%3Fanyhost%3Dtrue%26application%3Dunit-test%26dubbo%3D2.5.3%26interface%3Da.service%26methods%3DdoGet%26pid%3D1%26revision%3D1.0.0%26side%3Dprovider%26timestamp%3D1234567890%26version%3D1.0.0']

    _paths.clear()
    service.register('zk-1.test.corp:2181', '1.1')
    assert _paths == ['/dubbo', '/dubbo/a.service', '/dubbo/a.service/providers', '/dubbo/a.service/providers/dubbo%3A%2F%2F10.0.1.120%3A12345%2Fa.service%3Fanyhost%3Dtrue%26application%3Dunit-test%26dubbo%3D2.5.3%26interface%3Da.service%26methods%3DdoGet%26pid%3D2%26revision%3D1.0.0%26side%3Dprovider%26timestamp%3D1234567890%26version%3D1.1']


def test_dubbo_handler():
    service = DubboService(12358, 'unittest')

    def _multi_2_handler(num):
        return num * 2

    def _exp_handler(num):
        return num ** 2

    def _divide_handler(a, b):
        if b == 0:
            raise DubboError(40, 'divide by zero')
        return a / b

    service.add_method('calc', 'multi2', _multi_2_handler)
    service.add_method('calc', 'exp', _exp_handler)
    service.add_method('calc', 'divide', _divide_handler)
    service.start()
    client = DubboClient('127.0.0.1', 12358)
    assert client.send_request_and_return_response(service_name='calc', method_name='exp', service_version='1.0', args=[4], attachment={}).data == 16
    assert client.send_request_and_return_response(service_name='calc', method_name='multi2', service_version='1.0', args=[4], attachment={}).data == 8
    assert client.send_request_and_return_response(service_name='calc', method_name='divide', args=[3, 2]).data == 1.5
    error_resp = client.send_request_and_return_response(service_name='calc', method_name='divide', args=[3, 0])
    assert error_resp.status == 40
    assert error_resp.data is None
    assert error_resp.error == 'divide by zero'
