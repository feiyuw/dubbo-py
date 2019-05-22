import time
from dubbo.server import DubboService
from dubbo.codec.hessian2 import DubboResponse


def _multi_2_handler(request, sock):
    return sock.sendall(DubboResponse(request.id, DubboResponse.OK, request.args[0] * 2, None).encode())


def _exp_handler(request, sock):
    return sock.sendall(DubboResponse(request.id, DubboResponse.OK, request.args[0] ** 2, None).encode())


if __name__ == '__main__':
    service = DubboService(12358, 'demo')
    service.add_method('calc', 'multi2', _multi_2_handler)
    service.add_method('calc', 'exp', _exp_handler)
    service.register('127.0.0.1:2181')  # register to zookeeper
    service.start()  # service run in a daemon thread
    time.sleep(300)  # after 300 seconds, the service will be stopped
