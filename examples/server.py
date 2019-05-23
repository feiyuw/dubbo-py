import time
from dubbo.server import DubboService


def _multi_2_handler(num):
    return num * 2


def _exp_handler(num):
    return num ** 2


if __name__ == '__main__':
    service = DubboService(12358, 'demo')
    service.add_method('calc', 'multi2', _multi_2_handler)
    service.add_method('calc', 'exp', _exp_handler)
    service.register('127.0.0.1:2181')  # register to zookeeper
    service.start()  # service run in a daemon thread
    time.sleep(300)  # after 300 seconds, the service will be stopped
