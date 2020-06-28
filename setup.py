from setuptools import setup


setup(
    name='dubbo-py',
    version='0.2.2',
    description='dubbo adaptor for python',
    long_description='''`dubbo-py` 是一个用Python3进行dubbo协议编解码和service、client模拟的Library，开发它的目的是用于对现有dubbo服务进行功能自动化测试，包括模拟dubbo的provider和consumer。

[![Build Status](https://travis-ci.org/feiyuw/dubbo-py.svg?branch=master)](https://travis-ci.org/feiyuw/dubbo-py)

## Install

```sh
pip3 install dubbo-py
```

## Example

```python
# 作为Server
from dubbo.codec.hessian2 import DubboResponse
from dubbo.server import DubboService


def remote_max(nums):
    return max(nums)


def remote_divide(a, b):
    return a / b


service = DubboService(12358, 'demo')
service.add_method('com.myservice.math', 'max', remote_max)
service.add_method('com.myservice.math', 'divide', remote_divide)
# service.register('127.0.0.1:2181', '1.0.0')  # register to zookeeper
service.start()  # service run in a daemon thread


# 作为Client
from dubbo.client import DubboClient


client = DubboClient('127.0.0.1', 12358)
resp = client.send_request_and_return_response(service_name='com.myservice.math', method_name='max', args=[[1, 2, 3, 4]])
print(resp.ok)   # True
print(resp.data)  # 4
print(resp.error)  # None

resp = client.send_request_and_return_response(service_name='com.myservice.math', method_name='divide', args=[1, 0])
print(resp.ok)   # False
print(resp.data)  # None
print(resp.error)  # division by zero
```

也可以构造Java Object来请求，如：

```python
# client
from dubbo.codec.hessian2 import new_object
from dubbo.client import DubboClient

client = DubboClient('127.0.0.1', 12358)
# 构造一个Java Object为com.demo.test的参数
arg = new_object(
    'com.demo.test',
    uuid='1b7530ba-2afa-4e7f-9876-c6744831c3fd',
    id=10,
    key='helloEvt',
    param={'name': 'hello', 'value': 'world'},
    doit=True)
resp = client.send_request_and_return_response(
    service_name='com.myservice.complex',
    method_name='aggr',
    args=[arg])
```''',
    long_description_content_type='text/markdown',
    author='Zhang Yu',
    author_email='feiyuw@gmail.com',
    url='https://github.com/feiyuw/dubbo-py.git',
    python_requires='>=3.5',
    install_requires=['kazoo'],
    packages=[
        'dubbo',
        'dubbo.codec'],
    platforms='any',
)
