`dubbo-py` 是一个用Python3进行dubbo协议编解码和service、client模拟的Library，开发它的目的是用于对现有dubbo服务进行功能自动化测试，包括模拟dubbo的provider和consumer。

## Example

### 作为Server

```python
from dubbo.codec.hessian2 import DubboResponse, JavaList
from dubbo.server import DubboService


def remote_max(request, sock):
    return sock.sendall(DubboResponse(request.id, DubboResponse.OK, max(request.args[0]), None).encode())


service = DubboService(12358, 'demo')
service.add_method('com.myservice.math', 'max', remote_max)
service.register('127.0.0.1:2181', '1.0.0')  # register to zookeeper
service.start()  # service run in a daemon thread
```

### 作为Client

```python
from dubbo.client import DubboClient


client = DubboClient('127.0.0.1', 12358)
resp = client.send_request_and_return_response(service_name='com.myservice.math', method_name='max', service_version='1.0.0', args=[[1, 2, 3, 4]], attachment={})
print(resp.data)  # 4
```
