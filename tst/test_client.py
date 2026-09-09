''' DubboClient 行为测试（A1：响应按 request_id 关联；A2/A3：惰性连接与可配超时）'''
import socket
import threading
import time
from queue import Empty

import pytest

from dubbo.client import DubboClient
from dubbo.codec.hessian2 import Decoder, DubboResponse


class _FakeServer(threading.Thread):
    ''' 可控 TCP 服务：先发一个无请求对应的“陈旧”响应，再收真实请求并按 id 回复 '''

    def __init__(self):
        super().__init__(daemon=True)
        self._srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._srv.bind(('127.0.0.1', 0))
        self._srv.listen(1)
        self.port = self._srv.getsockname()[1]

    def run(self):
        conn, _ = self._srv.accept()
        try:
            # 先发一个没有任何请求对应的陈旧响应（旧实现在 FIFO 队列下会串包）
            conn.sendall(DubboResponse(999, DubboResponse.OK, 'stale', None).encode())
            # 再接收真实请求，按请求 id 回复
            msg = Decoder(conn).decode()
            conn.sendall(DubboResponse(msg.id, DubboResponse.OK, 'good', None).encode())
        finally:
            conn.close()
            self._srv.close()


def test_client_ignores_unmatched_response_by_request_id():
    srv = _FakeServer()
    srv.start()
    client = DubboClient('127.0.0.1', srv.port)
    resp = client.send_request_and_return_response(
        service_name='svc', method_name='m', args=[1])
    # 必须拿到与本次请求 id 匹配的响应，而不是队列里先到的陈旧响应
    assert resp.error is None
    assert resp.data == 'good'


class _SilentServer(threading.Thread):
    ''' 收到请求但不回复，用于验证超时 '''

    def __init__(self):
        super().__init__(daemon=True)
        self._srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._srv.bind(('127.0.0.1', 0))
        self._srv.listen(1)
        self.port = self._srv.getsockname()[1]

    def run(self):
        conn, _ = self._srv.accept()
        try:
            Decoder(conn).decode()  # 读完请求，但不回复，保持连接
            time.sleep(10)
        except Exception:
            pass
        finally:
            conn.close()
            self._srv.close()


def test_client_timeout_is_configurable():
    # A3: 超时应可配置，而非类级固定 5s
    srv = _SilentServer()
    srv.start()
    client = DubboClient('127.0.0.1', srv.port, timeout=0.3)
    t0 = time.time()
    with pytest.raises(Empty):
        client.send_request_and_return_response(service_name='s', method_name='m', args=[1])
    elapsed = time.time() - t0
    assert 0.25 <= elapsed < 3  # 明显小于默认 5s，证明参数生效


def test_client_lazy_connect():
    # A2: 构造时 provider 未就绪不应立即抛错，首次发送时才连接
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('127.0.0.1', 0))  # bind 但不 listen，connect 会被拒绝
    port = s.getsockname()[1]
    try:
        client = DubboClient('127.0.0.1', port)  # 构造不应抛 ConnectionRefusedError
    finally:
        s.close()
    with pytest.raises(OSError):
        client.send_request_and_return_response(service_name='s', method_name='m', args=[])