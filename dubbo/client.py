import time
import socket
import logging
import itertools
import threading
from queue import Queue, Empty
from threading import Thread
from .codec.hessian2 import Decoder, DubboRequest, DubboHeartBeatRequest, DubboHeartBeatResponse


__all__ = ('DubboClient', )


class DubboClient(object):
    def __init__(self, host, port, dubbo_version='2.5.3', timeout=5):
        self._host = host
        self._port = port
        self._dubbo_version = dubbo_version
        self._timeout = timeout  # A3: 可配置的 recv 超时
        self._request_id = itertools.count(1)
        self._sock = None  # A2: 惰性连接
        self._connect_lock = threading.Lock()
        self._msg_queue = Queue()
        # A1: 按 invoke_id 分发响应，避免并发/乱序/未知响应串包
        self._pending = {}  # invoke_id -> Queue(maxsize=1)

    @classmethod
    def from_zk(cls, zk_hosts, service_name, group=None, version='1.0.0', **kwargs):
        ''' #2: 从 zookeeper 服务发现 provider（按 group/version 路由），再复用直连接口。
        kwargs 透传 DubboClient 构造参数（如 timeout）。 '''
        from .registry import discover
        host, port = discover(zk_hosts, service_name, group=group, version=version)
        return cls(host, port, **kwargs)

    def _ensure_connected(self):
        ''' A2: 惰性连接——首次使用时才建立连接并启动收发/心跳线程 '''
        with self._connect_lock:
            if self._sock is not None:
                return
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._sock.connect((self._host, self._port))
            Thread(target=self._recv_loop, daemon=True).start()
            Thread(target=self._heartbeat_loop, daemon=True).start()

    def _recv_loop(self):
        while True:
            try:
                msg = Decoder(self._sock).decode()
            except EOFError:
                logging.warning('got EOF error, stop recv loop!')
                return
            if isinstance(msg, DubboHeartBeatRequest):
                if msg.is_twoway():
                    logging.debug('reply heartbeat message')
                    self.send_heartbeat_response(msg.id)
                else:
                    logging.warning('skip heartbeat request message not twoway.')
                continue
            elif isinstance(msg, DubboHeartBeatResponse):
                logging.warning('skip heartbeat response message')
                continue
            self._dispatch(msg)

    def _dispatch(self, msg):
        ''' 有 pending 等待者则按 id 投递，否则退回公共队列（如 telnet 命令响应） '''
        q = self._pending.get(getattr(msg, 'id', None))
        if q is not None:
            q.put(msg)
        else:
            self._msg_queue.put(msg)

    def _heartbeat_loop(self):
        while True:
            time.sleep(60)
            try:
                logging.debug('send heartbeat msg to provider')
                self.send_heartbeat_request(next(self._request_id))
            except EOFError:
                logging.warning('got EOF error, stop heartbeat loop!')
                return

    def get_services(self):
        command = 'ls'
        return self._execute_command(command)

    def get_methods(self, service_name):
        command = ''.join(['ls ', service_name])
        return self._execute_command(command)

    def _execute_command(self, command):
        self._ensure_connected()
        command += '\n'
        self._sock.sendall(command.encode())
        return self._msg_queue.get().decode().split('\r\n')[:-1]

    def send_heartbeat_request(self, id_):
        self._ensure_connected()
        self._sock.sendall(DubboHeartBeatRequest(id_).encode())

    def send_heartbeat_response(self, id_):
        self._sock.sendall(DubboHeartBeatResponse(id_).encode())

    def send_request_without_response(self, **kwargs):
        self._ensure_connected()
        self._sock.sendall(DubboRequest(
            id=next(self._request_id),
            twoway=False,
            dubbo_version=self._dubbo_version,
            **kwargs).encode())

    def send_request_and_return_response(self, **kwargs):
        self._ensure_connected()
        req_id = next(self._request_id)
        q = Queue(maxsize=1)
        # 先登记 pending 再发送，避免响应比登记先到
        self._pending[req_id] = q
        try:
            self._sock.sendall(DubboRequest(
                id=req_id,
                twoway=True,
                dubbo_version=self._dubbo_version,
                **kwargs).encode())
            return q.get(True, self._timeout)
        finally:
            self._pending.pop(req_id, None)
