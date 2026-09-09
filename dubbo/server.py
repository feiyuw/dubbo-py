''' Basic module for Dubbo protocol '''
import time
import logging
import inspect
import itertools
import socket
from threading import Thread
from urllib.parse import quote_plus
from kazoo.client import KazooClient
from socketserver import ThreadingTCPServer, BaseRequestHandler
from .utils import get_pub_ip, get_timestamp
from .codec.hessian2 import Decoder, DubboHeartBeatRequest, DubboHeartBeatResponse, DubboResponse
from .errors import DubboError


__all__ = ('DubboService', )


_pid_gen = itertools.count(1)  # process id generator


class DubboService(object):
    ''' Dubbo service class, provide dubbo service:
        1. register
        2. handler
    '''
    def __init__(self, port, app, dubbo_version='2.5.3'):
        self._host = get_pub_ip()
        self._port = port
        self._app = app
        self._dubbo_version = dubbo_version
        self._services = {}  # {'service-1': {method1: handler-1, method2: handler-2}}
        self._zk_client = None  # A6: 懒创建、多次 register 复用，stop 时释放
        self._server = _ServerThread(_DubboServer(('0.0.0.0', self._port), _get_dubbo_request_handler(self._services)))

    def register(self, zk, version='1.0.0', revision='1.0.0', group=None):
        if not self._services:
            # 无服务可注册时不建立 zk 连接
            return
        # A6: 复用单例 KazooClient，避免每次 register 新建连接导致会话/连接泄漏
        client = self._get_zk_client(zk)
        grp_field = group and f'group={group}' or ''
        for service, methods in self._services.items():
            logging.info(f'register service "{service}", methods "{methods}" to zookeeper "{zk}"')
            url = f'dubbo://{self._host}:{self._port}/{service}?anyhost=true&application={self._app}&dubbo={self._dubbo_version}{grp_field}&interface={service}&methods={",".join(methods)}&pid={next(_pid_gen)}&revision={revision}&side=provider&timestamp={get_timestamp()}&version={version}'
            provider_path = f'/dubbo/{service}/providers'
            client.ensure_path(provider_path)
            # A6: provider 节点必须为 ephemeral，服务下线由 zk 自动清理
            client.create(f'{provider_path}/{quote_plus(url)}', ephemeral=True)

    def _get_zk_client(self, zk):
        if self._zk_client is None:
            self._zk_client = KazooClient(zk)
            self._zk_client.start()
        return self._zk_client

    def start(self):
        self._server.start()

    def stop(self):
        # A6: 释放 zk 连接，ephemeral 节点随之被移除
        if self._zk_client is not None:
            self._zk_client.close()
            self._zk_client = None
        self._server.stop()

    def add_method(self, service, method, handler):
        service_map = self._services.setdefault(service, {})
        service_map[method] = handler


class _ServerThread(Thread):
    def __init__(self, server_instance):
        super().__init__()
        self.daemon = True
        self.server = server_instance

    def run(self):
        self.server.serve_forever()

    def stop(self):
        # 未 start 过的服务调用 shutdown()/join() 会永久阻塞或抛错，先判断线程是否存活
        if self.is_alive():
            self.server.shutdown()
            self.join(timeout=5)
        self.server.server_close()


class _DubboServer(ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, server_address, request_handler):
        super().__init__(server_address, request_handler, bind_and_activate=True)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)


def _validate_args(handler, args):
    ''' A8: 按 handler 签名校验参数个数/类型，返回 (ok, err_msg)。
    仅校验可内省且为具体类型注解的参数；*args/内省失败则跳过。 '''
    try:
        sig = inspect.signature(handler)
    except (TypeError, ValueError):
        return True, None  # 内省失败（builtin 等），跳过校验
    params = [p for p in sig.parameters.values()
              if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)]
    if any(p.kind == inspect.Parameter.VAR_POSITIONAL for p in sig.parameters.values()):
        return True, None  # 变长参数 handler：任意个数
    required = sum(1 for p in params if p.default is inspect.Parameter.empty)
    total = len(params)
    if not (required <= len(args) <= total):
        expected = str(required) if required == total else f'{required}~{total}'
        return False, f'参数个数不符：期望 {expected} 个，实际 {len(args)} 个'
    for i, (p, arg) in enumerate(zip(params, args)):
        ann = p.annotation
        if ann is inspect.Parameter.empty or not isinstance(ann, type):
            continue  # 无注解或非具体类型（typing 泛型等）跳过类型校验
        if not isinstance(arg, ann):
            return False, f'第 {i + 1} 个参数类型不符：期望 {ann.__name__}，实际 {type(arg).__name__}'
    return True, None


def _get_dubbo_request_handler(handler_map):
    class _DubboRequestHandler(BaseRequestHandler):
        def __init__(self, request, client_address, server):
            self._request_id = itertools.count(1)
            Thread(target=self._heartbeat_loop, daemon=True).start()
            BaseRequestHandler.__init__(self, request, client_address, server)

        def handle(self):
            while True:
                try:
                    msg = Decoder(self.request).decode()
                    logging.debug(f'got message {msg}')

                    if isinstance(msg, DubboHeartBeatRequest):  # heartbeat request
                        self.request.sendall(DubboHeartBeatResponse(msg.id).encode())
                        continue
                    elif isinstance(msg, DubboHeartBeatResponse):  # heartbeat response
                        logging.debug('skip heartbeat response message')
                        continue

                    handler = handler_map.get(msg.service_name.decode(), {}).get(msg.method_name.decode())
                    if isinstance(handler, str):  # base string
                        if hasattr(self, '_' + handler):
                            handler = getattr(self, '_' + handler)
                        else:
                            handler = None
                    if not handler:
                        logging.warning(f'no handler for {msg.service_name}.{msg.method_name}')
                        continue
                    ok, err_msg = _validate_args(handler, msg.args)
                    if not ok:
                        resp = DubboResponse(msg.id, DubboResponse.BAD_REQUEST, None, err_msg)
                    else:
                        try:
                            resp = DubboResponse(msg.id, DubboResponse.OK, handler(*msg.args), None)
                        except DubboError as err:
                            resp = DubboResponse(msg.id, err.status, None, err.message)
                        except EOFError:
                            raise
                        except Exception as err:
                            resp = DubboResponse(msg.id, DubboResponse.UnknownError, None, str(err))
                    self.request.sendall(resp.encode())
                except EOFError:
                    try:
                        self.request.shutdown(socket.SHUT_RDWR)
                    except socket.error as err:
                        logging.debug('error on request shutdown: "%s"' % err)
                    self.request.close()
                    break

        def _heartbeat_loop(self):
            while True:
                time.sleep(60)
                try:
                    logging.debug('send heartbeat msg to consumer')
                    self.request.sendall(DubboHeartBeatRequest(next(self._request_id), twoway=True).encode())
                except EOFError:
                    logging.warning('got EOF error, stop heartbeat loop!')
                    return

        # builtin handlers
        def _void(self, *args):
            # do nothing
            return

        def _empty_ok(self, *args):
            # response {}
            return {}

    return _DubboRequestHandler
