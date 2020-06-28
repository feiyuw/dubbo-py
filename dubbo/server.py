''' Basic module for Dubbo protocol '''
import time
import logging
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


_DUBBO_CTRL = b'\x01N'  # control charactors like b'\xda\xbb\xe2\x01\xb0\x01Nnull\r\nelapsed: 0 ms.\r\ndubbo>'
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
        self._server = _ServerThread(_DubboServer(('0.0.0.0', self._port), _get_dubbo_request_handler(self._services)))

    def register(self, zk, version='1.0.0', revision='1.0.0', group=None):
        client = KazooClient(zk)
        client.start()
        grp_field = group and f'group={group}' or ''
        for service, methods in self._services.items():
            logging.info(f'register service "{service}", methods "{methods}" to zookeeper "{zk}"')
            url = f'dubbo://{self._host}:{self._port}/{service}?anyhost=true&application={self._app}&dubbo={self._dubbo_version}{grp_field}&interface={service}&methods={",".join(methods)}&pid={next(_pid_gen)}&revision={revision}&side=provider&timestamp={get_timestamp()}&version={version}'
            client.ensure_path(f'/dubbo/{service}/providers/{quote_plus(url)}')

    def start(self):
        self._server.start()

    def stop(self):
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
        self.server.shutdown()
        self.server.server_close()
        self.join()


class _DubboServer(ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, server_address, request_handler):
        super().__init__(server_address, request_handler, bind_and_activate=True)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)


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
