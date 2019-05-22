import time
import socket
import logging
import itertools
from queue import Queue
from threading import Thread
from .codec.hessian2 import Decoder, DubboRequest, DubboHeartBeatRequest, DubboHeartBeatResponse


__all__ = ('DubboClient', )


class DubboClient(object):
    _timeout = 5  # recv timeout set to 5sec

    def __init__(self, host, port, dubbo_version='2.5.3'):
        self._host = host
        self._port = port
        self._dubbo_version = dubbo_version
        self._request_id = itertools.count(1)
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.connect((self._host, self._port))
        self._msg_queue = Queue()
        Thread(target=self._recv_loop, daemon=True).start()
        Thread(target=self._heartbeat_loop, daemon=True).start()

    def _recv_loop(self):
        while True:
            try:
                msg = Decoder(self._sock).decode()
            except EOFError:
                logging.warn('got EOF error, stop recv loop!')
                return
            if isinstance(msg, DubboHeartBeatRequest):
                if msg.is_twoway():
                    logging.debug('reply heartbeat message')
                    self.send_heartbeat_response(msg.id)
                else:
                    logging.warn('skip heartbeat request message not twoway.')
                continue
            elif isinstance(msg, DubboHeartBeatResponse):
                logging.warn('skip heartbeat response message')
                continue
            self._msg_queue.put(msg)

    def _heartbeat_loop(self):
        while True:
            time.sleep(60)
            try:
                logging.debug('send heartbeat msg to provider')
                self.send_heartbeat_request(next(self._request_id))
            except EOFError:
                logging.warn('got EOF error, stop heartbeat loop!')
                return

    def get_services(self):
        command = 'ls'
        return self._execute_command(command)

    def get_methods(self, service_name):
        command = ''.join(['ls ', service_name])
        return self._execute_command(command)

    def _execute_command(self, command):
        command += '\n'
        self._sock.sendall(command.encode())
        return self._msg_queue.get().decode().split('\r\n')[:-1]

    def send_heartbeat_request(self, id_):
        self._sock.sendall(DubboHeartBeatRequest(id_).encode())

    def send_heartbeat_response(self, id_):
        self._sock.sendall(DubboHeartBeatResponse(id_).encode())

    def send_request_without_response(self, **kwargs):
        self._sock.sendall(DubboRequest(
            id=next(self._request_id),
            twoway=False,
            dubbo_version=self._dubbo_version,
            **kwargs).encode())

    def send_request_and_return_response(self, **kwargs):
        self._sock.sendall(DubboRequest(
            id=next(self._request_id),
            twoway=True,
            dubbo_version=self._dubbo_version,
            **kwargs).encode())
        return self._msg_queue.get(True, self._timeout)
