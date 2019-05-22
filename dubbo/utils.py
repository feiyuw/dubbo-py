import time
import socket
import struct
import hashlib
from datetime import datetime
from functools import reduce


def bytes_to_long(bs):
    ''' convert 8 bytes bs to unsigned long (bigendian) '''
    return bytes_to_int(bs)


def long_to_bytes(num):
    ''' convert long to 8 bytes (bigendian) '''
    return struct.pack('>Q', num)


def byte(num):
    ''' convert num to one byte int'''
    return int_to_bytes(int(bin(num)[-8:], 2))


def bytes_to_int(bs, signed=False):
    ''' convert 4 bytes bs to unsigned/signed long integer (bigendian) '''
    if signed:
        return int.from_bytes(bs, 'big', signed=True)
    return reduce(lambda x, y: x * 256 + y, bs)


def int_to_bytes(num, length=None, signed=False):
    ''' convert integer to bytes (bigendian) '''
    if length == 4:
        return struct.pack(signed and '>i' or '>I', num)

    int_bytes = None
    if num == 0:
        int_bytes = num.to_bytes(1, 'big')
    else:
        int_bytes = num.to_bytes((num.bit_length() + 7) // 8, 'big', signed=signed)

    if length:
        return b'\x00' * (length - len(int_bytes)) + int_bytes

    return int_bytes


def double_to_bytes(num):
    ''' convert double to 8 bytes (bigendian) '''
    return struct.pack('>d', num)


def bytes_to_double(bs):
    ''' convert 8 bytes bs to double (bigendian) '''
    return struct.unpack('>d', bs)[0]


def timestamp_to_datetime(ts):
    if ts < 10e11:
        return datetime.fromtimestamp(ts)
    return datetime.fromtimestamp(ts / 1000)


def get_pub_ip():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.connect(('8.8.8.8', 53))
    return sock.getsockname()[0]


def get_timestamp():
    return int(time.time() * 1000)


def iter_directory(*directories):
    size = len(directories)
    for idx in range(1, size + 1):
        yield '/' + '/'.join(directories[:idx])


def md5(data: str) -> str:
    return hashlib.md5(data.encode()).hexdigest()
