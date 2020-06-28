import re
import struct
import logging
import binascii
from io import BytesIO
from collections import namedtuple
from ..utils import int_to_bytes, bytes_to_int, bytes_to_long, double_to_bytes, \
    bytes_to_double, timestamp_to_datetime, long_to_bytes
from ..java_class import JavaList, java_typed_data_to_python


_DUBBO_MAGIC = b'\xda\xbb'
_DUBBO_END = b'\r\ndubbo>'

_BC_INT_BYTE_ZERO = 0xc8
_BC_INT_SHORT_ZERO = 0xd4
_BC_LONG_BYTE_ZERO = 0xf8
_BC_LONG_SHORT_ZERO = 0x3c
_BC_LONG_ZERO = 0xe0
_BC_DOUBLE_ZERO = 0x5b
_BC_DOUBLE_ONE = 0x5c
_BC_DOUBLE_BYTE = 0x5d
_BC_DOUBLE_SHORT = 0x5e
_BC_DOUBLE_MILL = 0x5f
_BC_LONG_INT = 0x59
_BC_REF = 0x51
_BC_INT_ZERO = 0x90

_BS_STRING = ord(b'S')
_BS_STRING_TRUNK = ord(b'R')

_BYTE_NONE = ord(b'N')
_BYTE_TRUE = ord(b'T')
_BYTE_FALSE = ord(b'F')
_BYTE_L = ord(b'L')
_BYTE_D = ord(b'D')
_BYTE_DATE = 0x4a
_BYTE_DATE_MINUTE = 0x4b

_DIRECT_INTEGER = range(0x80, 0xbf + 1)
_DIRECT_LONG = range(0xd8, 0xef + 1)
_BYTE_INT = range(0xc0, 0xcf + 1)
_BYTE_LONG = range(0xf0, 0xff + 1)
_SHORT_INT = range(0xd0, 0xd7 + 1)
_SHORT_LONG = range(0x38, 0x3f + 1)
_ZERO_BYTE = range(0x00, 0x1f + 1)


_FLAG_REQUEST = 0x80
_FLAG_RESPONSE = 0x00
_FLAG_TWOWAY = 0x40
_FLAG_EVENT = 0x20
_SERIALIZATION_MASK = 0x1f
_HESSIAN2_SERIALIZATION_ID = 0x02


class Decoder(object):
    def __init__(self, stream):
        self._stream = stream
        self._twoway = False
        self._refs = []

    def decode(self):
        header = self._read(2)
        if header[:2] != _DUBBO_MAGIC:
            header += self._read_until_prompt()
            return header
        else:
            header += self._read(14)
        flag = header[2]
        proto = flag & _SERIALIZATION_MASK
        logging.debug('decode with version "%d"' % proto)
        if flag & _FLAG_TWOWAY:
            self._twoway = True
        status = header[3]
        invoke_id = bytes_to_long(header[4:12])
        body_length = bytes_to_int(header[12:16])
        self._stream = BytesIO(self._read(body_length))  # read all body into stream, to avoid over read issue
        try:
            if flag & _FLAG_REQUEST:
                if flag & _FLAG_EVENT:
                    return self._decode_heartbeat_request(invoke_id)
                # decode request
                return self._decode_request_body(invoke_id)
            else:
                if flag & _FLAG_EVENT:
                    return self._decode_heartbeat_response(invoke_id)
                # decode response
                return self._decode_response_body(invoke_id, status)
        except Exception:
            self._stream.seek(0)
            logging.warn('Unable to decode message "%s"' % self._stream.read())
            raise
        finally:
            left_bytes = self._stream.read()
            if left_bytes:
                logging.warn('bytes "%s" undecoded!' % binascii.hexlify(left_bytes))
            self._stream.close()

    def _decode_heartbeat_request(self, id_):
        data = self._read_object()
        return DubboHeartBeatRequest(id_, data, self._twoway)

    def _decode_heartbeat_response(self, id_):
        data = self._read_object()
        return DubboHeartBeatResponse(id_, data)

    def _decode_request_body(self, id_):
        dubbo_version = self._read_bytes()
        service_name = self._read_bytes()
        service_version = self._read_bytes()
        method_name = self._read_bytes()
        desc = self._read_bytes()
        arg_types = _desc_to_cls_names(desc.decode())
        args = []
        for _ in arg_types:
            args.append(self._read_object())
        # parse attachment
        attachment = self._read_object()
        # handle generic type request
        if attachment.get('generic') in ('true', True):
            method_name = args[0].encode()
            args = [java_typed_data_to_python(type_, data) for (type_, data) in zip(*args[1:])]
        return DubboRequest(id=id_, twoway=self._twoway, dubbo_version=dubbo_version, service_name=service_name, service_version=service_version, method_name=method_name, args=args, attachment=attachment)

    def _decode_response_body(self, id_, status):
        data, error = None, None
        if status == DubboResponse.OK:
            status_code = self._read_int()  # TODO: see DecodeableRpcResult.java decode
            if status_code == 1:
                data = self._read_object()
            elif status_code == 0:  # XXX: it should be error, need confirm
                data = self._read_object()
        else:
            error = self._read_object()
        return DubboResponse(id_, status, data, error)

    def _read_bytes(self):
        tag = ord(self._read(1))
        if tag == _BYTE_NONE:
            return None
        elif tag == _BYTE_TRUE:
            return b'true'
        elif tag == _BYTE_FALSE:
            return b'false'
        elif tag in _DIRECT_INTEGER:
            return int_to_bytes(tag - 0x90)
        elif tag in _BYTE_INT:
            return int_to_bytes((tag - _BC_INT_BYTE_ZERO) << 8) + self._read(1)
        elif tag in _SHORT_INT:
            return int_to_bytes(tag - _BC_INT_SHORT_ZERO) + self._read(2)
        elif tag in (b'I', _BC_LONG_INT):
            return self._read(4)
        elif tag in _DIRECT_LONG:
            return int_to_bytes(tag - _BC_LONG_ZERO)
        elif tag in _BYTE_LONG:
            return int_to_bytes(tag - _BC_LONG_BYTE_ZERO) + self._read(1)
        elif tag in _SHORT_LONG:
            return int_to_bytes(tag - _BC_LONG_SHORT_ZERO) + self._read(2)
        elif tag == _BYTE_L:
            return self._read(8)
        elif tag == _BC_DOUBLE_ZERO:
            return b'0.0'
        elif tag == _BC_DOUBLE_ONE:
            return b'1.0'
        elif tag == _BC_DOUBLE_BYTE:
            return self._read(1)
        elif tag == _BC_DOUBLE_SHORT:
            return self._read(2)
        elif tag == _BC_DOUBLE_MILL:
            return double_to_bytes(0.001 * bytes_to_int(self._read(4)))
        elif tag == _BYTE_D:
            return self._read(8)
        elif tag in (_BS_STRING, _BS_STRING_TRUNK):
            _sbuf = b''
            _chunk_len = bytes_to_int(self._read(2))
            for _ in range(_chunk_len):
                ch = self._read_char()
                if ch >= b'\x00':
                    _sbuf += ch
            return _sbuf
        elif tag in _ZERO_BYTE:
            _sbuf = b''
            _chunk_len = tag - 0x00
            for _ in range(_chunk_len):
                ch = self._read_char()
                if ch >= b'\x00':
                    _sbuf += ch
            return _sbuf
        elif tag in (0x30, 0x31, 0x32, 0x33):
            _sbuf = b''
            _chunk_len = (tag - 0x30) * 256 + ord(self._read(1))
            for _ in range(_chunk_len):
                ch = self._read_char()
                if ch >= b'\x00':
                    _sbuf += ch
            return _sbuf
        raise RuntimeError('read bytes "%d" error' % tag)

    def _read_object(self, tag=None):
        if tag is None:
            tag = ord(self._read(1))
        if tag == _BYTE_NONE:
            return None
        elif tag == _BYTE_TRUE:
            return True
        elif tag == _BYTE_FALSE:
            return False
        elif tag in _DIRECT_INTEGER:
            return tag - 0x90
        elif tag in _BYTE_INT:
            return ((tag - _BC_INT_BYTE_ZERO) << 8) + ord(self._read(1))
        elif tag in _SHORT_INT:
            return ((tag - _BC_INT_SHORT_ZERO) << 16) + bytes_to_int(self._read(2))
        elif tag in (b'I', _BC_LONG_INT):
            return bytes_to_int(self._read(4))
        elif tag in _DIRECT_LONG:
            return tag - _BC_LONG_ZERO
        elif tag in _BYTE_LONG:
            return (tag - _BC_LONG_BYTE_ZERO) * 256 + ord(self._read(1))
        elif tag in _SHORT_LONG:
            return ((tag - _BC_LONG_SHORT_ZERO) << 16) + bytes_to_int(self._read(2))
        elif tag == _BYTE_L:
            return bytes_to_long(self._read(8))
        elif tag == _BC_DOUBLE_ZERO:
            return 0.0
        elif tag == _BC_DOUBLE_ONE:
            return 1.0
        elif tag == _BC_DOUBLE_BYTE:
            return bytes_to_int(self._read(1), signed=True)
        elif tag == _BC_DOUBLE_SHORT:
            return bytes_to_int(self._read(2), signed=True)
        elif tag == _BC_DOUBLE_MILL:
            return 0.001 * bytes_to_int(self._read(4), signed=True)
        elif tag == _BYTE_D:
            return bytes_to_double(self._read(8))
        elif tag == _BYTE_DATE:
            return timestamp_to_datetime(bytes_to_long(self._read(8)))
        elif tag == _BYTE_DATE_MINUTE:
            return timestamp_to_datetime(bytes_to_int(self._read(4)) * 60)
        elif tag in (_BS_STRING, _BS_STRING_TRUNK):
            _sbuf = b''
            _chunk_len = bytes_to_int(self._read(2))
            for _ in range(_chunk_len):
                ch = self._read_char()
                if ch >= b'\x00':
                    _sbuf += ch
            return _sbuf.decode()
        elif tag in _ZERO_BYTE:
            _sbuf = b''
            _chunk_len = tag - 0x00
            for _ in range(_chunk_len):
                ch = self._read_char()
                if ch >= b'\x00':
                    _sbuf += ch
            return _sbuf.decode()
        elif tag in (0x30, 0x31, 0x32, 0x33):
            _sbuf = b''
            _chunk_len = (tag - 0x30) * 256 + ord(self._read(1))
            for _ in range(_chunk_len):
                ch = self._read_char()
                if ch >= b'\x00':
                    _sbuf += ch
            return _sbuf.decode()
        elif tag in (ord('A'), ord('B')):
            _sbuf = b''
            _chunk_len = bytes_to_int(self._read(2))
            _chunk_len, data = self._read_byte(_chunk_len)
            while data >= 0:
                _sbuf += data
                _chunk_len, data = self._read_byte(_chunk_len)
            return _sbuf
        elif tag in range(0x20, 0x2f + 1):
            return self._read(tag - 0x20)
        elif tag in (0x34, 0x35, 0x36, 0x37):
            return self._read((tag - 0x34) * 256 + ord(self._read(1)))
        elif tag == 0x55:  # variable length list typed
            raise RuntimeError('unimplemented')
        elif tag == 0x57:  # list variable untyped
            raise RuntimeError('unimplemented')
        elif tag == 0x56:  # fixed list typed
            self._read_bytes()  # list type
            length = self._read_int()
            return self._read_list(length, JavaList)
        elif tag == 0x58:  # fixed list untyped
            length = self._read_int()
            return self._read_list(length)
        elif tag in range(0x70, 0x78):  # compact fixed list
            self._read_bytes()  # list type
            length = tag - 0x70
            return self._read_list(length, JavaList)
        elif tag in range(0x78, 0x7f + 1):  # compact fixed list untyped
            length = tag - 0x78
            return self._read_list(length)
        elif tag == ord(b'H'):
            return self._read_map()
        elif tag == ord(b'M'):
            return self._read_map(tag)
        elif tag == ord(b'C'):
            self._read_object_def()
            return self._read_object()
        elif tag in range(0x60, 0x6f + 1):
            idx = tag - 0x60
            try:
                ref = self._refs[idx]
                ref['args'] = []
                for field_name in ref['fields']:
                    ref['args'].append(self._read_object())
                return _cls_factory(ref)
            except IndexError:
                raise RuntimeError('class definition not found, idx: %d' % idx)
        elif tag == ord(b'0'):
            raise RuntimeError('unimplemented')
        elif tag == _BC_REF:
            idx = self._read_int()
            return self._refs[idx]
        elif tag == 0x5a:  # b'Z'
            raise EOFError
        else:
            raise RuntimeError('unknown code "%s"' % tag)

    def _read_list(self, length, list_type=None):
        if list_type:
            return list_type([self._read_object() for _ in range(length)])
        return [self._read_object() for _ in range(length)]

    def _read_object_def(self):
        type_ = self._read_bytes()
        len_ = self._read_int()
        field_names = []
        for _ in range(len_):
            field_names.append(self._read_bytes())
        self._refs.append({'type': type_, 'fields': field_names})

    def _read_int(self):
        tag = ord(self._read(1))
        if tag == ord(b'N'):
            return 0
        elif tag == ord(b'F'):
            return 0
        elif tag == ord(b'T'):
            return 1
        elif tag in range(0x80, 0xbf + 1):
            return tag - _BC_INT_ZERO
        elif tag in range(0xc0, 0xcf + 1):
            return ((tag - _BC_INT_BYTE_ZERO) << 8) + ord(self._read(1))
        elif tag in range(0xd0, 0xd7 + 1):
            return ((tag - _BC_INT_SHORT_ZERO) << 16) + bytes_to_int(self._read(2))
        elif tag in (ord(b'I'), _BC_LONG_INT):
            return bytes_to_int(self._read(4))
        elif tag in range(0xd8, 0xef + 1):
            return tag - _BC_LONG_ZERO
        elif tag in range(0xf0, 0xff + 1):
            return ((tag - _BC_LONG_BYTE_ZERO) << 8) + ord(self._read(1))
        elif tag in range(0x38, 0x3f + 1):
            return ((tag - _BC_LONG_SHORT_ZERO) << 16) + bytes_to_int(self._read(2))
        elif tag == ord(b'L'):
            return bytes_to_long(self._read(8))
        elif tag == _BC_DOUBLE_ZERO:
            return 0
        elif tag == _BC_DOUBLE_ONE:
            return 1
        elif tag == _BC_DOUBLE_BYTE:
            return bytes_to_int(self._read(1), signed=True)
        elif tag == _BC_DOUBLE_SHORT:
            return bytes_to_int(self._read(2), signed=True)
        elif tag == _BC_DOUBLE_MILL:
            return int(0.001 * bytes_to_int(self._read(4), signed=True))
        elif tag == ord(b'D'):
            return bytes_to_long(self._read(8))
        raise RuntimeError('read int error "%d"' % tag)

    def _read_map(self, code=None):
        if code == b't':
            type_len = struct.unpack('>H', self._read(2))[0]
            if type_len > 0:
                # a typed map deserializes to an object
                type_ = self._read(type_len)
                logging.warn('typed map: %s' % type_)

            code = self._read(1)
        else:
            # untyped maps deserialize to a dict
            if code == b'M':
                # Read and discard type
                try:
                    self._read_object()
                except RuntimeError:
                    code = b'Z'
                code = self._read(1)

        result = {}

        while code not in (b'z', b'Z'):
            try:
                key, value = self._read_keyval(code)
            except EOFError:
                break

            if key == {}:
                return result

            result[key] = value

            code = self._read(1)

        return result

    def _read_keyval(self, code):
        key = self._read_object(code and ord(code))
        value = self._read_object()

        return key, value

    def _read_char(self):
        ch = self._read(1)
        int_ch = ord(ch)
        if int_ch < 0x80:
            return ch
        elif (int_ch & 0xe0) == 0xc0:
            return ch + self._read(1)
        elif ((int_ch & 0xf0) == 0xe0):
            return ch + self._read(2)

        raise RuntimeError('unknown charactor "%d"' % ch)

    def _read_byte(self, _chunk_len):
        while _chunk_len <= 0:
            code = ord(self._read(1))
            if code in (ord(b'A'), ord(b'B')):
                _chunk_len = bytes_to_int(self._read(2))
            elif code in range(0x20, 0x2f + 1):
                _chunk_len = code - 0xa0
            elif code in (0x34, 0x35, 0x36, 0x37):
                _chunk_len = (code - 0x34) * 256 + ord(self._read(1))

        _chunk_len -= 1
        return _chunk_len, self._read(1)

    def _read(self, length):
        read_func = hasattr(self._stream, 'recv') and self._stream.recv or self._stream.read
        received = b''
        while len(received) < length:
            chunk = read_func(length)
            if not chunk:
                raise EOFError
            received += chunk
        return received

    def _read_until_prompt(self):
        received = b''
        while not received.endswith(_DUBBO_END):
            chunk = self._read(1)
            if not chunk:
                raise EOFError
            received += chunk
        return received


_DESC_PTN = re.compile(r'(?:[VZBCDFIJS])|(?:L[_$a-zA-Z][_$a-zA-Z0-9/]*;)|(?:\[+(?:[VZBCDFIJS]|L[_$a-zA-Z][_$a-zA-Z0-9/]*;))')


def _desc_to_cls_names(desc):
    _handler_map = {
        'V': lambda e: 'None',
        'Z': lambda e: 'bool',
        'B': lambda e: 'bytes',
        'C': lambda e: 'chr',
        'D': lambda e: 'float',
        'F': lambda e: 'float',
        'I': lambda e: 'int',
        'J': lambda e: 'int',
        'S': lambda e: 'int',
        'L': lambda e: e[1:-1].replace('/', '.'),
        '[': lambda e: e.replace('/', '.')}  # TODO: array type handling
    cls_names = []

    for cls_desc in _DESC_PTN.findall(desc):
        _first_byte = cls_desc[0]
        if _first_byte not in _handler_map:
            raise RuntimeError('unknown type "%s"' % _first_byte)
        cls_names.append(_handler_map[_first_byte](cls_desc))

    return cls_names


def _cls_names_to_desc(cls_names):
    _handler_map = {
        'int': 'I',
        'long': 'J',
        'NoneType': 'V',
        'bool': 'Z',
        'bytes': 'B',
        'str': 'S',
        'float': 'D',
    }

    def complex_handler(type_name):
        if type_name[0] == '[':  # TODO: array handling
            return type_name.replace('.', '/')
        return 'L' + type_name.replace('.', '/') + ';'

    return ''.join(_handler_map.get(name, complex_handler(name)) for name in cls_names)


def _cls_factory(ref):
    type_name = ref['type'].decode()
    kwargs = dict(zip(map(lambda f: f.decode(), ref['fields']), ref['args']))
    return new_object(type_name, **kwargs)


_STRING_DIRECT_MAX = 0x1f
_STRING_SHORT_MAX = 0x3ff
_BC_STRING_SHORT = 0x30


def encode_object(field, idx=0, cls_names=[]):
    ''' encode an object into hessian2 stream '''
    if field is None:
        return b'N'
    elif field is True:
        return b'T'
    elif field is False:
        return b'F'
    elif isinstance(field, str):
        # TODO: field convert required? see Hessian2Output.java -> printString
        length = len(field)
        if length <= _STRING_DIRECT_MAX:
            return int_to_bytes(length) + field.encode()
        elif length <= _STRING_SHORT_MAX:
            return int_to_bytes((_BC_STRING_SHORT << 8) + length) + field.encode()
        return b'S' + int_to_bytes(length) + field.encode()
    elif isinstance(field, dict):
        result = b'H'
        for k, v in field.items():
            result += encode_object(k, cls_names=[])
            result += encode_object(v, cls_names=[])
        result += b'Z'
        return result
    elif isinstance(field, (list, set)):
        result = b''
        type_ = type(field).__name__
        if len(field) < 8:
            if type_ not in ('list', 'set'):
                result += int_to_bytes(len(field) + 0x70)
                result += encode_object(type_)
            else:
                result += int_to_bytes(len(field) + 0x78)
        else:
            if type_ not in ('list', 'set'):
                result += b'\x56'
                result += encode_object(type_)
            else:
                result += b'\x58'
            result += encode_object(len(field))
        result += b''.join(encode_object(e, cls_names=[]) for e in field)
        return result
    elif isinstance(field, long):
        if -0x08 <= field and field <= 0x0f:
            return int_to_bytes(field + _BC_LONG_ZERO)
        elif -0x800 <= field and field <= 0x7ff:
            return int_to_bytes((_BC_LONG_BYTE_ZERO << 8) + field)
        elif -0x40000 <= field and field <= 0x3ffff:
            return int_to_bytes((_BC_LONG_SHORT_ZERO << 16) + field)
        elif -0x80000000 <= field and field <= 0x7fffffff:
            return chr(_BC_LONG_INT).encode() + int_to_bytes(field, 4)
        return b'L' + long_to_bytes(field)
    elif isinstance(field, int):
        if field >= -0x10 and field <= 0x2f:
            return int_to_bytes(field + _BC_INT_ZERO)
        elif field >= -0x800 and field <= 0x7ff:
            return int_to_bytes((_BC_INT_BYTE_ZERO << 8) + field)
        elif field >= -0x40000 and field <= 0x3ffff:
            return int_to_bytes((_BC_INT_SHORT_ZERO << 16) + field)
        return b'I' + int_to_bytes(field, 4)
    elif isinstance(field, (float, double)):
        int_field = int(field)
        if int_field == field:
            if field == 0:
                return chr(_BC_DOUBLE_ZERO).encode()
            elif field == 1:
                return chr(_BC_DOUBLE_ONE).encode()
            elif field in range(-128, 128):
                return chr(_BC_DOUBLE_BYTE).encode() + int_to_bytes(int_field, signed=True)
            elif field in range(-32768, 32768):
                return chr(_BC_DOUBLE_SHORT).encode() + int_to_bytes(int_field, signed=True)

        mills = int(field * 1000)
        if mills * 0.001 == field:
            return chr(_BC_DOUBLE_MILL).encode() + int_to_bytes(mills, 4, signed=True)
        return b'D' + double_to_bytes(field)
    elif hasattr(field, '_fields'):  # namedtuple subclass instance
        cls_name = field.__class__.__name__
        if cls_name not in cls_names:  # XXX: not thread safe
            cls_names.append(cls_name)
        # object def
        result = b'C' + encode_object(cls_name)
        #   count of field names
        result += encode_object(len(field._fields))
        #   field names
        for field_name in field._fields:
            result += encode_object(field_name)
        # object fields
        result += int_to_bytes(idx + cls_names.index(cls_name) + 0x60)
        for field_name in field._fields:
            result += encode_object(getattr(field, field_name), idx, cls_names)
        return result
    else:  # custom object
        raise RuntimeError('unknown field "%s", type "%s"' % (field, type(field)))


class DubboRequest(object):
    def __init__(self, id, twoway, dubbo_version, service_name, method_name, args, service_version='1.0', attachment={}):
        self.id = id
        self.twoway = twoway
        self.dubbo_version = dubbo_version
        self.service_name = service_name
        self.service_version = service_version
        self.method_name = method_name
        self.args = args
        self.attachment = attachment

    def encode(self):
        stream = BytesIO()
        self._encode_header(stream)  # 12byte header
        self._encode_body(stream)  # 4byte body length + body
        stream.seek(0)
        try:
            return stream.read()
        finally:
            stream.close()

    def _encode_header(self, stream):
        stream.write(_DUBBO_MAGIC)
        flag = _FLAG_REQUEST | _HESSIAN2_SERIALIZATION_ID
        if self.twoway:
            flag |= _FLAG_TWOWAY
        stream.write(int_to_bytes(flag))  # flag
        stream.write(b'\x00')  # status
        stream.write(long_to_bytes(self.id))

    def _encode_body(self, stream):
        body = self._get_body()
        stream.write(int_to_bytes(len(body), 4))
        stream.write(body)

    def _get_body(self):
        return encode_object(self.dubbo_version) + \
            encode_object(self.service_name) + \
            encode_object(self.service_version) + \
            encode_object(self.method_name) + \
            self._get_desc() + \
            self._get_args() + \
            self._get_attachment()

    def _get_desc(self):
        return encode_object(_cls_names_to_desc([type(arg).__name__ for arg in self.args]))

    def _get_args(self):
        cls_names = []
        return b''.join([encode_object(arg, idx, cls_names) for idx, arg in enumerate(self.args)])

    def _get_attachment(self):
        return encode_object(self.attachment)

    def __repr__(self):
        return f'dubbo_version: {self.dubbo_version}, method: {self.service_name}.{self.method_name}:{self.service_version}, args: {self.args}, attachment: {self.attachment}'


class _HeartBeat(object):
    def __init__(self, id, data=None, twoway=False):
        self.id = id
        self.data = data
        self._twoway = twoway

    def is_twoway(self):
        return self._twoway

    def encode(self):
        stream = BytesIO()
        self._encode_header(stream)  # 12byte header
        self._encode_body(stream)  # 4byte body length + body
        stream.seek(0)
        try:
            return stream.read()
        finally:
            stream.close()

    def _encode_header(self, stream):
        stream.write(_DUBBO_MAGIC)
        stream.write(int_to_bytes(self._get_flag()))  # flag
        stream.write(b'\x00')  # status
        stream.write(long_to_bytes(self.id))

    def _get_flag(self):
        raise RuntimeError('not implemented')

    def _encode_body(self, stream):
        body = self._get_body()
        stream.write(int_to_bytes(len(body), 4))
        stream.write(body)

    def _get_body(self):
        return encode_object(self.data)

    def __repr__(self):
        return f'id: {self.id}, twoway: {self._twoway}'


class DubboHeartBeatRequest(_HeartBeat):
    def _get_flag(self):
        flag = _FLAG_REQUEST | _FLAG_EVENT | _HESSIAN2_SERIALIZATION_ID
        if self.is_twoway():
            flag |= _FLAG_TWOWAY
        return flag


class DubboHeartBeatResponse(_HeartBeat):
    def _get_flag(self):
        flag = _FLAG_RESPONSE | _FLAG_EVENT | _HESSIAN2_SERIALIZATION_ID
        if self.is_twoway():
            flag |= _FLAG_TWOWAY
        return flag


class DubboResponse(object):
    OK = 20
    UnknownError = 90

    def __init__(self, id, status, data, error):
        self.id = id
        self.status = status
        self.data = data
        self.error = error

    def encode(self):
        stream = BytesIO()
        self._encode_header(stream)  # 12byte header
        self._encode_body(stream)  # 4byte body length + body
        stream.seek(0)
        try:
            return stream.read()
        finally:
            stream.close()

    @property
    def ok(self):
        return self.status == self.OK

    def _encode_header(self, stream):
        stream.write(_DUBBO_MAGIC)
        stream.write(int_to_bytes(_FLAG_RESPONSE | _HESSIAN2_SERIALIZATION_ID))  # flag
        stream.write(int_to_bytes(self.status))  # status
        stream.write(long_to_bytes(self.id))

    def _encode_body(self, stream):
        body = self._get_body()
        stream.write(int_to_bytes(len(body), 4))
        stream.write(body)

    def _get_body(self):
        if self.error is None:
            status_byte = self.data is None and int_to_bytes(2 + 0x90) or int_to_bytes(1 + 0x90)
            return status_byte + encode_object(self.data, 0, [])
        return encode_object(self.error, 0, [])

    def __repr__(self):
        return f'id: {self.id}, status: {self.status}, data: {self.data}, error: {self.error}'


def new_object(cls_name, **fields):
    ''' generate a dynamic typed object with specified fields '''
    cls = namedtuple(cls_name.replace('.', '__DOT__'), fields.keys())
    cls.__name__ = cls_name

    return cls(**fields)
