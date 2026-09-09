import re
import struct
import logging
import binascii
from io import BytesIO
from collections import namedtuple
from datetime import datetime
from ..utils import int_to_bytes, bytes_to_int, bytes_to_long, double_to_bytes, \
    bytes_to_double, timestamp_to_datetime, long_to_bytes
from ..java_class import JavaList, java_typed_data_to_python
from ..types import long, double


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


_MAP_TYPES = frozenset([
    'java.util.Map', 'java.util.HashMap', 'java.util.LinkedHashMap',
    'java.util.TreeMap', 'java.util.SortedMap', 'java.util.NavigableMap',
    'java.util.Hashtable', 'java.util.IdentityHashMap', 'java.util.WeakHashMap',
    'java.util.EnumMap', 'java.util.Properties',
    'java.util.concurrent.ConcurrentHashMap',
])


class Decoder(object):
    def __init__(self, stream):
        self._stream = stream
        self._twoway = False
        # Hessian2 三张独立引用表（spec §5）：
        #   _refs        值引用：已读出的 list/map/object，按读取顺序下标引用（0x51）
        #   _class_defs  类定义：'C' 定义的 {type, fields}
        #   _types       类型引用：typed list/map 的 type 字符串
        self._refs = []
        self._class_defs = []
        self._types = []

    def decode(self):
        header = self._read(2)
        if header[:2] != _DUBBO_MAGIC:
            header += self._read_until_prompt()
            return header
        else:
            header += self._read(14)
        flag = header[2]
        proto = flag & _SERIALIZATION_MASK
        if proto != _HESSIAN2_SERIALIZATION_ID:
            # P0-F1: 非 hessian2 序列化（如 fastjson=6/jdk=1/gson=4）不能被当作 hessian2 解析
            raise RuntimeError('unsupported serialization id "%d" (only hessian2 id %d supported)'
                               % (proto, _HESSIAN2_SERIALIZATION_ID))
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
            logging.warning('Unable to decode message "%s"' % self._stream.read())
            raise
        finally:
            left_bytes = self._stream.read()
            if left_bytes:
                logging.warning('bytes "%s" undecoded!' % binascii.hexlify(left_bytes))
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
            # P0-F2: 对齐 DecodeableRpcResult.java
            #   1 = RESPONSE_VALUE（数据）
            #   0 = RESPONSE_WITH_EXCEPTION（异常）
            #   2 = RESPONSE_NULL_VALUE（空）
            status_code = self._read_int()
            if status_code == 1:
                data = self._read_object()
            elif status_code == 0:
                error = self._read_object()
        else:
            # 帧头 status != OK 时，body 直接是序列化的错误消息
            error = self._read_object()
        # #9: Java 2.7+ RpcResult 在 value/exception 后附加 attachments map；
        # 老版本/我们自身 encode 无此字段，有剩余才读，避免 undecoded
        pos = self._stream.tell()
        if self._stream.read(1):
            self._stream.seek(pos)
            self._read_object()  # attachments map
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
        elif tag in (ord(b'I'), _BC_LONG_INT):
            return self._read(4)
        elif tag in _DIRECT_LONG:
            return int_to_bytes(tag - _BC_LONG_ZERO)
        elif tag in _BYTE_LONG:
            return int_to_bytes(tag - _BC_LONG_BYTE_ZERO) + self._read(1)
        elif tag in _SHORT_LONG:
            return int_to_bytes(tag - _BC_LONG_SHORT_ZERO) + self._read(2)
        elif tag == _BYTE_L:
            return self._read(8)
        elif tag in (_BS_STRING, _BS_STRING_TRUNK) or tag in _ZERO_BYTE or 0x30 <= tag <= 0x33:
            return self._read_string_bytes(tag)
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
        elif tag in (ord(b'I'), _BC_LONG_INT):
            return bytes_to_int(self._read(4), signed=True)
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
        elif tag in (_BS_STRING, _BS_STRING_TRUNK) or tag in _ZERO_BYTE or 0x30 <= tag <= 0x33:
            return _cesu8_decode(self._read_string_bytes(tag))
        elif tag in (ord(b'A'), ord(b'B'), ord(b'b')) or tag in range(0x20, 0x2f + 1) or tag in range(0x34, 0x37 + 1):
            return self._read_binary(tag)
        elif tag == 0x55:  # variable length list typed
            return self._read_variable_list(typed=True)
        elif tag == 0x57:  # variable length list untyped
            return self._read_variable_list(typed=False)
        elif tag == 0x56:  # fixed list typed
            self._read_type()  # list type（记入类型表）
            length = self._read_int()
            return self._read_list(length, JavaList)
        elif tag == 0x58:  # fixed list untyped
            length = self._read_int()
            return self._read_list(length)
        elif tag in range(0x70, 0x78):  # compact fixed list typed
            self._read_type()
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
        elif tag == ord(b'O'):  # object long form: 'O' + class-def index + values
            idx = self._read_int()
            return self._read_object_instance(idx)
        elif tag in range(0x60, 0x6f + 1):  # object compact form: [x60-x6f] values*
            return self._read_object_instance(tag - 0x60)
        elif tag == _BC_REF:
            idx = self._read_int()
            try:
                return self._refs[idx]
            except IndexError:
                raise RuntimeError('value reference not found, idx: %d' % idx)
        elif tag == 0x5a:  # b'Z'
            raise EOFError
        else:
            raise RuntimeError('unknown code "%s"' % tag)

    def _read_list(self, length, list_type=None):
        if list_type:
            result = list_type()
        else:
            result = []
        # 读元素前先登记值引用，保证元素可自引用/互引用（H6）
        self._refs.append(result)
        for _ in range(length):
            result.append(self._read_object())
        return result

    def _read_variable_list(self, typed):
        if typed:
            self._read_type()
            result = JavaList()
        else:
            result = []
        self._refs.append(result)
        while True:
            tag = ord(self._read(1))
            if tag == 0x5a:  # 'Z' 终止
                return result
            result.append(self._read_object(tag))

    def _read_object_def(self):
        type_ = self._read_bytes()
        len_ = self._read_int()
        field_names = []
        for _ in range(len_):
            field_names.append(self._read_bytes())
        self._class_defs.append({'type': type_, 'fields': field_names})

    def _read_object_instance(self, def_idx):
        try:
            ref = self._class_defs[def_idx]
        except IndexError:
            raise RuntimeError('class definition not found, idx: %d' % def_idx)
        type_name = ref['type'].decode()
        field_names = [fn.decode() for fn in ref['fields']]
        args = [self._read_object() for _ in field_names]
        obj = new_object(type_name, **dict(zip(field_names, args)))
        # namedtuple 不可变，值引用只能在字段全部读完、对象创建后登记；
        # 因此“对象的属性引用对象自身”这种自引用目前无法正确还原（Java 对象可变，无此限制）
        self._refs.append(obj)
        return obj

    def _read_int(self, tag=None):
        if tag is None:
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
            return bytes_to_int(self._read(4), signed=True)
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

    def _read_type(self):
        ''' 读取类型引用：字符串形式入类型表，int 形式按下标查类型表（H7） '''
        tag = ord(self._read(1))
        if tag in (_BS_STRING, _BS_STRING_TRUNK) or tag in _ZERO_BYTE or 0x30 <= tag <= 0x33:
            type_ = _cesu8_decode(self._read_string_bytes(tag))
            self._types.append(type_)
            return type_
        idx = self._read_int(tag)
        try:
            if idx < 0:
                raise IndexError
            return self._types[idx]
        except IndexError:
            raise RuntimeError('type reference not found, idx: %d' % idx)

    def _read_string_bytes(self, tag):
        ''' 读取一个字符串（可能分块），返回原始 UTF-8(CESU-8) 字节（H3） '''
        sbuf = b''
        while True:
            if tag == _BS_STRING_TRUNK:  # 'R' 非终块
                chunk_len = bytes_to_int(self._read(2))
                for _ in range(chunk_len):
                    sbuf += self._read_char()
                tag = ord(self._read(1))
            elif tag == _BS_STRING:  # 'S' 终块
                chunk_len = bytes_to_int(self._read(2))
                for _ in range(chunk_len):
                    sbuf += self._read_char()
                return sbuf
            elif tag in _ZERO_BYTE:  # [x00-x1f] 定长
                chunk_len = tag - 0x00
                for _ in range(chunk_len):
                    sbuf += self._read_char()
                return sbuf
            elif 0x30 <= tag <= 0x33:  # [x30-x33] 短定长
                chunk_len = (tag - 0x30) * 256 + ord(self._read(1))
                for _ in range(chunk_len):
                    sbuf += self._read_char()
                return sbuf
            else:
                raise RuntimeError('read string error: code "%d"' % tag)

    def _read_binary(self, tag):
        ''' 读取二进制数据（可能分块）：'A'/'B' 分块（Java 参考实现）、兼容 spec 小写 'b'、紧凑形式 '''
        sbuf = b''
        while True:
            if tag in (ord(b'A'), ord(b'b'), ord(b'B')):
                chunk_len = bytes_to_int(self._read(2))
                final = tag == ord(b'B')
            elif tag in range(0x20, 0x2f + 1):
                chunk_len = tag - 0x20
                final = True
            elif tag in range(0x34, 0x37 + 1):
                chunk_len = (tag - 0x34) * 256 + ord(self._read(1))
                final = True
            else:
                raise RuntimeError('read binary error: code "%d"' % tag)
            sbuf += self._read(chunk_len)
            if final:
                return sbuf
            tag = ord(self._read(1))

    def _read_map(self, code=None):
        if code == ord(b'M'):
            # typed map：java.util.*Map 反序列化为 dict，其余按对象还原（H9）
            type_ = self._read_type()
            is_map = type_ is None or type_ in _MAP_TYPES or type_.endswith('Map')
        else:
            type_ = None
            is_map = True
        if is_map:
            result = {}
            self._refs.append(result)  # 读条目前先登记，支持自引用 map
        else:
            result = {}
        code = self._read(1)
        while code not in (b'z', b'Z'):
            key = self._read_object(code and ord(code))
            value = self._read_object()
            result[key] = value
            code = self._read(1)
        if is_map:
            return result
        obj = new_object(type_, **result)
        self._refs.append(obj)
        return obj

    def _read_char(self):
        ch = self._read(1)
        int_ch = ord(ch)
        if int_ch < 0x80:
            return ch
        elif (int_ch & 0xe0) == 0xc0:
            return ch + self._read(1)
        elif ((int_ch & 0xf0) == 0xe0):
            return ch + self._read(2)

        raise RuntimeError('unknown charactor "%d"' % int_ch)

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


class _EncodeState(object):
    ''' 一次 encode 调用内的共享状态：类定义表 + 值引用表（H11） '''

    def __init__(self, class_names=None):
        self.class_names = list(class_names) if class_names else []
        self.value_refs = {}  # id(obj) -> ref index


_STRING_DIRECT_MAX = 0x1f
_STRING_SHORT_MAX = 0x3ff
_STRING_CHUNK_UNITS = 0xffff  # 分块长度上限：16-bit 长度字段
_BC_STRING_SHORT = 0x30
_BINARY_CHUNK_SIZE = 0x2000  # 8192，对齐 caucho Hessian2Output.writeBytes
_INT32_MIN = -0x80000000
_INT32_MAX = 0x7fffffff


def _utf16_units(s):
    ''' 统计字符串的 UTF-16 单元数（spec: 长度字段按 16-bit 字符计数） '''
    return len(s.encode('utf-16-be', errors='surrogatepass')) // 2


def _cesu8_encode(s):
    ''' 编码为 CESU-8：代理对各 3 字节，对齐 Java Hessian2Output.printString '''
    out = bytearray()
    for ch in s:
        cp = ord(ch)
        if cp < 0x80:
            out.append(cp)
        elif cp < 0x800:
            out += bytes((0xc0 | (cp >> 6), 0x80 | (cp & 0x3f)))
        elif cp < 0x10000:
            out += bytes((0xe0 | (cp >> 12), 0x80 | ((cp >> 6) & 0x3f), 0x80 | (cp & 0x3f)))
        else:
            # 非 BMP：拆成高低两个代理，各 3 字节
            c = cp - 0x10000
            hi = 0xd800 | (c >> 10)
            lo = 0xdc00 | (c & 0x3ff)
            out += bytes((0xe0 | (hi >> 12), 0x80 | ((hi >> 6) & 0x3f), 0x80 | (hi & 0x3f)))
            out += bytes((0xe0 | (lo >> 12), 0x80 | ((lo >> 6) & 0x3f), 0x80 | (lo & 0x3f)))
    return bytes(out)


def _cesu8_decode(b):
    ''' CESU-8 → Python str，合并合法代理对；非法代理忠实返回 '''
    s = b.decode('utf-8', errors='surrogatepass')
    try:
        return s.encode('utf-16-be', errors='surrogatepass').decode('utf-16-be')
    except UnicodeDecodeError:
        return s


def _string_chunks(s, max_units):
    ''' 按码点切块，保证不切断代理对 '''
    chunks = []
    start = 0
    units = 0
    for i, ch in enumerate(s):
        units += 2 if ord(ch) > 0xffff else 1
        if units > max_units:
            chunks.append(s[start:i])
            start = i
            units = 2 if ord(ch) > 0xffff else 1
    if start < len(s):
        chunks.append(s[start:])
    return chunks


def _encode_string(s):
    ''' 编码字符串：短串用直接/短形式，长串按 64K(UTF-16 单元) 分块（H1） '''
    units = _utf16_units(s)
    if units <= _STRING_DIRECT_MAX:
        return int_to_bytes(units) + _cesu8_encode(s)
    elif units <= _STRING_SHORT_MAX:
        return int_to_bytes((_BC_STRING_SHORT << 8) + units) + _cesu8_encode(s)
    result = b''
    chunks = _string_chunks(s, _STRING_CHUNK_UNITS)
    for i, chunk in enumerate(chunks):
        tag = b'S' if i == len(chunks) - 1 else b'R'  # 终块 S / 非终块 R
        result += tag + int_to_bytes(_utf16_units(chunk), 2) + _cesu8_encode(chunk)
    return result


def _encode_int(field):
    ''' 编码整数：普通 int 按取值范围自动在 int/long 间选择（P0-T2） '''
    if field >= -0x10 and field <= 0x2f:
        return int_to_bytes(field + _BC_INT_ZERO)
    elif field >= -0x800 and field <= 0x7ff:
        return int_to_bytes((_BC_INT_BYTE_ZERO << 8) + field)
    elif field >= -0x40000 and field <= 0x3ffff:
        return int_to_bytes((_BC_INT_SHORT_ZERO << 16) + field)
    elif field >= _INT32_MIN and field <= _INT32_MAX:
        return b'I' + int_to_bytes(field, 4, signed=True)
    # 超出 int32 范围：自动升为 64-bit long，避免静默截断
    return b'L' + long_to_bytes(field)


def _encode_long(field):
    ''' 编码 long（显式 long 标记） '''
    if -0x08 <= field and field <= 0x0f:
        return int_to_bytes(field + _BC_LONG_ZERO)
    elif -0x800 <= field and field <= 0x7ff:
        return int_to_bytes((_BC_LONG_BYTE_ZERO << 8) + field)
    elif -0x40000 <= field and field <= 0x3ffff:
        return int_to_bytes((_BC_LONG_SHORT_ZERO << 16) + field)
    elif field >= _INT32_MIN and field <= _INT32_MAX:
        return chr(_BC_LONG_INT).encode() + int_to_bytes(field, 4, signed=True)
    return b'L' + long_to_bytes(field)


def _take_ref(field, state):
    ''' 若对象已编码过则返回引用字节；否则登记并返回 None '''
    ref = state.value_refs.get(id(field))
    if ref is not None:
        return b'\x51' + _encode_int(ref)
    state.value_refs[id(field)] = len(state.value_refs)
    return None


def encode_object(field, idx=0, cls_names=None):
    ''' encode an object into hessian2 stream '''
    state = _EncodeState(cls_names)
    return _encode_object(field, idx, state)


def _encode_binary(data):
    ''' 编码二进制：紧凑/短/分块。分块非终块用 'A'(0x41)、终块 'B'(0x42)，对齐 caucho
    Hessian2Output.writeBytes（spec 文档写的小写 'b' 与参考实现不符，Hessian2Input 实际不认
    0x62）。块大小 8192 仅为安全上限，Java 按 length 字段读任意合法长度，互通不受影响。 '''
    length = len(data)
    if length <= 0x0f:
        return int_to_bytes(0x20 + length) + data
    elif length <= 0x3ff:
        return int_to_bytes((0x34 << 8) + length) + data
    result = b''
    for i in range(0, length, _BINARY_CHUNK_SIZE):
        chunk = data[i:i + _BINARY_CHUNK_SIZE]
        tag = b'B' if i + len(chunk) >= length else b'A'
        result += tag + int_to_bytes(len(chunk), 2) + chunk
    return result


def _encode_date(dt):
    ''' 编码 datetime：x4a + 8 字节 UTC 毫秒（与 decode 0x4a/0x4b 对称，T5） '''
    import calendar
    millis = int(calendar.timegm(dt.utctimetuple()) * 1000 + dt.microsecond / 1000)
    return chr(_BYTE_DATE).encode() + long_to_bytes(millis)


def _encode_object(field, idx, state):
    if field is None:
        return b'N'
    elif field is True:
        return b'T'
    elif field is False:
        return b'F'
    elif isinstance(field, str):
        return _encode_string(field)
    elif isinstance(field, bytes):
        return _encode_binary(field)
    elif isinstance(field, datetime):
        return _encode_date(field)
    elif isinstance(field, dict):
        ref = _take_ref(field, state)
        if ref is not None:
            return ref
        result = b'H'
        for k, v in field.items():
            result += _encode_object(k, idx, state)
            result += _encode_object(v, idx, state)
        result += b'Z'
        return result
    elif isinstance(field, (list, set)):
        ref = _take_ref(field, state)
        if ref is not None:
            return ref
        result = b''
        type_ = type(field).__name__
        if len(field) < 8:
            if type_ not in ('list', 'set'):
                result += int_to_bytes(len(field) + 0x70)
                result += _encode_object(type_, idx, state)
            else:
                result += int_to_bytes(len(field) + 0x78)
        else:
            if type_ not in ('list', 'set'):
                result += b'\x56'
                result += _encode_object(type_, idx, state)
            else:
                result += b'\x58'
            result += _encode_object(len(field), idx, state)
        for e in field:
            result += _encode_object(e, idx, state)
        return result
    elif isinstance(field, long):
        return _encode_long(field)
    elif isinstance(field, int):
        return _encode_int(field)
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
        ref = _take_ref(field, state)
        if ref is not None:
            return ref
        cls_name = field.__class__.__name__
        if cls_name not in state.class_names:
            state.class_names.append(cls_name)
            # 类定义只在首次出现时写出
            result = b'C' + _encode_object(cls_name, idx, state)
            result += _encode_object(len(field._fields), idx, state)
            for field_name in field._fields:
                result += _encode_object(field_name, idx, state)
        else:
            result = b''
        # 对象引用类定义：0x60 + 类定义下标
        result += int_to_bytes(state.class_names.index(cls_name) + 0x60)
        for field_name in field._fields:
            result += _encode_object(getattr(field, field_name), idx, state)
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
    # P1-F4: 补齐 Dubbo 协议响应状态码
    OK = 20
    CLIENT_TIMEOUT = 30
    SERVER_TIMEOUT = 31
    BAD_REQUEST = 40
    BAD_RESPONSE = 50
    SERVICE_NOT_FOUND = 60
    SERVICE_ERROR = 70
    SERVER_ERROR = 80
    CLIENT_ERROR = 90
    UnknownError = 90  # 兼容旧名
    SERVER_THREADPOOL_EXHAUSTED_ERROR = 100

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
        if self.status != self.OK:
            # 帧头 status != OK：body 直接是序列化的错误消息（对齐 Dubbo 实现）
            return encode_object(self.error)
        if self.error is not None:
            # OK 状态携带业务异常：RESPONSE_WITH_EXCEPTION(0) + 异常对象（P1-F3）
            return int_to_bytes(0 + 0x90) + encode_object(self.error)
        status_byte = self.data is None and int_to_bytes(2 + 0x90) or int_to_bytes(1 + 0x90)
        return status_byte + encode_object(self.data, 0, [])

    def __repr__(self):
        return f'id: {self.id}, status: {self.status}, data: {self.data}, error: {self.error}'


def new_object(cls_name, **fields):
    ''' generate a dynamic typed object with specified fields '''
    cls = namedtuple(cls_name.replace('.', '__DOT__'), fields.keys())
    cls.__name__ = cls_name

    return cls(**fields)
