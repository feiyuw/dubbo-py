''' Hessian2 协议符合性修复的回归测试（P0/P1 编解码项）。

测试向量均取自 Hessian 2.0 Serialization Protocol 原始 spec。
按 TDD：先写测试（RED），再实现（GREEN）。
'''
import struct
import pytest
from datetime import datetime
from io import BytesIO

from dubbo import long
from dubbo.codec.hessian2 import Decoder, encode_object, new_object, DubboResponse
from dubbo.java_class import JavaList


def _decode(bs):
    return Decoder(BytesIO(bs))._read_object()


def _frame(flag, status, body):
    ''' 构造 dubbo 帧：magic + flag + status + invoke_id + body_length + body '''
    return b'\xda\xbb' + bytes([flag, status]) + struct.pack('>q', 1) + struct.pack('>I', len(body)) + body


# ---------------------------------------------------------------------------
# F1: 序列化 ID 校验（flag 低 5 位必须 == 2，hessian2）
# ---------------------------------------------------------------------------

def test_unsupported_serialization_id_raises():
    # flag = 0x06 → proto(6)=fastjson，不能被当作 hessian2 解析
    frame = _frame(0x06, 20, b'\x91N')
    with pytest.raises(RuntimeError, match='serialization'):
        Decoder(BytesIO(frame)).decode()


def test_hessian2_serialization_id_ok():
    frame = _frame(0x02, 20, b'\x91HZ')
    resp = Decoder(BytesIO(frame)).decode()
    assert resp.data == {}


# ---------------------------------------------------------------------------
# F2 / F3: 响应体标志字节（0=异常 1=数据 2=空）与错误映射
# ---------------------------------------------------------------------------

def test_response_exception_flag_maps_to_error():
    # OK 头 + flag(0) + 异常对象 → error 字段（此前会错放到 data）
    frame = _frame(0x02, 20, b'\x90' + encode_object('boom'))
    resp = Decoder(BytesIO(frame)).decode()
    assert resp.status == DubboResponse.OK
    assert resp.data is None
    assert resp.error == 'boom'


def test_response_null_flag_maps_to_none():
    frame = _frame(0x02, 20, b'\x92')
    resp = Decoder(BytesIO(frame)).decode()
    assert resp.status == DubboResponse.OK
    assert resp.data is None
    assert resp.error is None


def test_response_error_encode_with_exception_flag():
    # F3: error 非空时响应体必须带 RESPONSE_WITH_EXCEPTION(0) 标志字节，能自解码
    resp = DubboResponse(1, DubboResponse.OK, None, 'boom')
    out = Decoder(BytesIO(resp.encode())).decode()
    assert out.status == DubboResponse.OK
    assert out.data is None
    assert out.error == 'boom'


# ---------------------------------------------------------------------------
# T5: 类型映射补全 —— bytes -> binary、datetime -> date 的 encode（与原 decode 对称）
# ---------------------------------------------------------------------------

def test_encode_binary_short_direct():
    assert encode_object(b'\x01\x02\x03') == b'\x23\x01\x02\x03'
    assert encode_object(b'') == b'\x20'
    assert encode_object(b'abc') == b'\x23abc'


def test_encode_binary_medium_uses_compact():
    data = b'x' * 300
    enc = encode_object(data)
    assert enc[0:1] == b'\x35'  # [x34-x37] 短形式
    assert _decode(enc) == data


def test_encode_binary_large_uses_B():
    data = b'x' * 4096
    enc = encode_object(data)
    assert enc[0:1] == b'B'
    assert enc[1:3] == struct.pack('>H', 4096)
    assert _decode(enc) == data


def test_binary_roundtrip_various_sizes():
    for n in (1, 15, 16, 300, 1023, 1024, 4096, 70000):
        data = bytes(range(256)) * (n // 256 + 1)
        data = data[:n]
        assert _decode(encode_object(data)) == data


def test_datetime_encode_uses_x4a_and_roundtrips():
    dt = datetime(2018, 7, 30, 14, 41, 4)  # 以 UTC 毫秒编码
    enc = encode_object(dt)
    assert enc[0:1] == b'J'  # 0x4a
    assert _decode(enc) == dt


def test_datetime_minute_form_decode():
    # x4b：4 字节分钟数（2018-07-30 14:46 UTC）
    assert _decode(b'K\x01\x85\xda6') == datetime(2018, 7, 30, 14, 46)


def test_bytes_is_not_encoded_as_string():
    # 回归：bytes 不能落入 str 分支（旧行为是报 unknown field）
    assert encode_object(b'\x00\x01') != b'\x02\x00\x01'


def test_emoji_string_roundtrip_cesu8():
    # 非 BMP 字符按 CESU-8（代理对各 3 字节）编码，与 Java Hessian2Output 对齐
    # 向量取自真实 Java 序列化输出（java-interop 对端）
    assert encode_object('😀') == b'\x02\xed\xa0\xbd\xed\xb8\x80'
    assert _decode(bytes.fromhex('02eda0bdedb880')) == '😀'
    assert _decode(encode_object('😀')) == '😀'
    assert _decode(encode_object('a😀b')) == 'a😀b'


def test_response_status_constants():
    assert DubboResponse.OK == 20
    assert DubboResponse.CLIENT_TIMEOUT == 30
    assert DubboResponse.SERVER_TIMEOUT == 31
    assert DubboResponse.BAD_REQUEST == 40
    assert DubboResponse.BAD_RESPONSE == 50
    assert DubboResponse.SERVICE_NOT_FOUND == 60
    assert DubboResponse.SERVICE_ERROR == 70
    assert DubboResponse.SERVER_ERROR == 80
    assert DubboResponse.CLIENT_ERROR == 90
    assert DubboResponse.UnknownError == 90
    assert DubboResponse.SERVER_THREADPOOL_EXHAUSTED_ERROR == 100


# ---------------------------------------------------------------------------
# T2: int / long 的有符号编码与 int32 溢出自动升级
# ---------------------------------------------------------------------------

def test_encode_int_over_int32_promotes_to_long():
    # > 2^31-1 的普通 int 应自动按 long 编码，而不是静默截断
    assert encode_object(3_000_000_000) == b'L' + struct.pack('>q', 3_000_000_000)


def test_encode_negative_int32_uses_signed_bytes():
    # -2^31 .. -262145 之间的负 int 走 'I'，必须用有符号大端
    assert encode_object(-300000) == b'I' + struct.pack('>i', -300000)


def test_encode_int_beyond_int32_but_in_int64():
    assert encode_object(2 ** 40) == b'L' + struct.pack('>q', 2 ** 40)


def test_encode_long_negative():
    # long 的 'Y'(32bit 转 long) 与 'L'(64bit) 都要有符号
    assert encode_object(long(-300000)) == b'Y' + struct.pack('>i', -300000)
    assert encode_object(long(-3_000_000_000)) == b'L' + struct.pack('>q', -3_000_000_000)


def test_decode_int_signed():
    assert _decode(b'I' + struct.pack('>i', -300000)) == -300000
    assert _decode(b'I\x00\x04\xf1#') == 323875  # 普通正 int（'I' 原先无法解码的回归）


def test_decode_long_signed():
    assert _decode(b'L' + struct.pack('>q', -1)) == -1
    assert _decode(b'Y' + struct.pack('>i', -300000)) == -300000


def test_int_long_roundtrip():
    ints = [0, 1, -1, 47, -16, 1000, -1000, 190000, -190000, 323875, -300000,
            2 ** 31 - 1, -2 ** 31, 2 ** 31, -2 ** 31 - 1, 2 ** 40, -2 ** 40]
    for v in ints:
        assert _decode(encode_object(v)) == v
    longs = [0, 1, -1, 1000, -1000, 190000, -190000, -300000,
             2 ** 31 - 1, -2 ** 31, 3_000_000_000, -3_000_000_000]
    for v in longs:
        assert _decode(encode_object(long(v))) == v


# ---------------------------------------------------------------------------
# H1 / H3: 长字符串按 64K 分块编码、'R'/'S' 分块解码
# ---------------------------------------------------------------------------

def test_encode_string_longer_than_64k_uses_chunks():
    s = 'a' * 70000
    enc = encode_object(s)
    # 首字节应为非终块 'R'（0x52），而非 'S'+3字节长度
    assert enc[0:1] == b'R'
    assert _decode(enc) == s


def test_decode_chunked_string():
    # 'R' 非终块 + 'S' 终块 拼接
    stream = b'R\x00\x03abc' + b'S\x00\x03def'
    assert _decode(stream) == 'abcdef'


def test_decode_64k_chunked_string_roundtrip():
    s = '长字符串' * 20000  # 80000 个 UTF-16 单元级字符
    assert _decode(encode_object(s)) == s


# ---------------------------------------------------------------------------
# H4: 二进制分块解码
# ---------------------------------------------------------------------------

def test_decode_binary_final_chunk():
    assert _decode(b'B\x00\x03\x01\x02\x03') == b'\x01\x02\x03'


def test_decode_binary_multi_chunk():
    # 真实 Java（caucho）用 'A' 非终块 + 'B' 终块（块大小 8192）
    assert _decode(b'A\x00\x03abc' + b'B\x00\x02de') == b'abcde'
    # 兼容 spec 文档写的小写 'b'
    assert _decode(b'b\x00\x03abc' + b'B\x00\x02de') == b'abcde'


def test_decode_binary_compact_short():
    assert _decode(b'\x23\x01\x02\x03') == b'\x01\x02\x03'
    assert _decode(b'\x34\x03abc') == b'abc'


# ---------------------------------------------------------------------------
# H5: 对象实例长形式 'O'
# ---------------------------------------------------------------------------

def test_decode_object_instance_long_form():
    # class-def "Car"(3) 1 field "color"(5) ... 'O' + class-def idx + 1 value
    stream = b'C\x03Car\x91\x05color' + b'O\x90\x03red'
    car = _decode(stream)
    assert car.__class__.__name__ == 'Car'
    assert car.color == 'red'


# ---------------------------------------------------------------------------
# H6: 值引用表（循环 / 共享引用）
# ---------------------------------------------------------------------------

def test_decode_circular_map():
    m = _decode(b'H\x91\x51\x90Z')
    assert isinstance(m, dict)
    assert m[1] is m


def test_decode_circular_list():
    lst = _decode(b'\x58\x91\x51\x90')  # 固定 untyped list，length=1，元素引用自身
    assert isinstance(lst, list)
    assert lst[0] is lst


def test_decode_shared_object_ref():
    # list[对象, 对同一对象的引用]
    stream = b'\x58\x92' + b'C\x0dexample.Color\x91\x04name' + b'\x60\x03RED' + b'\x51\x91'
    result = _decode(stream)
    assert result[0] is result[1]
    assert result[0].name == 'RED'


# ---------------------------------------------------------------------------
# H7: 类型引用表
# ---------------------------------------------------------------------------

def test_decode_list_type_reference():
    # spec Figure 16：第二个 typed list 用 int 引用第一个的 type
    stream = b'\x72\x04[int\x90\x91' + b'\x73\x90\x92\x93\x94'
    dec = Decoder(BytesIO(stream))
    first = dec._read_object()
    second = dec._read_object()
    assert isinstance(first, JavaList) and first == [0, 1]
    assert isinstance(second, JavaList) and second == [2, 3, 4]


# ---------------------------------------------------------------------------
# H8: 变长 list
# ---------------------------------------------------------------------------

def test_decode_variable_length_untyped_list():
    assert _decode(b'\x57\x90\x91Z') == [0, 1]


def test_decode_variable_length_typed_list():
    result = _decode(b'\x55\x04[int\x90\x91Z')
    assert isinstance(result, JavaList)
    assert result == [0, 1]


# ---------------------------------------------------------------------------
# H9: typed map -> 对象（非 Map 类型）
# ---------------------------------------------------------------------------

def test_decode_typed_map_to_object():
    # spec Figure 21: Car 以 typed map 表示
    stream = (b'M\x13com.caucho.test.Car'
              b'\x05color\x0aaquamarine'
              b'\x05model\x06Beetle'
              b'Z')
    car = _decode(stream)
    assert car.__class__.__name__ == 'com.caucho.test.Car'
    assert car.color == 'aquamarine'
    assert car.model == 'Beetle'


def test_decode_typed_map_java_map_stays_dict():
    stream = (b'M\x11java.util.HashMap'
              b'\x05color\x0aaquamarine'
              b'Z')
    result = _decode(stream)
    assert isinstance(result, dict)
    assert result == {'color': 'aquamarine'}


# ---------------------------------------------------------------------------
# H11: 编码侧复用类定义与值引用
# ---------------------------------------------------------------------------

def test_long_string_many_chunks_roundtrip():
    # 回归：分块长度字段未定长 2 字节，末块 <256 时被短缩导致协议破坏
    for n in (131071, 1 << 20):
        s = u'x' * n
        assert _decode(encode_object(s)) == s


def test_encode_binary_final_chunk_short_length():
    # 回归：binary 末块长度 <256 时长度字段仍须占 2 字节
    for n in (8192 + 200, 8200):
        data = b'\x2a' * n
        assert _decode(encode_object(data)) == data


def test_encode_reuses_class_definition():
    a = new_object('com.demo.Car', color='red', model='x')
    b = new_object('com.demo.Car', color='green', model='y')
    enc = encode_object([a, b])
    # 类定义 'C' 只应出现一次，第二个对象用 0x60 短形式
    # 'com.demo.Car' 为 12 字符，长度前缀为 \x0c
    assert enc.count(b'C\x0ccom.demo.Car') == 1
    # 解码后结构一致
    assert _decode(enc) == [a, b]


def test_encode_shared_object_as_ref():
    shared = new_object('com.demo.Car', color='red', model='x')
    enc = encode_object([shared, shared])
    result = _decode(enc)
    assert result[0] is result[1]
    assert result[0].color == 'red'