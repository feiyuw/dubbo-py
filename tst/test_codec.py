from io import BytesIO
from dubbo.codec.hessian2 import Decoder, _desc_to_cls_names, _cls_names_to_desc, encode_object, DubboResponse, DubboHeartBeatResponse, DubboHeartBeatRequest, new_object
from dubbo.java_class import JavaList


def test_encode_object():
    assert encode_object(None) == b'N'
    assert encode_object(True) == b'T'
    assert encode_object(False) == b'F'
    assert encode_object('abcde') == b'\x05abcde'
    assert encode_object('') == b'\x00'
    assert encode_object(long(1000)) == b'\xfb\xe8'
    assert encode_object(long(190000)) == b'>\xe60'
    assert encode_object(long(1234567890)) == b'YI\x96\x02\xd2'
    assert encode_object(1000) == b'\xcb\xe8'
    assert encode_object(190000) == b'\xd6\xe60'
    assert encode_object(323875) == b'I\x00\x04\xf1#'
    assert encode_object('a' * 100) == b'0d' + b'a' * 100
    assert encode_object('a' * 10000) == b"S'\x10" + b'a' * 10000
    assert encode_object({
        'androidDeviceRoot': False,
        'hardid': '',
        'phone_number': '12345678901',
        'engine_result': 'ACCEPT',
        'process_time': '2018-07-30 14:41:04'}) == b'H\x11androidDeviceRootF\x06hardid\x00\x0cphone_number\x0b12345678901\rengine_result\x06ACCEPT\x0cprocess_time\x132018-07-30 14:41:04Z'
    assert encode_object(new_object('com.xxx.test', a=1, b=2), 0, []) == b'C\x0ccom.xxx.test\x92\x01a\x01b`\x91\x92'
    assert encode_object(JavaList([long(2)])) == b'q\x0ejava.util.List\xe2'
    assert encode_object(JavaList([long(2)] * 8)) == b'\x56\x0ejava.util.List\x98\xe2\xe2\xe2\xe2\xe2\xe2\xe2\xe2'
    assert encode_object([2]) == b'y\x92'

    child = new_object('child', b=long(2))
    obj = new_object('parent', a=child)
    assert encode_object(obj, 0, []) == b'C\x06parent\x91\x01a`C\x05child\x91\x01ba\xe2'

    assert encode_object(0.0) == b'\x5b'
    assert encode_object(1.0) == b'\x5c'
    assert encode_object(127.0) == b'\x5d\x7f'
    assert encode_object(-127.0) == b'\x5d\x81'
    assert encode_object(1.123) == b'\x5f\x00\x00\x04c'
    assert encode_object(-1.123) == b'\x5f\xff\xff\xfb\x9d'
    assert encode_object(0.12345) == b'D?\xbf\x9akP\xb0\xf2|'
    assert encode_object(-0.12345) == b'D\xbf\xbf\x9akP\xb0\xf2|'


def test_decode_object():
    def _read_object(stream):
        return Decoder(BytesIO(stream))._read_object()

    assert _read_object(b'y\x92') == [2]
    assert _read_object(b'z\x91\x92') == [1, 2]
    assert _read_object(b'\x58\x98\x92\x92\x92\x92\x92\x92\x92\x92') == [2] * 8  # list over 7 elements
    assert _read_object(b'q\x0ejava.util.List\xe2') == JavaList([long(2)])
    assert _read_object(b'\x56\x0ejava.util.List\x98\xe2\xe2\xe2\xe2\xe2\xe2\xe2\xe2') == JavaList([long(2)] * 8)
    assert _read_object(b'\x5b') == 0.0
    assert _read_object(b'\x5c') == 1.0
    assert _read_object(b'\x5d\x7f') == 127.0
    assert _read_object(b'\x5d\x81') == -127.0
    assert _read_object(b'\x5e\x00\x80') == 128.0
    assert _read_object(b'\x5f\x00\x00\x04c') == 1.123
    assert _read_object(b'\x5f\xff\xff\xfb\x9d') == -1.123
    assert _read_object(b'D?\xbf\x9akP\xb0\xf2|') == 0.12345
    assert _read_object(b'D\xbf\xbf\x9akP\xb0\xf2|') == -0.12345
    assert _read_object(b'YI\x96\x02\xd2') == long(1234567890)


def test_dubbo_bad_response_decode():
    data = b'\xda\xbb\x02(\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\xd80\xd6Fail to decode request due to: RpcInvocation [methodName=listMenu, parameterTypes=[], arguments=null, attachments={path=com.xxxxxxxxxinc.yyyyyyyyy.api.interfaces.XXXXService, input=103, dubbo=2.5.3, version=1.0.0}]'
    stream = BytesIO(data)
    resp = Decoder(stream).decode()
    assert resp.status == 40
    assert resp.data is None
    assert resp.error == 'Fail to decode request due to: RpcInvocation [methodName=listMenu, parameterTypes=[], arguments=null, attachments={path=com.xxxxxxxxxinc.yyyyyyyyy.api.interfaces.XXXXService, input=103, dubbo=2.5.3, version=1.0.0}]'


def test_desc_to_cls_names():
    assert _desc_to_cls_names('Lcn/com/xxx/SerwVi;VB') == ['cn.com.xxx.SerwVi', 'None', 'bytes']
    assert _desc_to_cls_names('[[Lcom/bbcc/dd;DLcn/com/xxx/yyy;CS') == ['[[Lcom.bbcc.dd;', 'float', 'cn.com.xxx.yyy', 'chr', 'int']
    assert _cls_names_to_desc(['cn.com.xxx.SerwVi', 'NoneType', 'bytes']) == 'Lcn/com/xxx/SerwVi;VB'
    assert _cls_names_to_desc(['[[Lcom.bbcc.dd;', 'float', 'cn.com.xxx.yyy', 'int']) == '[[Lcom/bbcc/dd;DLcn/com/xxx/yyy;I'


def test_heartbeat_decode():
    msg = b'\xda\xbb\x22\x14\x00\x00\x00\x00\x00\x00\x17\x71\x00\x00\x00\x01\x4e'
    hb = Decoder(BytesIO(msg)).decode()
    assert hb.id == 6001
    assert hb.data is None


def test_response_decode():
    msg = b'\xda\xbb\x02\x14\x00\x00\x00\x00\x00\x00\x00\x07\x00\x00\x00\x03\x91\x48\x5a'
    res = Decoder(BytesIO(msg)).decode()
    assert res.id == 7
    assert res.data == {}


def test_response_encode():
    resp = DubboResponse(7, DubboResponse.OK, {}, None)
    assert resp.encode() == b'\xda\xbb\x02\x14\x00\x00\x00\x00\x00\x00\x00\x07\x00\x00\x00\x03\x91\x48\x5a'


def test_new_object():
    obj = new_object('a.b.c.d', a=1, b=2)
    assert obj.__class__.__name__ == 'a.b.c.d'
    assert obj.a == 1
    assert obj.b == 2
    assert obj._fields == ('a', 'b')
    obj_simple = new_object('child', c=1)
    assert obj_simple.__class__.__name__ == 'child'
    assert obj_simple.c == 1
    assert obj_simple._fields == ('c', )


def test_heartbeat_encode():
    assert DubboHeartBeatResponse(570, None, False).encode() == b'\xda\xbb\x22\x00\x00\x00\x00\x00\x00\x00\x02\x3a\x00\x00\x00\x01\x4e'
    assert DubboHeartBeatRequest(570, None, True).encode() == b'\xda\xbb\xe2\x00\x00\x00\x00\x00\x00\x00\x02\x3a\x00\x00\x00\x01\x4e'
