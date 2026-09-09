''' T2: 与真实 Java hessian2（com.caucho:hessian）的双向互通测试。

对端工具目在 java-interop/（`mvn -DskipTests package` 生成 target/java-interop.jar）。
jar 不存在时自动 skip，便于纯 Python 环境仍可跑其余测试。
'''
import os
import subprocess
from datetime import datetime
from io import BytesIO

import pytest

from dubbo.codec.hessian2 import Decoder, encode_object

_JAR = os.path.normpath(os.path.join(
    os.path.dirname(__file__), '..', 'java-interop', 'target', 'java-interop.jar'))


def _java(args):
    return subprocess.check_output(
        ['java', '-jar', _JAR] + args, stderr=subprocess.STDOUT).decode()


@pytest.fixture(scope='module')
def java_golden():
    if not os.path.exists(_JAR):
        pytest.skip('java-interop jar 不存在：请在 java-interop/ 下执行 mvn -DskipTests package')
    fixtures = {}
    for line in _java(['encode']).splitlines():
        if '\x07' in line:
            name, hexs = line.split('\x07')
            fixtures[name] = hexs
    return fixtures


def _py_decode(hexs):
    return Decoder(BytesIO(bytes.fromhex(hexs)))._read_object()


# ---------------------------------------------------------------------------
# Java → Python：真实 Java 序列化字节 → dubbo-py 解码，值相等
# ---------------------------------------------------------------------------

_INT_CASES = {
    'i0': 0, 'i1': 1, 'i_neg1': -1, 'i47': 47, 'i_neg16': -16,
    'i1000': 1000, 'i_neg1000': -1000, 'i190000': 190000, 'i_neg190000': -190000,
    'i300000': 300000, 'i_max': 2147483647, 'i_min': -2147483648,
    'long_2_40': 2 ** 40, 'long_3billion': 3000000000,
    'long_neg_3billion': -3000000000, 'long_1234567890': 1234567890,
}


@pytest.mark.parametrize('name,expected', sorted(_INT_CASES.items()))
def test_java_to_python_int(java_golden, name, expected):
    assert _py_decode(java_golden[name]) == expected


_DOUBLE_CASES = {
    'd0': 0.0, 'd1': 1.0, 'd127': 127.0, 'd_neg127': -127.0,
    'd1_123': 1.123, 'd0_12345': 0.12345,
}


@pytest.mark.parametrize('name,expected', sorted(_DOUBLE_CASES.items()))
def test_java_to_python_double(java_golden, name, expected):
    assert _py_decode(java_golden[name]) == expected


_STRING_CASES = {
    's_short': 'abcde',
    's_empty': '',
    's_100': 'a' * 100,
    's_10000': 'a' * 10000,
    's_70000': 'a' * 70000,
    's_zh': '长字符串' * 20000,
    's_emoji': '😀',
}


@pytest.mark.parametrize('name,expected', sorted(_STRING_CASES.items()))
def test_java_to_python_string(java_golden, name, expected):
    assert _py_decode(java_golden[name]) == expected


def test_java_to_python_bool_and_none(java_golden):
    assert _py_decode(java_golden['b_true']) is True
    assert _py_decode(java_golden['b_false']) is False
    assert _py_decode(java_golden['n_null']) is None


def test_java_to_python_date(java_golden):
    assert _py_decode(java_golden['date_utc']) == datetime(2018, 7, 30, 14, 41, 4, 62000)


def test_java_to_python_bytes(java_golden):
    assert _py_decode(java_golden['bytes_3']) == b'\x01\x02\x03'
    assert _py_decode(java_golden['bytes_300']) == b'\x2a' * 300


def test_java_to_python_list(java_golden):
    assert _py_decode(java_golden['list_2']) == [0, 1]
    assert _py_decode(java_golden['list_8']) == [0, 1, 2, 3, 4, 5, 6, 7]
    assert _py_decode(java_golden['nested_list']) == [[0, 1], 2]


def test_java_to_python_map(java_golden):
    assert _py_decode(java_golden['map_hm']) == {'color': 'aquamarine', 'model': 'Beetle'}


# ---------------------------------------------------------------------------
# Python → Java：dubbo-py 序列化 → Java 反序列化，值相等
# ---------------------------------------------------------------------------

def _java_decode(hexs):
    return _java(['decode', hexs]).strip()


_PY_TO_JAVA_CASES = [
    (0, '0'),
    (-1, '-1'),
    (1000, '1000'),
    (-1000, '-1000'),
    (190000, '190000'),
    (2 ** 31 - 1, '2147483647'),
    (-2 ** 31, '-2147483648'),
    (2 ** 40, '1099511627776'),
    (3000000000, '3000000000'),
    (True, 'true'),
    (False, 'false'),
    (None, 'null'),
    ('abcde', '"abcde"'),
    ('a' * 10000, '"' + 'a' * 10000 + '"'),
    ('😀', '"😀"'),
    ('长字符串', '"长字符串"'),
    (1.5, '1.5'),
    (0.12345, '0.12345'),
    ([0, 1], '[0, 1]'),
    ([0, 1, 2, 3, 4, 5, 6, 7], '[0, 1, 2, 3, 4, 5, 6, 7]'),
    ({'color': 'red'}, '{"color": "red"}'),
]


@pytest.mark.parametrize('value,expected_rendered', _PY_TO_JAVA_CASES)
def test_python_to_java(value, expected_rendered):
    if not os.path.exists(_JAR):
        pytest.skip('java-interop jar 不存在')
    hexs = encode_object(value).hex()
    assert _java_decode(hexs) == expected_rendered