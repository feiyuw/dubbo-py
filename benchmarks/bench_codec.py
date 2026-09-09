''' T3: Hessian2 编解码性能基准（手动运行，非 pytest 断言，避免 CI 波动）

用法::

    python benchmarks/bench_codec.py

本轮 T3 修复的分块/引用逻辑（长字符串分块、binary 'A'/'B' 分块、值引用表）都可在此观察吞吐。
'''
import os
import sys
import time
from io import BytesIO

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dubbo.codec.hessian2 import Decoder, encode_object


def _bench(name, value, rounds):
    enc = encode_object(value)
    size = len(enc)

    t0 = time.perf_counter()
    for _ in range(rounds):
        Decoder(BytesIO(enc))._read_object()
    decode_ms = (time.perf_counter() - t0) / rounds * 1e3

    t0 = time.perf_counter()
    for _ in range(rounds):
        encode_object(value)
    encode_ms = (time.perf_counter() - t0) / rounds * 1e3

    print(f'{name:20s} size={size:>10d}B  '
          f'encode={encode_ms:8.3f}ms  decode={decode_ms:8.3f}ms  '
          f'enc={size / 1e6 / (encode_ms / 1e3):8.1f}MB/s  '
          f'dec={size / 1e6 / (decode_ms / 1e3):8.1f}MB/s')


def main():
    cases = [
        ('str 1MB 分块', 'x' * (1 << 20)),
        ('str 128K 分块', '长字符串' * 16384),
        ('list 10000 int', list(range(10000))),
        ('map 2000 kv', {f'k{i}': i for i in range(2000)}),
        ('binary 256K 分块', b'\x2a' * (256 << 10)),
    ]
    for name, value in cases:
        _bench(name, value, 50)


if __name__ == '__main__':
    main()