import builtins

import dubbo
from dubbo import double, long


def test_no_builtins_pollution():
    # T1: 导入 dubbo 后不得再向 __builtins__ 注入 long/double
    assert not hasattr(builtins, 'long')
    assert not hasattr(builtins, 'double')


def test_long_double_importable():
    assert issubclass(long, int)
    assert issubclass(double, float)
    assert long(5) == 5
    assert isinstance(long(5), int)
    assert double(1.5) == 1.5
    assert isinstance(double(1.5), float)