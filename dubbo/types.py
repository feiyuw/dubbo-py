''' Java 数值类型到 Python 的映射。

Python3 中 int/float 本身无界，这里的 long/double 子类只作为显式类型标注：
- ``long`` 标记希望按 Hessian2 64-bit long（'L'/'Y'/紧凑 long）编码的整数；
- ``double`` 标记希望按 64-bit double 编码的浮点数。

它们不应再注入 ``__builtins__``（那会全局污染、依赖 import 顺序、且在新版本
Python 上无意义）。普通 int 会按取值自动在 int/long 之间选择。
'''


class long(int):
    pass


class double(float):
    pass


__all__ = ('long', 'double')