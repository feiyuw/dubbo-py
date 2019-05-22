import sys
if sys.version_info.major < 3:
    raise RuntimeError('dubbo_py require python 3+')


class long(int):
    pass


class double(float):
    pass


__builtins__['long'] = long  # add long to builtin
__builtins__['double'] = double  # add double to builtin
