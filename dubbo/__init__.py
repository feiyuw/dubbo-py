class long(int):
    pass


class double(float):
    pass


__builtins__['long'] = long  # add long to builtin
__builtins__['double'] = double  # add double to builtin
