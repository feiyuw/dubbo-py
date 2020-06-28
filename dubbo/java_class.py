JavaList = type('java.util.List', (list, ), {})
JavaLong = type('java.lang.Long', (long, ), {})
JavaMap = type('java.util.Map', (dict, ), {})
JavaString = type('java.lang.String', (str, ), {})


# TODO: support more data types
_type_handlers = {
    'boolean': bool,
    'short': int,
    'int': int,
    'float': float,
    'double': float,
    'java.lang.String': str,
    'java.lang.Long': long,
    'java.lang.Float': float,
    'java.lang.Double': float,
    'java.lang.Integer': int,
    'java.lang.Short': int,
    'java.lang.Boolean': bool,
}


def java_typed_data_to_python(type_, data):
    handler = _type_handlers.get(type_)
    if not handler:
        raise RuntimeError(f'unsupported type "{type_}"')
    return handler(data)
