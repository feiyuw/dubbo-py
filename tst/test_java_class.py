from dubbo.java_class import java_typed_data_to_python


def test_java_typed_data_to_python():
    assert java_typed_data_to_python('short', '2') == 2
    assert java_typed_data_to_python('int', '2') == 2
    assert java_typed_data_to_python('double', '1') == 1.0
    assert java_typed_data_to_python('float', '1') == 1.0
    assert java_typed_data_to_python('java.lang.String', '1a') == '1a'
