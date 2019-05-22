from dubbo.client import DubboClient


if __name__ == '__main__':
    client = DubboClient('127.0.0.1', 12358)
    resp = client.send_request_and_return_response(service_name='calc', method_name='exp', service_version='1.0', args=[4], attachment={})
    print(resp.data)  # 16
