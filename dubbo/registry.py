''' Dubbo 服务发现：从 zookeeper 拉取 provider 并按 group/version 路由（issue #2）。'''
from urllib.parse import unquote, parse_qs, urlparse

from kazoo.client import KazooClient


def parse_provider_url(url):
    ''' 解析 dubbo://host:port/interface?params，返回 (host, port, params)。
    params 值为 query 参数（同名取最后一个）。 '''
    u = urlparse(url)
    if u.hostname is None or u.port is None:
        raise RuntimeError(f'invalid provider url: {url}')
    return u.hostname, u.port, {k: v[-1] for k, v in parse_qs(u.query).items()}


def discover(zk_hosts, service_name, group=None, version=None):
    ''' 从 zk 拉取 {service_name} 的 providers，按 group/version 过滤，返回 (host, port)。
    group/version 为 None 表示不约束该维度；无匹配 provider 时抛 RuntimeError。 '''
    client = KazooClient(zk_hosts)
    client.start()
    try:
        path = f'/dubbo/{service_name}/providers'
        providers = client.get_children(path)
    finally:
        client.close()  # close 内部会 stop，释放连接

    for raw in providers:
        host, port, params = parse_provider_url(unquote(raw))
        if group is not None and params.get('group') != group:
            continue
        if version is not None and params.get('version') != version:
            continue
        return host, port
    raise RuntimeError(
        f'no provider for {service_name} group={group!r} version={version!r}')