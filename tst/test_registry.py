''' #2: zk 服务发现（group/version 路由）的回归测试。'''
from urllib.parse import quote_plus

import pytest

from dubbo import registry
from dubbo.registry import parse_provider_url, discover


def test_parse_provider_url():
    url = ('dubbo://10.0.1.120:12345/a.service'
           '?anyhost=true&application=x&group=dubbo&version=1.0.0&interface=a.service')
    host, port, params = parse_provider_url(url)
    assert host == '10.0.1.120'
    assert port == 12345
    assert params['group'] == 'dubbo'
    assert params['version'] == '1.0.0'
    assert params['interface'] == 'a.service'


def _url(host, port, group, version):
    return quote_plus(f'dubbo://{host}:{port}/a.service?group={group}&version={version}')


def _patch_kazoo(monkeypatch, providers):
    class FakeKazoo(object):
        def __init__(self, ps):
            self._providers = ps
            self.closed = False

        def start(self):
            pass

        def get_children(self, path):
            self.path = path
            return self._providers

        def close(self):
            self.closed = True

    monkeypatch.setattr(registry, 'KazooClient', lambda zk: FakeKazoo(providers))
    return FakeKazoo


def test_discover_filters_group_and_version(monkeypatch):
    providers = [
        _url('10.0.1.1', 10001, 'dubbo', '1.0.0'),
        _url('10.0.1.2', 10002, 'other', '1.0.0'),
        _url('10.0.1.3', 10003, 'dubbo', '2.0.0'),
    ]
    _patch_kazoo(monkeypatch, providers)
    host, port = discover('zk-1.test:2181', 'a.service', group='dubbo', version='1.0.0')
    assert (host, port) == ('10.0.1.1', 10001)


def test_discover_no_group_constraint_picks_first(monkeypatch):
    providers = [
        _url('10.0.1.1', 10001, 'dubbo', '1.0.0'),
        _url('10.0.1.2', 10002, 'other', '1.0.0'),
    ]
    _patch_kazoo(monkeypatch, providers)
    host, port = discover('zk-1.test:2181', 'a.service')
    assert (host, port) == ('10.0.1.1', 10001)


def test_discover_no_match_raises(monkeypatch):
    _patch_kazoo(monkeypatch, [_url('10.0.1.1', 10001, 'dubbo', '1.0.0')])
    with pytest.raises(RuntimeError, match='no provider'):
        discover('zk-1.test:2181', 'a.service', group='none')