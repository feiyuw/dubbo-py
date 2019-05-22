import os
from setuptools import setup

CURDIR = os.path.abspath(os.path.dirname(__file__))


setup(
    name='dubbo_py',
    version='0.1',
    description='dubbo adaptor for python',
    author='Zhang Yu',
    author_email='feiyuw@gmail.com',
    url='https://github.com/feiyuw/dubbo_py.git',
    install_requires=['kazoo'],
    packages=[
        'dubbo',
        'dubbo.codec'],
    platforms='any',
)
