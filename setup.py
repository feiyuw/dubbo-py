from setuptools import setup


setup(
    name='dubbo-py',
    version='0.2',
    description='dubbo adaptor for python',
    author='Zhang Yu',
    author_email='feiyuw@gmail.com',
    url='https://github.com/feiyuw/dubbo-py.git',
    python_requires='>=3.5',
    install_requires=['kazoo'],
    packages=[
        'dubbo',
        'dubbo.codec'],
    platforms='any',
)
