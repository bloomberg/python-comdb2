from setuptools import setup
from comdb2 import __version__

setup(
    name='python-comdb2',
    version=__version__,
    author='Alex Chamberlain',
    author_email='achamberlai9@bloomberg.net',
    packages=['comdb2'],
    setup_requires=["cffi>=1.0.0"],
    cffi_modules=["build.py:ffi"],
    install_requires=["cffi>=1.0.0", "six", "pytz"]
)
