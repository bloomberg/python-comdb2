from setuptools import setup

setup(
    name='python-comdb2',
    version='1.0',
    author='Alex Chamberlain',
    author_email='achamberlai9@bloomberg.net',
    packages=['comdb2'],
    setup_requires=["cffi>=1.0.0"],
    cffi_modules=["build.py:ffi"],
    install_requires=["cffi>=1.0.0", "six", "pytz"]
)
