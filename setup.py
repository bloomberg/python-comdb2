# Copyright 2017 Bloomberg Finance L.P.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
    install_requires=["cffi>=1.0.0", "six", "pytz"],
    tests_require=["python-dateutil>=2.6.0", "pytest"]
)
