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

import sys
import pkgconfig

from setuptools import setup, Extension

about = {}
with open('comdb2/_about.py') as fp:
    exec(fp.read(), about)


def make_static_extension(name, **kwargs):
    libraries = kwargs.pop('libraries', [])
    pkg_config = pkgconfig.parse(' '.join(libraries), static=True)
    libraries = pkg_config['libraries']
    library_dirs = pkg_config['library_dirs']
    include_dirs = kwargs.pop("include_dirs", []) + pkg_config['include_dirs']
    return Extension(
        name,
        libraries=libraries,
        library_dirs=library_dirs,
        include_dirs=include_dirs,
        **kwargs
    )


ccdb2 = make_static_extension(
    "comdb2._ccdb2",
    extra_compile_args=['-std=c99'],
    libraries=['cdb2api'],
    sources=["comdb2/_ccdb2.pyx"]
)


setup(
    name='comdb2',
    version=about["__version__"],
    author='Alex Chamberlain',
    author_email='achamberlai9@bloomberg.net',
    packages=['comdb2'],
    install_requires=["six", "pytz"],
    extras_require={"tests": ["python-dateutil>=2.6.0", "pytest"]},
    python_requires=">=3.6",
    ext_modules=[ccdb2],
    package_data={"comdb2": ["py.typed", "*.pyi"]},
)
