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

"""This package provides Python interfaces to Comdb2 databases.

Two different Python submodules are provided for interacting with Comdb2
databases.  Both submodules work from Python 2.7+ and from Python 3.5+.

`comdb2.dbapi2` provides an interface that conforms to `the Python Database API
Specification v2.0 <https://www.python.org/dev/peps/pep-0249/>`_.  If you're
already familiar with the Python DB-API, or if you intend to use libraries that
expect to be given DB-API compliant connections, this module is likely to be
the best fit for you.  Additionally, if a better way of communicating with
a Comdb2 database than ``libcdb2api`` is ever introduced, this module will be
upgraded to it under the hood.

`comdb2.cdb2` provides a thin, pythonic wrapper over cdb2api.  If you're more
familiar with ``libcdb2api`` than with the Python DB-API and you don't
anticipate a need to interact with libraries that require DB-API compliant
connections, this module may be simpler to get started with.
"""

from ._about import *
