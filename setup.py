#!/usr/bin/env python

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import ast
import re
import textwrap

from setuptools import find_packages, setup

_version_re = re.compile(r"__version__\s+=\s+(.*)")

with open("trino/__init__.py", "rb") as f:
    trino_version = _version_re.search(f.read().decode("utf-8"))
    assert trino_version is not None
    version = str(ast.literal_eval(trino_version.group(1)))

kerberos_require = ["requests_kerberos"]
sqlalchemy_require = ["sqlalchemy~=1.3"]
external_authentication_token_cache_require = ["keyring"]

# We don't add localstorage_require to all_require as users must explicitly opt in to use keyring.
all_require = kerberos_require + sqlalchemy_require

tests_require = all_require + [
    # httpretty >= 1.1 duplicates requests in `httpretty.latest_requests`
    # https://github.com/gabrielfalcao/HTTPretty/issues/425
    "httpretty < 1.1",
    "pytest",
    "pytest-runner",
    "click",
    "sqlalchemy_utils",
    "pre-commit",
    "black",
    "isort",
]

setup(
    name="trino",
    author="Trino Team",
    author_email="python-client@trino.io",
    version=version,
    url="https://github.com/trinodb/trino-python-client",
    packages=find_packages(include=["trino", "trino.*"]),
    package_data={"": ["LICENSE", "README.md"]},
    description="Client for the Trino distributed SQL Engine",
    long_description=textwrap.dedent(
        """
    Client for Trino (https://trino.io), a distributed SQL engine for
    interactive and batch big data processing. Provides a low-level client and
    a DBAPI 2.0 implementation.
    """
    ),
    license="Apache 2.0",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX",
        "Operating System :: Microsoft :: Windows",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Topic :: Database :: Front-Ends",
    ],
    python_requires=">=3.7",
    install_requires=["pytz", "requests"],
    extras_require={
        "all": all_require,
        "kerberos": kerberos_require,
        "sqlalchemy": sqlalchemy_require,
        "tests": tests_require,
        "external-authentication-token-cache": external_authentication_token_cache_require,
    },
    entry_points={
        "sqlalchemy.dialects": [
            "trino = trino.sqlalchemy.dialect:TrinoDialect",
        ]
    },
)
