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

import os
from codecs import open
from typing import Any, Dict

from setuptools import find_packages, setup

about: Dict[str, Any] = {}
here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, "trino", "_version.py"), "r", "utf-8") as f:
    exec(f.read(), about)

with open(os.path.join(here, "README.md"), "r", "utf-8") as f:
    readme = f.read()

kerberos_require = ["requests_kerberos"]
gssapi_require = ["requests_gssapi"]
sqlalchemy_require = ["sqlalchemy >= 1.3"]
external_authentication_token_cache_require = ["keyring"]

# We don't add localstorage_require to all_require as users must explicitly opt in to use keyring.
all_require = kerberos_require + sqlalchemy_require

tests_require = all_require + gssapi_require + [
    # httpretty >= 1.1 duplicates requests in `httpretty.latest_requests`
    # https://github.com/gabrielfalcao/HTTPretty/issues/425
    "httpretty < 1.1",
    "pytest",
    "pytest-runner",
    "pre-commit",
    "black",
    "isort",
]

setup(
    name=about["__title__"],
    author=about["__author__"],
    author_email=about["__author_email__"],
    version=about["__version__"],
    url=about["__url__"],
    packages=find_packages(include=["trino", "trino.*"]),
    package_data={"": ["LICENSE", "README.md"]},
    description=about["__description__"],
    long_description=readme,
    long_description_content_type="text/markdown",
    license=about["__license__"],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX",
        "Operating System :: Microsoft :: Windows",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Topic :: Database :: Front-Ends",
    ],
    python_requires=">=3.8",
    install_requires=[
        "backports.zoneinfo;python_version<'3.9'",
        "python-dateutil",
        "pytz",
        # requests CVE https://github.com/advisories/GHSA-j8r2-6x86-q33q
        "requests>=2.31.0",
        "tzlocal",
    ],
    extras_require={
        "all": all_require,
        "kerberos": kerberos_require,
        "gssapi": gssapi_require,
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
