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
from setuptools import setup


_version_re = re.compile(r"__version__\s+=\s+(.*)")


with open("presto/__init__.py", "rb") as f:
    version = str(
        ast.literal_eval(_version_re.search(f.read().decode("utf-8")).group(1))
    )

tests_require = ["pytest", "pytest-runner"]

setup(
    name="presto-client",
    author="Trino Team",
    author_email="python-client@trino.io",
    version=version,
    url="https://github.com/trinodb/trino-python-client",
    packages=["presto"],
    package_data={"": ["LICENSE", "README.md"]},
    description="Presto Client is now Trino",
    long_description="""
This was a package for PrestoSQL. The package itself is no longer maintained,
as PrestoSQL got renamed to Trino. Read more at
https://trino.io/blog/2020/12/27/announcing-trino.html

If you are using an older PrestoSQL release, you can install a previous
version of the package with:

    pip install presto-client==0.302.0

The package has been superseded with a client for Trino. You can install it
with:

    pip install trino

Apologies for the disruption and very short notice, resulting in no transition
period.
""",
    license="Apache 2.0",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX",
        "Operating System :: Microsoft :: Windows",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Topic :: Database :: Front-Ends",
    ],
    extras_require={
        "all": [],
        "tests": tests_require,
    },
)
