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
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from typing import Any, Optional, Text  # NOQA: mypy types


DEFAULT_PORT = 8080
DEFAULT_SOURCE = "presto-python-client"
DEFAULT_CATALOG = None  # type: Optional[Text]
DEFAULT_SCHEMA = None  # type: Optional[Text]
DEFAULT_AUTH = None  # type: Optional[Any]
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_REQUEST_TIMEOUT = 30.0  # type: float

HTTP = "http"
HTTPS = "https"

URL_STATEMENT_PATH = "/v1/statement"

HEADER_PREFIX = "X-Presto-"
HEADER_CATALOG = HEADER_PREFIX + "Catalog"
HEADER_SCHEMA = HEADER_PREFIX + "Schema"
HEADER_SOURCE = HEADER_PREFIX + "Source"
HEADER_USER = HEADER_PREFIX + "User"

HEADER_SESSION = HEADER_PREFIX + "Session"
HEADER_SET_SESSION = HEADER_PREFIX + "Set-Session"
HEADER_CLEAR_SESSION = HEADER_PREFIX + "Clear-Session"

HEADER_STARTED_TRANSACTION = HEADER_PREFIX + "Started-Transaction-Id"
HEADER_TRANSACTION = HEADER_PREFIX + "Transaction-Id"
