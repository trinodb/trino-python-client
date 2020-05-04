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

HEADER_CATALOG = "X-Presto-Catalog"
HEADER_SCHEMA = "X-Presto-Schema"
HEADER_SOURCE = "X-Presto-Source"
HEADER_USER = "X-Presto-User"
HEADER_CLIENT_INFO = "X-Presto-Client-Info"

HEADER_SESSION = "X-Presto-Session"
HEADER_SET_SESSION = "X-Presto-Set-Session"
HEADER_CLEAR_SESSION = "X-Presto-Clear-Session"

HEADER_STARTED_TRANSACTION = "X-Presto-Started-Transaction-Id"
HEADER_TRANSACTION = "X-Presto-Transaction-Id"
