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
"""

This module defines exceptions for Trino operations. It follows the structure
defined in pep-0249.
"""
from typing import Any, Dict, Optional, Tuple, Union

import trino.logging

logger = trino.logging.get_logger(__name__)


# PEP 249 Errors
class Error(Exception):
    pass


class Warning(Exception):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class InternalError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class DataError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


# dbapi module errors (extending PEP 249 errors)
class TrinoAuthError(OperationalError):
    pass


class TrinoDataError(NotSupportedError):
    pass


class TrinoQueryError(Error):
    def __init__(self, error: Union[Dict[str, Any], str], query_id: Optional[str] = None) -> None:
        if isinstance(error, dict):
            self._error = error
        elif isinstance(error, str):
            self._error = {"message": error}
        self._query_id = query_id

    @property
    def error_code(self) -> Optional[int]:
        return self._error.get("errorCode")

    @property
    def error_name(self) -> Optional[str]:
        return self._error.get("errorName")

    @property
    def error_type(self) -> Optional[str]:
        return self._error.get("errorType")

    @property
    def error_exception(self) -> Optional[str]:
        return self.failure_info.get("type") if self.failure_info else None

    @property
    def failure_info(self) -> Optional[Dict[str, Any]]:
        return self._error.get("failureInfo")

    @property
    def message(self) -> str:
        return self._error.get("message", "Trino did not return an error message")

    @property
    def error_location(self) -> Optional[Tuple[int, int]]:
        location = self._error.get("errorLocation")
        if location:
            return (location["lineNumber"], location["columnNumber"])
        else:
            return None

    @property
    def query_id(self) -> Optional[str]:
        return self._query_id

    def __repr__(self) -> str:
        return '{}(type={}, name={}, message="{}", query_id={})'.format(
            self.__class__.__name__,
            self.error_type,
            self.error_name,
            self.message,
            self.query_id,
        )

    def __str__(self) -> str:
        return repr(self)


class TrinoExternalError(TrinoQueryError, OperationalError):
    pass


class TrinoInternalError(TrinoQueryError, InternalError):
    pass


class TrinoUserError(TrinoQueryError, ProgrammingError):
    pass


# client module errors
class HttpError(Exception):
    pass


class Http502Error(HttpError):
    pass


class Http503Error(HttpError):
    pass


class Http504Error(HttpError):
    pass
