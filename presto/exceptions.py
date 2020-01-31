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

This module defines exceptions for Presto operations. It follows the structure
defined in pep-0249.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import functools
import random
import time

import presto.logging

logger = presto.logging.get_logger(__name__)


class HttpError(Exception):
    pass


class Http503Error(HttpError):
    pass


class PrestoError(Exception):
    pass


class TimeoutError(Exception):
    pass


class PrestoQueryError(Exception):
    def __init__(self, error, query_id=None):
        self._error = error
        self._query_id = query_id

    @property
    def error_code(self):
        return self._error.get("errorCode", None)

    @property
    def error_name(self):
        return self._error.get("errorName", None)

    @property
    def error_type(self):
        return self._error.get("errorType", None)

    @property
    def error_exception(self):
        return self.failure_info.get("type", None) if self.failure_info else None

    @property
    def failure_info(self):
        return self._error.get("failureInfo", None)

    @property
    def message(self):
        return self._error.get("message", "Presto did no return an error message")

    @property
    def error_location(self):
        location = self._error["errorLocation"]
        return (location["lineNumber"], location["columnNumber"])

    @property
    def query_id(self):
        return self._query_id

    def __repr__(self):
        return '{}(type={}, name={}, message="{}", query_id={})'.format(
            self.__class__.__name__,
            self.error_type,
            self.error_name,
            self.message,
            self.query_id,
        )

    def __str__(self):
        return repr(self)


class PrestoExternalError(PrestoQueryError):
    pass


class PrestoInternalError(PrestoQueryError):
    pass


class PrestoUserError(PrestoQueryError):
    pass


def retry_with(handle_retry, exceptions, conditions, max_attempts):
    def wrapper(func):
        @functools.wraps(func)
        def decorated(*args, **kwargs):
            error = None
            result = None
            for attempt in range(1, max_attempts + 1):
                try:
                    result = func(*args, **kwargs)
                    if any(guard(result) for guard in conditions):
                        handle_retry.retry(func, args, kwargs, None, attempt)
                        continue
                    return result
                except Exception as err:
                    error = err
                    if any(isinstance(err, exc) for exc in exceptions):
                        handle_retry.retry(func, args, kwargs, err, attempt)
                        continue
                    break
            logger.info("failed after {} attempts".format(attempt))
            if error is not None:
                raise error
            return result

        return decorated

    return wrapper


class DelayExponential(object):
    def __init__(
        self, base=0.1, exponent=2, jitter=True, max_delay=2 * 3600  # 100ms  # 2 hours
    ):
        self._base = base
        self._exponent = exponent
        self._jitter = jitter
        self._max_delay = max_delay

    def __call__(self, attempt):
        delay = float(self._base) * (self._exponent ** attempt)
        if self._jitter:
            delay *= random.random()
        delay = min(float(self._max_delay), delay)
        return delay


class RetryWithExponentialBackoff(object):
    def __init__(
        self, base=0.1, exponent=2, jitter=True, max_delay=2 * 3600  # 100ms  # 2 hours
    ):
        self._get_delay = DelayExponential(base, exponent, jitter, max_delay)

    def retry(self, func, args, kwargs, err, attempt):
        delay = self._get_delay(attempt)
        time.sleep(delay)


# PEP 249
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
