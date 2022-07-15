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
from enum import Enum, unique
from typing import Iterable

from trino import constants
import trino.client
import trino.exceptions
import trino.logging


logger = trino.logging.get_logger(__name__)


NO_TRANSACTION = "NONE"
START_TRANSACTION = "START TRANSACTION"
ROLLBACK = "ROLLBACK"
COMMIT = "COMMIT"


@unique
class IsolationLevel(Enum):
    AUTOCOMMIT = 0
    READ_UNCOMMITTED = 1
    READ_COMMITTED = 2
    REPEATABLE_READ = 3
    SERIALIZABLE = 4

    @classmethod
    def levels(cls) -> Iterable[str]:
        return {isolation_level.name for isolation_level in IsolationLevel}

    @classmethod
    def values(cls) -> Iterable[int]:
        return {isolation_level.value for isolation_level in IsolationLevel}

    @classmethod
    def check(cls, level: int) -> int:
        if level not in cls.values():
            raise ValueError("invalid isolation level {}".format(level))
        return level


class Transaction(object):
    def __init__(self, request):
        self._request = request
        self._id = NO_TRANSACTION

    @property
    def id(self):
        return self._id

    @property
    def request(self):
        return self._request

    def begin(self):
        response = self._request.post(START_TRANSACTION)
        if not response.ok:
            raise trino.exceptions.DatabaseError(
                "failed to start transaction: {}".format(response.status_code)
            )
        transaction_id = response.headers.get(constants.HEADER_STARTED_TRANSACTION)
        if transaction_id and transaction_id != NO_TRANSACTION:
            self._id = response.headers[constants.HEADER_STARTED_TRANSACTION]
        status = self._request.process(response)
        while status.next_uri:
            response = self._request.get(status.next_uri)
            transaction_id = response.headers.get(constants.HEADER_STARTED_TRANSACTION)
            if transaction_id and transaction_id != NO_TRANSACTION:
                self._id = response.headers[constants.HEADER_STARTED_TRANSACTION]
            status = self._request.process(response)
        self._request.transaction_id = self._id
        logger.info("transaction started: %s", self._id)

    def commit(self):
        query = trino.client.TrinoQuery(self._request, COMMIT)
        try:
            list(query.execute())
        except Exception as err:
            raise trino.exceptions.DatabaseError(
                "failed to commit transaction {}: {}".format(self._id, err)
            )
        self._id = NO_TRANSACTION
        self._request.transaction_id = self._id

    def rollback(self):
        query = trino.client.TrinoQuery(self._request, ROLLBACK)
        try:
            list(query.execute())
        except Exception as err:
            raise trino.exceptions.DatabaseError(
                "failed to rollback transaction {}: {}".format(self._id, err)
            )
        self._id = NO_TRANSACTION
        self._request.transaction_id = self._id
