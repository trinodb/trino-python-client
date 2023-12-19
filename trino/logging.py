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

import logging
import os
from typing import Optional

_level = os.environ.get('TRINO_LOGLEVEL', 'INFO')
if _level.upper() == _level and hasattr(logging, _level) and isinstance(getattr(logging, _level), int):
    LEVEL = getattr(logging, _level)
elif _level == 'NONE':
    LEVEL = None
else:
    LEVEL = logging.INFO


# TODO: provide interface to use ``logging.dictConfig``
def get_logger(name: str, log_level: Optional[int] = LEVEL) -> logging.Logger:
    logger = logging.getLogger(name)
    if log_level is not None:
        logger.setLevel(log_level)
    return logger
