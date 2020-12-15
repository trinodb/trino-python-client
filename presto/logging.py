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

import logging
import inspect
import string

LEVEL = logging.INFO

class LoggingFormatError(Exception):
    """
    Raise this exception when the logging adapter is unable to
    deduce whether arguments are meant for formatting or for the logger itself.
    """

class LogMessage:
    def __init__(self, fmt, args, kwargs):
        self.fmt = fmt
        self.args = args
        self.kwargs = kwargs

    def __str__(self):
        return str(self.fmt).format(*self.args, **self.kwargs)

    @property
    def keywords(self):
        return 

class FormatLogger(logging.LoggerAdapter):
    def __init__(self, logger, extra=None):
        super(FormatLogger, self).__init__(logger, extra or {})

        # The keywords may be different per instance of FormatLogger depending 
        # on whether this logger is derived and _log is overridden.
        sig = inspect.signature(self.logger._log)
        self.logger_keywords = set(p.name for p in sig.parameters.values() if p.default != inspect.Parameter.empty)
    
    def filter_kwargs(self, msg, kwargs):
        msg_keywords = set(fname for _, fname, _, _ in string.Formatter().parse(msg) if fname)

        ambiguous_parameters = self.logger_keywords & msg_keywords
        if ambiguous_parameters:
            raise LoggingFormatError("Ambiguous parameters {} used during logging.".format(ambiguous_parameters))

        logger_kwargs = {}
        msg_kwargs = {}
        for name, val in kwargs.items():
            if name in logger_kwargs:
                logger_kwargs[name] = val
            else:
                msg_kwargs[name] = val
        return logger_kwargs, msg_kwargs

    def log(self, level, msg, *args, **kwargs):
        if self.isEnabledFor(level):
            msg, kwargs = self.process(msg, kwargs)
            logger_kwargs, msg_kwargs = self.filter_kwargs(msg, kwargs)
            self.logger._log(level, LogMessage(msg, args, msg_kwargs), tuple(), **logger_kwargs)

# TODO: provide interface to use ``logging.dictConfig``
def get_logger(name, log_level=LEVEL):
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    return FormatLogger(logger)
