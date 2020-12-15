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
import pytest
import logging
import presto.logging

def test_log_no_args(caplog):
    with caplog.at_level(logging.DEBUG):
        logger = presto.logging.get_logger(__name__, log_level=logging.DEBUG)
        logger.debug("hello")
        logger.info("there")

    assert len(caplog.records) == 2
    assert caplog.records[0].levelno == logging.DEBUG
    assert caplog.records[1].levelno == logging.INFO
    assert caplog.records[0].message == "hello"
    assert caplog.records[1].message == "there"

def test_log_args(caplog):
    with caplog.at_level(logging.DEBUG):
        logger = presto.logging.get_logger(__name__, log_level=logging.DEBUG)
        logger.debug("args: {} {} {}", 1, 2, 3)
        logger.info("ordered: {2} {1} {0}", 1, 2, 3)

    assert len(caplog.records) == 2
    assert caplog.records[0].levelno == logging.DEBUG
    assert caplog.records[1].levelno == logging.INFO
    assert caplog.records[0].message == "args: 1 2 3"
    assert caplog.records[1].message == "ordered: 3 2 1"
    
def test_log_kwargs(caplog):
    with caplog.at_level(logging.DEBUG):
        logger = presto.logging.get_logger(__name__, log_level=logging.DEBUG)
        logger.debug("kwargs: {a} {b} {c}", c=2, b=1, a=3)

    assert len(caplog.records) == 1
    assert caplog.records[0].levelno == logging.DEBUG
    assert caplog.records[0].message == "kwargs: 3 1 2"

def test_log_conflict_kwargs():
    with pytest.raises(presto.logging.LoggingFormatError):
        logger = presto.logging.get_logger(__name__, log_level=logging.DEBUG)
        logger.debug("kwargs: {stack_info}", stack_info=5)
    
