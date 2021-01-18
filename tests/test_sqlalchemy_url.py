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
from sqlalchemy.engine.url import make_url

import trino
from trino.sqlalchemy import TrinoDialect


def test_connect_args():
    url = make_url('trino://user@localhost')
    connect_args, connect_kwargs = TrinoDialect.create_connect_args(url)
    assert connect_args == []
    assert connect_kwargs == {
        'user': 'user',
        'host': 'localhost',
        'port': 8080,
    }

    url = make_url('trino://user@localhost:8081')
    connect_args, connect_kwargs = TrinoDialect.create_connect_args(url)
    assert connect_args == []
    assert connect_kwargs == {
        'user': 'user',
        'host': 'localhost',
        'port': 8081,
    }

    url = make_url('trino://user:pass@localhost')
    connect_args, connect_kwargs = TrinoDialect.create_connect_args(url)
    assert connect_args == []
    assert connect_kwargs['user'] == 'user'
    assert connect_kwargs['host'] == 'localhost'
    assert connect_kwargs['port'] == 8080
    assert connect_kwargs['auth']._username == 'user'
    assert connect_kwargs['auth']._password == 'pass'

    url = make_url('trino://user@localhost/system/nodes')
    connect_args, connect_kwargs = TrinoDialect.create_connect_args(url)
    assert connect_args == []
    assert connect_kwargs == {
        'user': 'user',
        'host': 'localhost',
        'port': 8080,
        'catalog': 'system',
        'schema': 'nodes',
    }

    url = make_url('trino://user@localhost/system')
    connect_args, connect_kwargs = TrinoDialect.create_connect_args(url)
    assert connect_args == []
    assert connect_kwargs == {
        'user': 'user',
        'host': 'localhost',
        'port': 8080,
        'catalog': 'system',
    }

    url = make_url('trino://user@localhost/system?ssl=true')
    connect_args, connect_kwargs = TrinoDialect.create_connect_args(url)
    assert connect_args == []
    assert connect_kwargs == {
        'user': 'user',
        'host': 'localhost',
        'port': 8080,
        'catalog': 'system',
        'http_scheme': trino.constants.HTTPS
    }
