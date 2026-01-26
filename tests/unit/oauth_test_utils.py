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
import json
import re
import uuid
from collections import namedtuple

import httpretty
import keyring.backend

from trino import constants

SERVER_ADDRESS = "https://coordinator"
REDIRECT_PATH = "oauth2/initiate"
TOKEN_PATH = "oauth2/token"
REDIRECT_RESOURCE = f"{SERVER_ADDRESS}/{REDIRECT_PATH}"
TOKEN_RESOURCE = f"{SERVER_ADDRESS}/{TOKEN_PATH}"


class RedirectHandler:
    def __init__(self):
        self.redirect_server = ""

    def __call__(self, url):
        self.redirect_server += url


class RedirectHandlerWithException:
    def __init__(self, exception):
        self.exception = exception

    def __call__(self, url):
        raise self.exception


class PostStatementCallback:
    def __init__(self, redirect_server, token_server, tokens, sample_post_response_data):
        self.redirect_server = redirect_server
        self.token_server = token_server
        self.tokens = tokens
        self.sample_post_response_data = sample_post_response_data

    def __call__(self, request, uri, response_headers):
        authorization = request.headers.get("Authorization")
        if authorization and authorization.replace("Bearer ", "") in self.tokens:
            return [200, response_headers, json.dumps(self.sample_post_response_data)]
        elif self.redirect_server is None and self.token_server is not None:
            return [401,
                    {
                        'Www-Authenticate': (
                            'Bearer realm="Trino", token_type="JWT", '
                            f'Bearer x_token_server="{self.token_server}"'
                        ),
                        'Basic realm': '"Trino"'
                    },
                    ""]
        return [401,
                {
                    'Www-Authenticate': (
                        'Bearer realm="Trino", token_type="JWT", '
                        f'Bearer x_redirect_server="{self.redirect_server}", '
                        f'x_token_server="{self.token_server}"'
                    ),
                    'Basic realm': '"Trino"'
                },
                ""]


class GetTokenCallback:
    def __init__(self, token_server, token, attempts=1):
        self.token_server = token_server
        self.token = token
        self.attempts = attempts

    def __call__(self, request, uri, response_headers):
        self.attempts -= 1
        if self.attempts < 0:
            return [404, response_headers, "{}"]
        if self.attempts == 0:
            return [200, response_headers, f'{{"token": "{self.token}"}}']
        return [200, response_headers, f'{{"nextUri": "{self.token_server}"}}']


def _get_token_requests(challenge_id):
    return list(filter(
        lambda r: r.method == "GET" and r.path == f"/{TOKEN_PATH}/{challenge_id}",
        httpretty.latest_requests()))


def _post_statement_requests():
    return list(filter(
        lambda r: r.method == "POST" and r.path == constants.URL_STATEMENT_PATH,
        httpretty.latest_requests()))


class MultithreadedTokenServer:
    Challenge = namedtuple('Challenge', ['token', 'attempts'])

    def __init__(self, sample_post_response_data, attempts=1):
        self.tokens = set()
        self.challenges = {}
        self.sample_post_response_data = sample_post_response_data
        self.attempts = attempts

        # bind post statement
        httpretty.register_uri(
            method=httpretty.POST,
            uri=f"{SERVER_ADDRESS}{constants.URL_STATEMENT_PATH}",
            body=self.post_statement_callback)

        # bind get token
        httpretty.register_uri(
            method=httpretty.GET,
            uri=re.compile(rf"{TOKEN_RESOURCE}/.*"),
            body=self.get_token_callback)

    # noinspection PyUnusedLocal
    def post_statement_callback(self, request, uri, response_headers):
        authorization = request.headers.get("Authorization")

        if authorization and authorization.replace("Bearer ", "") in self.tokens:
            return [200, response_headers, json.dumps(self.sample_post_response_data)]

        challenge_id = str(uuid.uuid4())
        token = str(uuid.uuid4())
        self.tokens.add(token)
        self.challenges[challenge_id] = MultithreadedTokenServer.Challenge(token, self.attempts)
        redirect_server = f"{REDIRECT_RESOURCE}/{challenge_id}"
        token_server = f"{TOKEN_RESOURCE}/{challenge_id}"
        return [401, {'Www-Authenticate': f'Bearer x_redirect_server="{redirect_server}", '
                                          f'x_token_server="{token_server}"',
                      'Basic realm': '"Trino"'}, ""]

    # noinspection PyUnusedLocal
    def get_token_callback(self, request, uri, response_headers):
        challenge_id = uri.replace(f"{TOKEN_RESOURCE}/", "")
        challenge = self.challenges[challenge_id]
        challenge = challenge._replace(attempts=challenge.attempts - 1)
        self.challenges[challenge_id] = challenge
        if challenge.attempts < 0:
            return [404, response_headers, "{}"]
        if challenge.attempts == 0:
            return [200, response_headers, f'{{"token": "{challenge.token}"}}']
        return [200, response_headers, f'{{"nextUri": "{uri}"}}']


class MockKeyring(keyring.backend.KeyringBackend):
    priority = 1

    def __init__(self):
        self.file_location = self._generate_test_root_dir()

    @staticmethod
    def _generate_test_root_dir():
        import tempfile

        return tempfile.mkdtemp(prefix="trino-python-client-unit-test-")

    def _get_file_path(self, servicename, username):
        from os.path import join

        file_location = self.file_location
        file_name = f"{servicename}_{username}.txt"
        return join(file_location, file_name)

    def set_password(self, servicename, username, password):
        file_path = self._get_file_path(servicename, username)

        with open(file_path, "w") as file:
            file.write(password)

    def get_password(self, servicename, username):
        import os

        file_path = self._get_file_path(servicename, username)
        if not os.path.exists(file_path):
            return None

        with open(file_path, "r") as file:
            password = file.read()

        return password

    def delete_password(self, servicename, username):
        import os

        file_path = self._get_file_path(servicename, username)
        if not os.path.exists(file_path):
            return None

        os.remove(file_path)
