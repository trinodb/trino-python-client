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

import abc

from six import with_metaclass
import ipaddress
import socket
from typing import Any, Text  # NOQA
from six.moves.urllib_parse import urlparse


class RedirectHandler(with_metaclass(abc.ABCMeta)):  # type: ignore
    @abc.abstractmethod
    def handle(self, url):
        pass


def _normalize_url_with_hostname(url):
    # type: (Text) -> Text
    # TODO: replace urlparse by more robust utf-8 handling code
    parsed = urlparse(url.encode("utf-8"))
    hostname = parsed.hostname.decode("utf-8")  # type: ignore
    try:
        ipaddress.ip_address(hostname)
        hostname = socket.gethostbyaddr(hostname)[0].encode("utf-8")
    except ValueError:
        return url
    return parsed._replace(netloc=b"%s:%d" % (hostname, parsed.port)).geturl()


class GatewayRedirectHandler(RedirectHandler):
    def handle(self, url):
        # type: (Text) -> Text
        if url is None:
            return None
        return _normalize_url_with_hostname(url)
