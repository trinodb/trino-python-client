import abc
from future.standard_library import install_aliases
install_aliases()

from future.utils import with_metaclass
import ipaddress
import socket
from typing import Any  # NOQA
from urllib.parse import urlparse


class RedirectHandler(with_metaclass(abc.ABCMeta)):
    @abc.abstractmethod
    def handle(self, url):
        pass


def _normalize_url_with_hostname(url):
    # TODO: replace urlparse by more robust utf-8 handling code
    parsed = urlparse(url.encode('utf-8'))
    hostname = parsed.hostname.decode('utf-8')
    try:
        ipaddress.ip_address(hostname)
        hostname = socket.gethostbyaddr(hostname)[0].encode('utf-8')
    except ValueError:
        return url
    return parsed._replace(netloc=b'%s:%d' % (hostname, parsed.port)).geturl()


class GatewayRedirectHandler(RedirectHandler):
    def handle(self, url):
        if url is None:
            return None
        return _normalize_url_with_hostname(url)
