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
from unittest import mock

import pytest

from trino.client import ClientSession
from trino.client import DecodableSegment
from trino.client import SegmentIterator
from trino.client import SpooledSegment
from trino.client import TrinoQuery
from trino.client import TrinoRequest


def _spooled_fetch_response():
    """Minimal spooled protocol GET response JSON."""
    resp = mock.Mock()
    resp.status_code = 200
    resp.ok = True
    resp.headers = {}
    resp.text = json.dumps({
        "id": "q1",
        "infoUri": "http://coordinator/query.html?q1",
        "stats": {"state": "FINISHED"},
        "data": {
            "encoding": "json",
            "segments": [
                {
                    "type": "inline",
                    "metadata": {"uncompressedSize": "10", "segmentSize": "10"},
                    "data": "",
                }
            ],
        },
    })
    return resp


def test_fetch_returns_segment_iterator():
    session = ClientSession(user="test", encoding="json")
    req = TrinoRequest(host="coordinator", port=8080, client_session=session, http_scheme="http")
    req._next_uri = "http://coordinator/v1/statement/q1/1"
    query = TrinoQuery(req, query="SELECT 1")
    query._row_mapper = mock.Mock()

    with mock.patch.object(req, "get", return_value=_spooled_fetch_response()):
        result = query.fetch()

    assert isinstance(result, SegmentIterator)


class _FakeSpooledSegment(SpooledSegment):
    """SpooledSegment that records acknowledgments instead of sending requests."""
    def __init__(self, name):
        super().__init__(
            {
                "type": "spooled",
                "uri": f"http://storage/{name}",
                "ackUri": f"http://storage/{name}/ack",
                "metadata": {"uncompressedSize": "10", "segmentSize": "10"},
            },
            request=None,
        )
        self.acknowledge_count = 0

    def acknowledge(self):
        self.acknowledge_count += 1


class _FlakyDecoder:
    """Decoder that fails the first decode of `failing_segment`, then succeeds."""
    def __init__(self, rows_by_segment, failing_segment):
        self._rows_by_segment = rows_by_segment
        self._failing_segment = failing_segment

    def decode(self, segment):
        if segment is self._failing_segment:
            self._failing_segment = None
            raise IOError("segment download failed")
        return self._rows_by_segment[segment]


@pytest.mark.parametrize("failing_segment_index", (0, 1, 2))
def test_segment_iterator_retries_failed_segment_without_skipping_it(failing_segment_index):
    segs = [_FakeSpooledSegment(name) for name in ("s1", "s2", "s3")]
    segments = [DecodableSegment("json", None, seg) for seg in segs]
    iterator = SegmentIterator(segments, mapper=None)
    iterator._decoder = _FlakyDecoder(
        {seg: [[index + 1]] for index, seg in enumerate(segs)},
        failing_segment=segs[failing_segment_index],
    )

    rows = [next(iterator) for _ in range(failing_segment_index)]
    with pytest.raises(IOError):
        next(iterator)
    # The failed segment was neither acknowledged nor skipped. Retrying should deliver its rows.
    assert segs[failing_segment_index].acknowledge_count == 0
    rows.extend(iterator)
    assert rows == [[1], [2], [3]]
    with pytest.raises(StopIteration):
        next(iterator)
    assert [seg.acknowledge_count for seg in segs] == [1, 1, 1]


def _spooled_segment_with_headers(coordinator_host, custom_headers):
    segment_to = {
        "type": "spooled",
        "uri": "https://coordinator/v1/spooled/download/seg1",
        "ackUri": "https://coordinator/v1/spooled/ack/seg1",
        "headers": {"X-Trino-Spooling-Token": ["token-abc"]},
        "metadata": {"segmentSize": "1", "uncompressedSize": "1"},
    }
    request = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(user="test"),
        http_scheme="https",
    )
    return SpooledSegment(
        segment_to,
        request,
        coordinator_host=coordinator_host,
        custom_headers=custom_headers,
    )


def test_send_spooling_request_forwards_custom_headers_to_coordinator():
    custom_headers = {"X-Auth-Gateway-Token": "user-token"}
    segment = _spooled_segment_with_headers(coordinator_host="coordinator", custom_headers=custom_headers)

    recorded = {}

    def fake_get(uri, headers=None, **kwargs):
        recorded["headers"] = headers
        return mock.Mock(ok=True)

    segment._request._get = fake_get
    segment._send_spooling_request(segment.uri)

    assert recorded["headers"]["X-Auth-Gateway-Token"] == "user-token"
    assert recorded["headers"]["X-Trino-Spooling-Token"] == "token-abc"


def test_send_spooling_request_does_not_forward_custom_headers_to_external_storage():
    custom_headers = {"X-Auth-Gateway-Token": "user-token"}
    segment = _spooled_segment_with_headers(coordinator_host="coordinator", custom_headers=custom_headers)

    recorded = {}

    def fake_get(uri, headers=None, **kwargs):
        recorded["headers"] = headers
        return mock.Mock(ok=True)

    segment._request._get = fake_get
    external_uri = "https://s3.amazonaws.com/bucket/seg1?X-Amz-Signature=abc"
    segment._send_spooling_request(external_uri)

    assert "X-Auth-Gateway-Token" not in recorded["headers"]
    assert recorded["headers"]["X-Trino-Spooling-Token"] == "token-abc"


def test_send_spooling_request_segment_header_takes_precedence_over_custom_header():
    # Custom header uses the same name as the segment protocol header; the segment header must win.
    custom_headers = {"X-Trino-Spooling-Token": "should-not-be-used"}
    segment = _spooled_segment_with_headers(coordinator_host="coordinator", custom_headers=custom_headers)

    recorded = {}

    def fake_get(uri, headers=None, **kwargs):
        recorded["headers"] = headers
        return mock.Mock(ok=True)

    segment._request._get = fake_get
    segment._send_spooling_request(segment.uri)

    assert recorded["headers"]["X-Trino-Spooling-Token"] == "token-abc"
