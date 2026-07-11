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
import time
from unittest import mock

import pytest

from trino.client import _RequestHeartbeat
from trino.client import ClientSession
from trino.client import DecodableSegment
from trino.client import InlineSegment
from trino.client import SegmentIterator
from trino.client import SpooledSegment
from trino.client import TrinoQuery
from trino.client import TrinoRequest


def _mock_trino_request():
    req = TrinoRequest(
        host="coordinator",
        port=8080,
        client_session=ClientSession(user="test"),
        http_scheme="http",
    )
    req._next_uri = "http://coordinator/v1/statement/q/1"
    return req


def _head_response(status_code):
    return mock.Mock(status_code=status_code, ok=(200 <= status_code < 300))


@pytest.fixture
def ensure_max_failures_3():
    # Some tests assume _RequestHeartbeart.MAX_FAILURES is set to 3
    with mock.patch.object(_RequestHeartbeat, "MAX_FAILURES", 3):
        yield


def test_heartbeat_sends_head_to_next_uri():
    req = _mock_trino_request()
    with mock.patch.object(req, "head", return_value=_head_response(200)) as mock_head:
        with _RequestHeartbeat(req, interval=0.01):
            time.sleep(0.1)
    assert mock_head.call_count >= 2
    mock_head.assert_called_with(req.next_uri)


@pytest.mark.parametrize("status_code", (404, 405))
def test_heartbeat_stops_on_404_405(status_code):
    req = _mock_trino_request()
    with mock.patch.object(req, "head", return_value=_head_response(status_code)) as mock_head:
        with _RequestHeartbeat(req, interval=0.01):
            time.sleep(0.1)
    # 404/405 means the server does not support heartbeat requests; they should stop after the first one
    assert mock_head.call_count == 1


def test_heartbeat_stops_after_max_failures_non_2xx(ensure_max_failures_3):
    req = _mock_trino_request()
    with mock.patch.object(req, "head", return_value=_head_response(500)) as mock_head:
        with _RequestHeartbeat(req, interval=0.01):
            time.sleep(0.1)
    assert mock_head.call_count == _RequestHeartbeat.MAX_FAILURES


def test_heartbeat_stops_after_max_failures_on_exception(ensure_max_failures_3):
    req = _mock_trino_request()
    with mock.patch.object(req, "head", side_effect=Exception("network error")) as mock_head:
        with _RequestHeartbeat(req, interval=0.01):
            time.sleep(0.1)
    assert mock_head.call_count == _RequestHeartbeat.MAX_FAILURES


def test_heartbeat_resets_failure_count_on_success(ensure_max_failures_3):
    req = _mock_trino_request()
    # Failure counter resets on 200 so the heartbeat keeps running past initial failures
    responses = [_head_response(500), _head_response(500)] + [_head_response(200)] * 20
    with mock.patch.object(req, "head", side_effect=responses) as mock_head:
        with _RequestHeartbeat(req, interval=0.01):
            time.sleep(0.1)
    assert mock_head.call_count > _RequestHeartbeat.MAX_FAILURES


def test_heartbeat_skips_when_next_uri_is_none():
    req = _mock_trino_request()
    req._next_uri = None
    with mock.patch.object(req, "head") as mock_head:
        with _RequestHeartbeat(req, interval=0.01):
            time.sleep(0.1)
    mock_head.assert_not_called()


def test_heartbeat_stop_is_immediate():
    req = _mock_trino_request()
    with mock.patch.object(req, "head", return_value=_head_response(200)):
        hb = _RequestHeartbeat(req, interval=30)
        start = time.monotonic()
        with hb:
            pass
        elapsed = time.monotonic() - start
    assert elapsed < 1.0


def _spooled_iterator(request, heartbeat_interval, rows=None):
    """SegmentIterator with one SpooledSegment and a pre-set mock decoder."""
    segment = DecodableSegment("json", None, mock.Mock(spec=SpooledSegment))
    mapper = mock.Mock()
    it = SegmentIterator([segment], mapper, request=request, heartbeat_interval=heartbeat_interval)
    it._decoder = mock.Mock()
    it._decoder.decode.return_value = rows if rows is not None else [[1, 2]]
    return it


@pytest.mark.parametrize(
    "trino_request, interval",
    [(None, 1.0), (_mock_trino_request(), None), (_mock_trino_request(), 0.0)]
)
def test_iterator_value_error_when_only_request_or_heartbeat_interval_specified(trino_request, interval):
    with pytest.raises(ValueError):
        _ = _spooled_iterator(trino_request, interval)


def test_heartbeat_starts_during_spooled_segment_download():
    req = _mock_trino_request()
    iterator = _spooled_iterator(req, heartbeat_interval=30.0)
    with mock.patch("trino.client._RequestHeartbeat") as MockHB:
        next(iterator)
    MockHB.assert_called_once_with(req, 30.0)
    # Make sure MockHB instance is used as a context manager
    MockHB.return_value.__enter__.assert_called_once()
    MockHB.return_value.__exit__.assert_called_once()


def test_no_heartbeat_for_inline_segment():
    segment = DecodableSegment("json", None, mock.Mock(spec=InlineSegment))
    mapper = mock.Mock()
    iterator = SegmentIterator([segment], mapper, request=_mock_trino_request(), heartbeat_interval=30.0)
    iterator._decoder = mock.Mock()
    iterator._decoder.decode.return_value = [[1, 2]]
    with mock.patch("trino.client._RequestHeartbeat") as MockHB:
        next(iterator)
    MockHB.assert_not_called()


@pytest.mark.parametrize("interval", (None, 0.0))
def test_no_heartbeat_when_interval_none_or_zero(interval):
    iterator = _spooled_iterator(request=None, heartbeat_interval=interval)
    with mock.patch("trino.client._RequestHeartbeat") as MockHB:
        next(iterator)
    MockHB.assert_not_called()


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


@pytest.mark.parametrize("heartbeat_interval", (30.0, None))
def test_fetch_passes_request_and_interval_to_segment_iterator(heartbeat_interval):
    session = ClientSession(user="test", encoding="json", heartbeat_interval=heartbeat_interval)
    req = TrinoRequest(host="coordinator", port=8080, client_session=session, http_scheme="http")
    req._next_uri = "http://coordinator/v1/statement/q1/1"
    query = TrinoQuery(req, query="SELECT 1")
    query._row_mapper = mock.Mock()

    with mock.patch.object(req, "get", return_value=_spooled_fetch_response()):
        with mock.patch("trino.client.SegmentIterator") as MockSI:
            MockSI.return_value = iter([])
            query.fetch()

    assert MockSI.call_args.kwargs["request"] is req
    assert MockSI.call_args.kwargs["heartbeat_interval"] == heartbeat_interval


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
