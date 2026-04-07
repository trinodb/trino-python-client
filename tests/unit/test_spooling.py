
import unittest
from unittest.mock import MagicMock, patch
from trino.client import TrinoQuery, TrinoRequest, ClientSession, TrinoResult
from trino.client import SegmentIterator

class TestTrinoQueryLazy(unittest.TestCase):
    def setUp(self):
        self.mock_request = MagicMock(spec=TrinoRequest)
        self.client_session = ClientSession("user")
        self.mock_request.client_session = self.client_session

    def test_fetch_returns_iterator_for_spooled_segments(self):
        # Mock the initial POST response
        post_response = MagicMock()
        post_response.id = "query_1"
        post_response.stats = {}
        post_response.info_uri = "info"
        post_response.next_uri = "next_1"
        post_response.rows = [] # No rows initially
        
        self.mock_request.process.return_value = post_response
        self.mock_request.post.return_value = MagicMock()
        
        query = TrinoQuery(self.mock_request, "SELECT 1")
        
        # Execute should return empty result initially but try to fetch
        # We need to mock fetch behavior too since execute calls it if rows are empty
        
        # Mock the GET response for fetch()
        get_response_status = MagicMock()
        get_response_status.next_uri = None # Finished
        get_response_status.stats = {}
        # Status rows as dict indicates spooling protocol
        get_response_status.rows = {
            "encoding": "json",
            "segments": [
                {"type": "spooled", "uri": "u1", "ackUri": "a1", "metadata": {"segmentSize": "10", "uncompressedSize": "10"}}
            ],
            "metadata": {}
        }
        
        # When execute calls fetch(), it calls request.get -> process -> returns get_response_status
        self.mock_request.process.side_effect = [post_response, get_response_status]
        self.mock_request.get.return_value = MagicMock()
        
        # Mock _to_segments to return a list of decodable segments
        # We can just verify that fetch returns a SegmentIterator
        # But _to_segments is internal.
        
        # We need to patch SegmentIterator or check the return type
        
        result = query.execute()
        
        # Verify result.rows is a SegmentIterator, NOT a list
        self.assertIsInstance(result.rows, SegmentIterator)
        self.assertNotIsInstance(result.rows, list)
        
    def test_fetch_returns_list_for_normal_segments(self):
         # Mock the initial POST response
        post_response = MagicMock()
        post_response.id = "query_1"
        post_response.stats = {}
        post_response.info_uri = "info"
        post_response.next_uri = "next_1"
        post_response.rows = [] 
        
        # Mock the GET response for fetch()
        get_response_status = MagicMock()
        get_response_status.next_uri = None
        get_response_status.stats = {}
        get_response_status.rows = [[1], [2]] # Normal list rows
        
        self.mock_request.process.side_effect = [post_response, get_response_status]
        
        query = TrinoQuery(self.mock_request, "SELECT 1")
        result = query.execute()
        
        # Verify result.rows is a list (appended)
        self.assertIsInstance(result.rows, list)
        self.assertEqual(result.rows, [[1], [2]])

if __name__ == '__main__':
    unittest.main()
