import unittest
from unittest import mock
import tap_github.__init__ as tap_github
import requests

class Mockresponse:
    def __init__(self, status_code, json, raise_error, headers={'X-RateLimit-Remaining': 1}, text=None, content=None):
        self.status_code = status_code
        self.raise_error = raise_error
        self.text = json
        self.headers = headers
        self.content = content if content is not None else 'github'

    def raise_for_status(self):
        if not self.raise_error:
            return self.status_code

        raise requests.HTTPError("Sample message")

    def json(self):
        return self.text

class MockParseArgs:
    config = {}
    def __init__(self, config):
        self.config = config

def get_args(config):
    return MockParseArgs(config)

def get_response(status_code, json={}, raise_error=False, content=None):
    return Mockresponse(status_code, json, raise_error, content=content)

@mock.patch("time.sleep")
@mock.patch("requests.Session.request")
@mock.patch("singer.utils.parse_args")
class TestTimeoutValue(unittest.TestCase):
    """
        Test case to verify the timeout value is set as expected
    """

    def test_timeout_value_in_config(self, mocked_parse_args, mocked_request, mocked_sleep):
        json = {"key": "value"}
        # mock response
        mocked_request.return_value = get_response(200, json)

        mock_config = {"request_timeout": 100}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config)

        # get the timeout value for assertion
        timeout = tap_github.get_request_timeout()
        # function call
        tap_github.authed_get("test_source", "")

        # verify that we got expected timeout value
        self.assertEquals(100.0, timeout)
        # verify that the request was called with expected timeout value
        mocked_request.assert_called_with(method='get', url='', timeout=100.0)

    def test_timeout_value_not_in_config(self, mocked_parse_args, mocked_request, mocked_sleep):
        json = {"key": "value"}
        # mock response
        mocked_request.return_value = get_response(200, json)

        mock_config = {}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config)

        # get the timeout value for assertion
        timeout = tap_github.get_request_timeout()
        # function call
        tap_github.authed_get("test_source", "")

        # verify that we got expected timeout value
        self.assertEquals(300.0, timeout)
        # verify that the request was called with expected timeout value
        mocked_request.assert_called_with(method='get', url='', timeout=300.0)

    def test_timeout_string_value_in_config(self, mocked_parse_args, mocked_request, mocked_sleep):
        json = {"key": "value"}
        # mock response
        mocked_request.return_value = get_response(200, json)

        mock_config = {"request_timeout": "100"}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config)

        # get the timeout value for assertion
        timeout = tap_github.get_request_timeout()
        # function call
        tap_github.authed_get("test_source", "")

        # verify that we got expected timeout value
        self.assertEquals(100.0, timeout)
        # verify that the request was called with expected timeout value
        mocked_request.assert_called_with(method='get', url='', timeout=100.0)

    def test_timeout_empty_value_in_config(self, mocked_parse_args, mocked_request, mocked_sleep):
        json = {"key": "value"}
        # mock response
        mocked_request.return_value = get_response(200, json)

        mock_config = {"request_timeout": ""}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config)

        # get the timeout value for assertion
        timeout = tap_github.get_request_timeout()
        # function call
        tap_github.authed_get("test_source", "")

        # verify that we got expected timeout value
        self.assertEquals(300.0, timeout)
        # verify that the request was called with expected timeout value
        mocked_request.assert_called_with(method='get', url='', timeout=300.0)

    def test_timeout_0_value_in_config(self, mocked_parse_args, mocked_request, mocked_sleep):
        json = {"key": "value"}
        # mock response
        mocked_request.return_value = get_response(200, json)

        mock_config = {"request_timeout": 0.0}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config)

        # get the timeout value for assertion
        timeout = tap_github.get_request_timeout()
        # function call
        tap_github.authed_get("test_source", "")

        # verify that we got expected timeout value
        self.assertEquals(300.0, timeout)
        # verify that the request was called with expected timeout value
        mocked_request.assert_called_with(method='get', url='', timeout=300.0)

    def test_timeout_string_0_value_in_config(self, mocked_parse_args, mocked_request, mocked_sleep):
        json = {"key": "value"}
        # mock response
        mocked_request.return_value = get_response(200, json)

        mock_config = {"request_timeout": "0.0"}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config)

        # get the timeout value for assertion
        timeout = tap_github.get_request_timeout()
        # function call
        tap_github.authed_get("test_source", "")

        # verify that we got expected timeout value
        self.assertEquals(300.0, timeout)
        # verify that the request was called with expected timeout value
        mocked_request.assert_called_with(method='get', url='', timeout=300.0)

@mock.patch("time.sleep")
@mock.patch("requests.Session.request")
@mock.patch("singer.utils.parse_args")
class TestTimeoutAndConnnectionErrorBackoff(unittest.TestCase):
    """
        Test case to verify that we backoff for 5 times for Connection and Timeout error
    """

    def test_timeout_backoff(self, mocked_parse_args, mocked_request, mocked_sleep):
        # mock request and raise 'Timeout' error
        mocked_request.side_effect = requests.Timeout

        mock_config = {}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config)

        try:
            # function call
            tap_github.authed_get("test_source", "")
        except requests.Timeout:
            pass

        # verify that we backoff 5 times
        self.assertEquals(5, mocked_request.call_count)

    def test_connection_error_backoff(self, mocked_parse_args, mocked_request, mocked_sleep):
        # mock request and raise 'Connection' error
        mocked_request.side_effect = requests.ConnectionError

        mock_config = {}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config)

        try:
            # function call
            tap_github.authed_get("test_source", "")
        except requests.ConnectionError:
            pass

        # verify that we backoff 5 times
        self.assertEquals(5, mocked_request.call_count)

    def test_Server5xx_error_backoff(self, mocked_parse_args, mocked_request, mocked_sleep):
        """Verify the tap retries for 5 times for Server5xx error"""
        # mock request and raise 'Server5xx' error
        mocked_request.side_effect = tap_github.Server5xxError

        mock_config = {}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config)

        try:
            # function call
            tap_github.authed_get("test_source", "")
        except tap_github.Server5xxError:
            pass

        # verify that we backoff 5 times
        self.assertEquals(5, mocked_request.call_count)
