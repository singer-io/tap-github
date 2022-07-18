import unittest
from unittest import mock
import tap_github
from tap_github.client import GithubClient, REQUEST_TIMEOUT
import requests

class Mockresponse:
    """ Mock response object class."""

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
        """ Response JSON method."""
        return self.text

class MockParseArgs:
    """Mock args object class"""
    config = {}
    def __init__(self, config):
        self.config = config

def get_args(config):
    """ Returns required args response. """
    return MockParseArgs(config)

def get_response(status_code, json={}, raise_error=False, content=None):
    """ Returns required mock response. """
    return Mockresponse(status_code, json, raise_error, content=content)

@mock.patch("tap_github.client.GithubClient.verify_access_for_repo", return_value = None)
@mock.patch("time.sleep")
@mock.patch("requests.Session.request")
@mock.patch("singer.utils.parse_args")
class TestTimeoutValue(unittest.TestCase):
    """
        Test case to verify the timeout value is set as expected
    """

    def test_timeout_value_in_config(self, mocked_parse_args, mocked_request, mocked_sleep, mock_verify_access):
        """
        Test if timeout value given in config
        """
        json = {"key": "value"}
        # mock response
        mocked_request.return_value = get_response(200, json)

        mock_config = {"request_timeout": 100, "access_token": "access_token"}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config)
        test_client = GithubClient(mock_config)

        # get the timeout value for assertion
        timeout = test_client.get_request_timeout()
        # function call
        test_client.authed_get("test_source", "")

        # verify that we got expected timeout value
        self.assertEqual(100.0, timeout)
        # verify that the request was called with expected timeout value
        mocked_request.assert_called_with(method='get', url='', timeout=100.0)

    def test_timeout_value_not_in_config(self, mocked_parse_args, mocked_request, mocked_sleep, mock_verify_access):
        """
        Test if timeout value not given in config
        """
        json = {"key": "value"}
        # mock response
        mocked_request.return_value = get_response(200, json)

        mock_config = {"access_token": "access_token"}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config)
        test_client = GithubClient(mock_config)

        # get the timeout value for assertion
        timeout = test_client.get_request_timeout()
        # function call
        test_client.authed_get("test_source", "")

        # verify that we got expected timeout value
        self.assertEqual(300.0, timeout)
        # verify that the request was called with expected timeout value
        mocked_request.assert_called_with(method='get', url='', timeout=REQUEST_TIMEOUT)

    def test_timeout_string_value_in_config(self, mocked_parse_args, mocked_request, mocked_sleep, mock_verify_access):
        """
        Test if timeout value given as string in config
        """
        json = {"key": "value"}
        # mock response
        mocked_request.return_value = get_response(200, json)

        mock_config = {"request_timeout": "100", "access_token": "access_token"}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config)
        test_client = GithubClient(mock_config)

        # get the timeout value for assertion
        timeout = test_client.get_request_timeout()
        # function call
        test_client.authed_get("test_source", "")

        # verify that we got expected timeout value
        self.assertEqual(100.0, timeout)
        # verify that the request was called with expected timeout value
        mocked_request.assert_called_with(method='get', url='', timeout=100.0)

    def test_timeout_empty_value_in_config(self, mocked_parse_args, mocked_request, mocked_sleep, mock_verify_access):
        """
        Test if timeout value given as empty string in config
        """
        json = {"key": "value"}
        # mock response
        mocked_request.return_value = get_response(200, json)

        mock_config = {"request_timeout": "", "access_token": "access_token"}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config)
        test_client = GithubClient(mock_config)

        # get the timeout value for assertion
        timeout = test_client.get_request_timeout()
        # function call
        test_client.authed_get("test_source", "")

        # verify that we got expected timeout value
        self.assertEqual(300.0, timeout)
        # verify that the request was called with expected timeout value
        mocked_request.assert_called_with(method='get', url='', timeout=300.0)

    def test_timeout_0_value_in_config(self, mocked_parse_args, mocked_request, mocked_sleep, mock_verify_access):
        """
        Test if timeout value given as `0` in config
        """
        json = {"key": "value"}
        # mock response
        mocked_request.return_value = get_response(200, json)

        mock_config = {"request_timeout": 0.0, "access_token": "access_token"}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config)
        test_client = GithubClient(mock_config)

        # get the timeout value for assertion
        timeout = test_client.get_request_timeout()
        # function call
        test_client.authed_get("test_source", "")

        # verify that we got expected timeout value
        self.assertEqual(300.0, timeout)
        # verify that the request was called with expected timeout value
        mocked_request.assert_called_with(method='get', url='', timeout=300.0)

    def test_timeout_string_0_value_in_config(self, mocked_parse_args, mocked_request, mocked_sleep, mock_verify_access):
        """
        Test if timeout value given as `0` string in config
        """
        json = {"key": "value"}
        # mock response
        mocked_request.return_value = get_response(200, json)

        mock_config = {"request_timeout": "0.0", "access_token": "access_token"}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config)
        test_client = GithubClient(mock_config)

        # get the timeout value for assertion
        timeout = test_client.get_request_timeout()
        # function call
        test_client.authed_get("test_source", "")

        # verify that we got expected timeout value
        self.assertEqual(300.0, timeout)
        # verify that the request was called with expected timeout value
        mocked_request.assert_called_with(method='get', url='', timeout=300.0)

@mock.patch("tap_github.client.GithubClient.verify_access_for_repo", return_value = None)
@mock.patch("time.sleep")
@mock.patch("requests.Session.request")
@mock.patch("singer.utils.parse_args")
class TestTimeoutAndConnnectionErrorBackoff(unittest.TestCase):
    """
        Test case to verify that we backoff for 5 times for Connection and Timeout error
    """

    def test_timeout_backoff(self, mocked_parse_args, mocked_request, mocked_sleep, mock_verify_access):
        """
        Test if `timeout error` raises
        """
        # mock request and raise 'Timeout' error
        mocked_request.side_effect = requests.Timeout

        mock_config = {"access_token": "access_token"}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config)
        test_client = GithubClient(mock_config)

        with self.assertRaises(requests.Timeout):
            test_client.authed_get("test_source", "")

        # verify that we backoff 5 times
        self.assertEqual(5, mocked_request.call_count)

    def test_connection_error_backoff(self, mocked_parse_args, mocked_request, mocked_sleep, mock_verify_access):
        """
        Test if `connection error` error raises
        """
        # mock request and raise 'Connection' error
        mocked_request.side_effect = requests.ConnectionError

        mock_config = {"access_token": "access_token"}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config)
        test_client = GithubClient(mock_config)

        with self.assertRaises(requests.ConnectionError):
            test_client.authed_get("test_source", "")

        # verify that we backoff 5 times
        self.assertEqual(5, mocked_request.call_count)

