import unittest
from unittest import mock
from tap_github.client import GithubClient, BadCredentialsException

from tests.unittests.test_timeout import get_args, get_response


@mock.patch("tap_github.client.GithubClient.verify_access_for_repo", return_value = None)
@mock.patch("time.sleep")
@mock.patch("requests.Session.request")
@mock.patch("singer.utils.parse_args")
class TestBadCredentialsBackoff(unittest.TestCase):
    """
        Test case to verify that we backoff for 5 times for Bad Credentials error
    """

    def test_backoff(self, mocked_parse_args, mocked_request, mocked_sleep, mock_verify_access):
        """
        Test that tap retry timeout or connection error 5 times.
        """
        # mock request and raise error
        mocked_request.return_value = get_response(401, json={"message": "Bad credentials"})

        mock_config = {"access_token": "access_token"}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config)
        test_client = GithubClient(mock_config)

        with self.assertRaises(BadCredentialsException):
            test_client.authed_get("test_source", "")

        # verify that we backoff 5 times
        self.assertEqual(5, mocked_request.call_count)

