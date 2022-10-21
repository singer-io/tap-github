import tap_github
from tap_github.client import rate_throttling, GithubException
import unittest
from unittest import mock
from math import ceil
import time
import requests

DEFAULT_SLEEP_SECONDS = 600
DEFAULT_MIN_RATE_LIMIT = 0
def api_call():
    return requests.get("https://api.github.com/rate_limit")

@mock.patch('time.sleep')
class TestRateLimit(unittest.TestCase):
    """
    Test `rate_throttling` function from client.
    """

    config = {"access_token": "", "repository": "singer-io/tap-github"}

    def test_rate_limt_wait(self, mocked_sleep):
        """
        Test `rate_throttling` for 'sleep_time' less than `MAX_SLEEP_SECONDS`
        """

        mocked_sleep.side_effect = None

        resp = api_call()
        resp.headers["X-RateLimit-Reset"] = int(ceil(time.time())) + 120
        resp.headers["X-RateLimit-Remaining"] = 0

        rate_throttling(resp, DEFAULT_SLEEP_SECONDS, DEFAULT_MIN_RATE_LIMIT)

        # Verify `time.sleep` is called with expected seconds in response
        mocked_sleep.assert_called_with(121)
        self.assertTrue(mocked_sleep.called)


    def test_rate_limit_exception(self, mocked_sleep):
        """
        Test `rate_throttling` for 'sleep_time' greater than `MAX_SLEEP_SECONDS`
        """

        mocked_sleep.side_effect = None

        resp = api_call()
        resp.headers["X-RateLimit-Reset"] = int(ceil(time.time())) + 601
        resp.headers["X-RateLimit-Remaining"] = 0

        # Verify exception is raised with proper message
        with self.assertRaises(tap_github.client.RateLimitExceeded) as e:
            rate_throttling(resp, DEFAULT_SLEEP_SECONDS, DEFAULT_MIN_RATE_LIMIT)
        self.assertEqual(str(e.exception), "API rate limit exceeded, please try after 602 seconds.")


    def test_rate_limit_not_exceeded(self, mocked_sleep):
        """
        Test `rate_throttling` if sleep time does not exceed limit
        """

        mocked_sleep.side_effect = None

        resp = api_call()
        resp.headers["X-RateLimit-Reset"] = int(ceil(time.time())) + 10
        resp.headers["X-RateLimit-Remaining"] = 5

        rate_throttling(resp, DEFAULT_SLEEP_SECONDS, DEFAULT_MIN_RATE_LIMIT)

        # Verify that `time.sleep` is not called
        self.assertFalse(mocked_sleep.called)

    def test_rate_limit_wait_with_min_rate_limit_defined(self, mocked_sleep):
        """
        Test `rate_throttling` if sleep time does exceed limit with a different value for the `min_rate_limit`
        """

        mocked_sleep.side_effect = None

        resp = api_call()
        resp.headers["X-RateLimit-Reset"] = int(ceil(time.time())) + 10
        resp.headers["X-RateLimit-Remaining"] = 5

        rate_throttling(resp, DEFAULT_SLEEP_SECONDS, min_rate_limit=5)

        # Verify `time.sleep` is called
        self.assertTrue(mocked_sleep.called)

    def test_rate_limt_header_not_found(self, mocked_sleep):
        """
        Test that the `rate_throttling` function raises an exception if `X-RateLimit-Reset` key is not found in the header.
        """
        resp = api_call()
        resp.headers={}
        
        with self.assertRaises(GithubException) as e:
            rate_throttling(resp, DEFAULT_SLEEP_SECONDS, DEFAULT_MIN_RATE_LIMIT)
        
        # Verifying the message formed for the invalid base URL
        self.assertEqual(str(e.exception), "The API call using the specified base url was unsuccessful. Please double-check the provided base URL.")
