import tap_github
from tap_github.client import rate_throttling
import unittest
from unittest import mock
import time
import requests

DEFAULT_SLEEP_SECONDS = 600
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
        resp.headers["X-RateLimit-Reset"] = int(round(time.time(), 0)) + 120
        resp.headers["X-RateLimit-Remaining"] = 0

        rate_throttling(resp, DEFAULT_SLEEP_SECONDS)

        # Verify `time.sleep` is called with expected seconds in response
        mocked_sleep.assert_called_with(120)
        self.assertTrue(mocked_sleep.called)


    def test_rate_limit_exception(self, mocked_sleep):
        """
        Test `rate_throttling` for 'sleep_time' greater than `MAX_SLEEP_SECONDS`
        """

        mocked_sleep.side_effect = None

        resp = api_call()
        resp.headers["X-RateLimit-Reset"] = int(round(time.time(), 0)) + 601
        resp.headers["X-RateLimit-Remaining"] = 0

        # Verify exception is raised with proper message
        with self.assertRaises(tap_github.client.RateLimitExceeded) as e:
            rate_throttling(resp, DEFAULT_SLEEP_SECONDS)
        self.assertEqual(str(e.exception), "API rate limit exceeded, please try after 601 seconds.")


    def test_rate_limit_not_exceeded(self, mocked_sleep):
        """
        Test `rate_throttling` if sleep time does not exceed limit
        """

        mocked_sleep.side_effect = None

        resp = api_call()
        resp.headers["X-RateLimit-Reset"] = int(round(time.time(), 0)) + 10
        resp.headers["X-RateLimit-Remaining"] = 5

        rate_throttling(resp, DEFAULT_SLEEP_SECONDS)

        # Verify that `time.sleep` is not called
        self.assertFalse(mocked_sleep.called)
