import tap_github.__init__ as tap_github
import unittest
from unittest import mock
import time
import requests
import importlib

def api_call():
    return requests.get("https://api.github.com/rate_limit")

@mock.patch('time.sleep')
class TestRateLimit(unittest.TestCase):

    def setUp(self) -> None:
        importlib.reload(tap_github)


    def test_rate_limit_wait_with_default_max_rate_limit(self, mocked_sleep):

        mocked_sleep.side_effect = None

        resp = api_call()
        resp.headers["X-RateLimit-Reset"] = str(int(round(time.time(), 0)) + 120)
        resp.headers["X-RateLimit-Remaining"] = "0"

        tap_github.rate_throttling(resp)

        mocked_sleep.assert_called_with(120)
        self.assertTrue(mocked_sleep.called)


    def test_rate_limit_exception_when_exceed_default_max_rate_limit(self, mocked_sleep):

        mocked_sleep.side_effect = None

        resp = api_call()
        resp.headers["X-RateLimit-Reset"] = int(round(time.time(), 0)) + 601
        resp.headers["X-RateLimit-Remaining"] = 0

        try:
            tap_github.rate_throttling(resp)
        except tap_github.RateLimitExceeded as e:
            self.assertEqual(str(e), "API rate limit exceeded, please try after 601 seconds.")


    def test_rate_limit_not_exceed_default_max_rate_limit(self, mocked_sleep):

        mocked_sleep.side_effect = None

        resp = api_call()
        resp.headers["X-RateLimit-Reset"] = int(round(time.time(), 0)) + 10
        resp.headers["X-RateLimit-Remaining"] = 5

        tap_github.rate_throttling(resp)

        self.assertFalse(mocked_sleep.called)

    def test_rate_limit_config_override_throw_exception(self, mocked_sleep):
        tap_github.MAX_RATE_LIMIT_WAIT_SECONDS = 1

        resp = api_call()
        resp.headers["X-RateLimit-Reset"] = str(int(round(time.time(), 0)) + 10)
        resp.headers["X-RateLimit-Remaining"] = "0"

        with self.assertRaises(tap_github.RateLimitExceeded):
            self.assertEqual(0, mocked_sleep.call_count)
            tap_github.rate_throttling(resp)
