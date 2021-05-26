import tap_github.__init__ as tap_github
import unittest
from unittest import mock
import time
import requests

def api_call():
    return requests.get("https://api.github.com/rate_limit")

@mock.patch('time.sleep')
class TestRateLimit(unittest.TestCase):


    def test_rate_limt_wait(self, mocked_sleep):

        mocked_sleep.side_effect = None

        resp = api_call()
        resp.headers["X-RateLimit-Reset"] = int(round(time.time(), 0)) + 120
        resp.headers["X-RateLimit-Remaining"] = 0

        tap_github.rate_throttling(resp)

        mocked_sleep.assert_called_with(120)
        self.assertTrue(mocked_sleep.called)


    def test_rate_limit_exception(self, mocked_sleep):

        mocked_sleep.side_effect = None

        resp = api_call()
        resp.headers["X-RateLimit-Reset"] = int(round(time.time(), 0)) + 601
        resp.headers["X-RateLimit-Remaining"] = 0

        try:
            tap_github.rate_throttling(resp)
        except tap_github.RateLimitExceeded as e:
            self.assertEquals(str(e), "API rate limit exceeded, please try after 601 seconds.")


    def test_rate_limit_not_exceeded(self, mocked_sleep):

        mocked_sleep.side_effect = None

        resp = api_call()
        resp.headers["X-RateLimit-Reset"] = int(round(time.time(), 0)) + 10
        resp.headers["X-RateLimit-Remaining"] = 5

        tap_github.rate_throttling(resp)

        self.assertFalse(mocked_sleep.called)
