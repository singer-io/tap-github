import unittest
from unittest import mock
import singer
import json
import tap_github.__init__ as tap_github
logger = singer.get_logger()

class Mockresponse:
    def __init__(self, resp):
        self.json_data = resp

    def json(self):
        return [(self.json_data)]

def get_response(json):
    yield Mockresponse(resp=json)

@mock.patch("tap_github.__init__.authed_get_all_pages")
@mock.patch("tap_github.logger.error")
class TestRateLimit(unittest.TestCase):

    @mock.patch("tap_github.__init__.get_all_team_members")
    def test_slug_and_sub_stream_in_resp(self, mocked_team_members, mocked_logger_error, mocked_request):
        """
            "slug" is present in responnse and "team_members" is selected in schema,
            so function will perform smoothly and get data for "team_members"
        """
        schemas = {"team_members": "None"}
        json = {"key": "value", "slug": "my-team"}
        mocked_request.return_value = get_response(json)

        tap_github.get_all_teams(schemas, "tap-github", {}, {})

        self.assertEquals(mocked_logger_error.call_count, 0)
        self.assertEquals(mocked_team_members.call_count, 1)


    def test_not_slug_and_sub_stream_in_resp(self, mocked_logger_error, mocked_request):
        """
            "slug" is not given in response,
            error will be generated
        """
        schemas = {"team_members": "None"}
        json = {"key": "value"}
        mocked_request.return_value = get_response(json)

        tap_github.get_all_teams(schemas, "tap-github", {}, {})

        self.assertEquals(mocked_logger_error.call_count, 1)
        mocked_logger_error.assert_called_with('Could not find slug in org: %s', 'tap-github')
    
    def test_slug_and_not_sub_stream_in_resp(self, mocked_logger_error, mocked_request):
        """
            "slug" is present in responnse and "team_members" is not selected in schema,
            so function will perform smoothly and not get the data for "team_members"
        """

        json = {"key": "value", "slug": "my-team"}
        mocked_request.return_value = get_response(json)

        tap_github.get_all_teams({}, "tap-github", {}, {})

        self.assertEquals(mocked_logger_error.call_count, 0)

    def test_not_slug_and_not_sub_stream_in_resp(self, mocked_logger_error, mocked_request):
        """
            "slug" is not given in response,
            error will be generated
        """

        json = {"key": "value"}
        mocked_request.return_value = get_response(json)

        tap_github.get_all_teams({}, "tap-github", {}, {})

        self.assertEquals(mocked_logger_error.call_count, 0)
