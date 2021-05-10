from unittest import mock
import tap_github
import unittest

class Mockresponse:
    def __init__(self, status_code, text=None):
        self.status_code = status_code
        self.text = ""

def get_response(status_code):
    return Mockresponse(status_code=status_code)

@mock.patch("tap_github.logger.error")
@mock.patch("requests.Session.request")
class TestCredentials(unittest.TestCase):

    def test_repo_invalid_creds(self, mocked_request, mocked_logger_error):
        mocked_request.return_value = get_response(404)

        with self.assertRaises(tap_github.NotFoundException):
            tap_github.verify_repo_access("", "repo")
        mocked_logger_error.assert_called_with("Access Token does not have permission to access private repositories or this account is not collaborator of '%s' this repository", "repo")

    def test_org_invalid_creds_1(self, mocked_request, mocked_logger_error):
        mocked_request.return_value = get_response(404)

        with self.assertRaises(tap_github.NotFoundException):
            tap_github.verify_org_access("", "org")
        mocked_logger_error.assert_called_with("'%s' is not an oragnization", "org")

    def test_org_invalid_creds_2(self, mocked_request, mocked_logger_error):
        mocked_request.return_value = get_response(403)

        with self.assertRaises(tap_github.AuthException):
            tap_github.verify_org_access("", "org")
        mocked_logger_error.assert_called_with("Not a member or access token has not permission to read orgs data for this '%s' organization", "org")

    @mock.patch("tap_github.get_catalog")
    def test_discover_valid_creds(self, mocked_get_catalog, mocked_request, mocked_logger_error):
        mocked_request.return_value = get_response(200)
        mocked_get_catalog.return_value = {}

        tap_github.do_discover({"access_token": "access_token", "repository": "org/repo"})

        self.assertTrue(mocked_get_catalog.call_count, 1)

    @mock.patch("tap_github.get_catalog")
    def test_discover_invalid_creds_1(self, mocked_get_catalog, mocked_request, mocked_logger_error):
        mocked_request.return_value = get_response(404)
        mocked_get_catalog.return_value = {}

        with self.assertRaises(tap_github.NotFoundException):
            tap_github.do_discover({"access_token": "access_token", "repository": "org/repo"})
        self.assertEqual(mocked_get_catalog.call_count, 0)

    @mock.patch("tap_github.get_catalog")
    def test_discover_invalid_creds_2(self, mocked_get_catalog, mocked_request, mocked_logger_error):
        mocked_request.return_value = get_response(403)
        mocked_get_catalog.return_value = {}

        with self.assertRaises(tap_github.AuthException):
            tap_github.do_discover({"access_token": "access_token", "repository": "org/repo"})
        self.assertEqual(mocked_get_catalog.call_count, 0)


@mock.patch("tap_github.logger.info")
@mock.patch("tap_github.verify_repo_access")
@mock.patch("tap_github.verify_org_access")
class TestRepoCallCount(unittest.TestCase):
    """
        Here 3 repos are given,
        so tap will check creds for 3 repos
    """
    def test_repo_call_count(self, mocked_org, mocked_repo, mocked_logger_info):
        mocked_org.return_value = None
        mocked_repo.return_value = None

        config = {"access_token": "access_token", "repository": "org1/repo1 org1/repo2 org2/repo1"}
        tap_github.verify_access_for_repo_org(config)

        self.assertEquals(mocked_logger_info.call_count, 3)
        self.assertEquals(mocked_org.call_count, 3)
        self.assertEquals(mocked_repo.call_count, 3)
