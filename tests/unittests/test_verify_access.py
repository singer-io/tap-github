from unittest import mock
import tap_github
import unittest
import requests

class Mockresponse:
    def __init__(self, status_code, json, raise_error, text=None):
        self.status_code = status_code
        self.raise_error = raise_error
        self.text = json

    def raise_for_status(self):
        if not self.raise_error:
            return self.status_code

        raise requests.HTTPError("Sample message")

    def json(self):
        return self.text

def get_response(status_code, json={}, raise_error=False):
    return Mockresponse(status_code, json, raise_error)

@mock.patch("requests.Session.request")
class TestCredentials(unittest.TestCase):

    def test_repo_invalid_creds(self, mocked_request):
        json = {"message": "Not Found", "documentation_url": "https:/"}
        mocked_request.return_value = get_response(404, json, True)

        try:
            tap_github.verify_repo_access("", "repo")
        except tap_github.NotFoundException as e:
            self.assertEquals(str(e), "HTTP-error-code: 404, Error: Please check the repository name 'repo' or you do not have sufficient permissions to access this repository.")

    def test_repo_wrong_creds(self, mocked_request):
        json = {"message":"Bad credentials","documentation_url":"https://docs.github.com/rest"}
        mocked_request.return_value = get_response(401, json, True)

        try:
            tap_github.verify_repo_access("", "repo")
        except tap_github.BadCredentialsException as e:
            self.assertEquals(str(e), "HTTP-error-code: 401, Error: {}".format(json))

    def test_org_invalid_creds_1(self, mocked_request):
        json = {"message": "Not Found", "documentation_url": "https:/"}
        mocked_request.return_value = get_response(404, json, True)

        try:
            tap_github.verify_org_access("")
        except tap_github.NotFoundException as e:
            self.assertEquals(str(e), "HTTP-error-code: 404, Error: {}".format(json))

    def test_org_invalid_creds_2(self, mocked_request):
        json = {'message': 'Must have admin rights to Repository.', 'documentation_url': 'https://docs.github.com/rest/reference/'}
        mocked_request.return_value = get_response(403, json, True)

        try:
            tap_github.verify_org_access("")
        except tap_github.AuthException as e:
            self.assertEquals(str(e), "HTTP-error-code: 403, Error: {}".format(json))

    def test_org_wrong_creds(self, mocked_request):
        json = {"message":"Bad credentials","documentation_url":"https://docs.github.com/rest"}
        mocked_request.return_value = get_response(401, json, True)

        try:
            tap_github.verify_org_access("")
        except tap_github.BadCredentialsException as e:
            self.assertEquals(str(e), "HTTP-error-code: 401, Error: {}".format(json))

    @mock.patch("tap_github.get_catalog")
    def test_discover_valid_creds(self, mocked_get_catalog, mocked_request):
        mocked_request.return_value = get_response(200)
        mocked_get_catalog.return_value = {}

        tap_github.do_discover({"access_token": "access_token", "repository": "org/repo"})

        self.assertTrue(mocked_get_catalog.call_count, 1)

    @mock.patch("tap_github.get_catalog")
    def test_discover_invalid_creds_1(self, mocked_get_catalog, mocked_request):
        json = {"message": "Not Found", "documentation_url": "https:/"}
        mocked_request.return_value = get_response(404, json, True)
        mocked_get_catalog.return_value = {}

        try:
            tap_github.do_discover({"access_token": "access_token", "repository": "org/repo"})
        except tap_github.NotFoundException as e:
                self.assertEquals(str(e), "HTTP-error-code: 404, Error: Please check the repository name 'org/repo' or you do not have sufficient permissions to access this repository.")
        self.assertEqual(mocked_get_catalog.call_count, 0)

    @mock.patch("tap_github.get_catalog")
    def test_discover_invalid_creds_2(self, mocked_get_catalog, mocked_request):
        json = {"message":"Bad credentials","documentation_url":"https://docs.github.com/rest"}
        mocked_request.return_value = get_response(401, json, True)
        mocked_get_catalog.return_value = {}

        try:
            tap_github.do_discover({"access_token": "access_token", "repository": "org/repo"})
        except tap_github.BadCredentialsException as e:
                self.assertEquals(str(e), "HTTP-error-code: 401, Error: {}".format(json))
        self.assertEqual(mocked_get_catalog.call_count, 0)

    @mock.patch("tap_github.get_catalog")
    def test_discover_invalid_creds_3(self, mocked_get_catalog, mocked_request):
        json = {'message': 'Must have admin rights to Repository.', 'documentation_url': 'https://docs.github.com/rest/reference/'}
        mocked_request.return_value = get_response(403, json, True)
        mocked_get_catalog.return_value = {}

        try:
            tap_github.do_discover({"access_token": "access_token", "repository": "org/repo"})
        except tap_github.AuthException as e:
                self.assertEquals(str(e), "HTTP-error-code: 403, Error: {}".format(json))
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
