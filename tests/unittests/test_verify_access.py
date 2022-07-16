from unittest import mock
import tap_github
from tap_github.client import GithubClient
import unittest
import requests

class Mockresponse:
    """ Mock response object class."""

    def __init__(self, status_code, json, raise_error, headers={'X-RateLimit-Remaining': 1}, text=None):
        self.status_code = status_code
        self.raise_error = raise_error
        self.text = json
        self.headers = headers
        self.content = "github"

    def raise_for_status(self):
        if not self.raise_error:
            return self.status_code

        raise requests.HTTPError("Sample message")

    def json(self):
        """ Response JSON method."""
        return self.text

def get_response(status_code, json={}, raise_error=False):
    """ Returns required mock response. """
    return Mockresponse(status_code, json, raise_error)

@mock.patch("tap_github.client.GithubClient.verify_access_for_repo", return_value = None)
@mock.patch("requests.Session.request")
@mock.patch("singer.utils.parse_args")
class TestCredentials(unittest.TestCase):
    """
    Test `verify_repo_access` error handling
    """

    config = {"access_token": "", "repository": "singer-io/tap-github"}

    @mock.patch("tap_github.client.LOGGER.warning")
    def test_repo_not_found(self, mock_logger, mocked_parse_args, mocked_request, mock_verify_access):
        """Verify if 404 error arises while checking access of repo"""
        test_client = GithubClient(self.config)
        json = {"message": "Not Found", "documentation_url": "https:/docs.github.com/"}
        mocked_request.return_value = get_response(404, json, True)

        with self.assertRaises(tap_github.client.NotFoundException) as e:
            test_client.verify_repo_access("", "repo")

        # Verify logger called with proper message
        self.assertEqual(str(e.exception), "HTTP-error-code: 404, Error: Please check the repository name 'repo' or you do not have sufficient permissions to access this repository.")

    def test_repo_bad_request(self, mocked_parse_args, mocked_request, mock_verify_access):
        """Verify if 400 error arises"""
        test_client = GithubClient(self.config)
        mocked_request.return_value = get_response(400, raise_error = True)

        with self.assertRaises(tap_github.client.BadRequestException) as e:
            test_client.verify_repo_access("", "repo")

        # Verify error with proper message
        self.assertEqual(str(e.exception), "HTTP-error-code: 400, Error: The request is missing or has a bad parameter.")

    def test_repo_bad_creds(self, mocked_parse_args, mocked_request, mock_verify_access):
        """Verify if 401 error arises"""
        test_client = GithubClient(self.config)
        json = {"message": "Bad credentials", "documentation_url": "https://docs.github.com/"}
        mocked_request.return_value = get_response(401, json, True)

        with self.assertRaises(tap_github.client.BadCredentialsException) as e:
            test_client.verify_repo_access("", "repo")

        # Verify error with proper message
        self.assertEqual(str(e.exception), "HTTP-error-code: 401, Error: {}".format(json))


@mock.patch("tap_github.client.GithubClient.verify_repo_access")
class TestVerifyAccessForRepo(unittest.TestCase):
    """
    Test `verify_access_for_repo` handling
    """

    def test_for_one_repo(self, mock_verify_access):
        """
        Test method when one repo is given in the config
        """
        config = {"access_token": "", "repository": "singer-io/tap-github"}

        test_client = GithubClient(config)

        # Verify `verify_repo_access` called with expected args
        self.assertEqual(mock_verify_access.call_count, 1)
        mock_verify_access.assert_called_with("https://api.github.com/repos/singer-io/tap-github/commits", "singer-io/tap-github")

    def test_for_multiple_repos(self, mock_verify_access):
        """
        Test method if multiple repos are given in config
        """
        config = {"access_token": "", "repository": "singer-io/tap-github singer-io/test-repo singer-io/tap-jira"}

        test_client = GithubClient(config)

        expected_calls = [mock.call("https://api.github.com/repos/singer-io/tap-github/commits","singer-io/tap-github"),
                          mock.call("https://api.github.com/repos/singer-io/test-repo/commits", "singer-io/test-repo"),
                          mock.call("https://api.github.com/repos/singer-io/tap-jira/commits", "singer-io/tap-jira")]

        # Verify `verify_repo_access` called with expected args
        self.assertEqual(mock_verify_access.call_count, 3)

        self.assertIn(mock_verify_access.mock_calls[0], expected_calls)
        self.assertIn(mock_verify_access.mock_calls[1], expected_calls)
        self.assertIn(mock_verify_access.mock_calls[2], expected_calls)
