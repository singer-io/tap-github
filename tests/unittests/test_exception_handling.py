from unittest import mock
import tap_github
from tap_github.client import GithubClient, raise_for_error
import unittest
import requests

class Mockresponse:
    """ Mock response object class."""

    def __init__(self, status_code, json, raise_error, headers={'X-RateLimit-Remaining': 1}, content=None):
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

def get_mock_http_response(status_code, contents):
    """Return http mock response."""
    response = requests.Response()
    response.status_code = status_code
    response._content = contents.encode()
    return response

def get_response(status_code, json={}, raise_error=False, content=None):
    """ Returns required mock response. """
    return Mockresponse(status_code, json, raise_error, content=content)

@mock.patch("tap_github.client.GithubClient.verify_access_for_repo", return_value = None)
@mock.patch("requests.Session.request")
@mock.patch("singer.utils.parse_args")
class TestExceptionHandling(unittest.TestCase):
    
    """
    Test Error handling for `authed_get` method in client.
    """

    config = {"access_token": "", "repository": "org/test-repo"}

    def test_json_decoder_error(self, mocked_parse_args, mocked_request, mock_verify_access):
        """
        Verify handling of JSONDecoderError from the response.
        """

        mock_response = get_mock_http_response(409, "json_error")

        with self.assertRaises(tap_github.client.ConflictError) as e:
            raise_for_error(mock_response, "")

        # Verifying the message formed for the custom exception
        self.assertEqual(str(e.exception), "HTTP-error-code: 409, Error: The request could not be completed due to a conflict with the current state of the server.")

    def test_zero_content_length(self, mocked_parse_args, mocked_request, mock_verify_access):
        """
        Verify that `authed_get` raises 400 error with proper message for no content.
        """
        mocked_request.return_value = get_response(400, raise_error = True, content='')
        test_client = GithubClient(self.config)

        with self.assertRaises(tap_github.client.BadRequestException) as e:
            test_client.authed_get("", "")

        # Verifying the message formed for the custom exception
        self.assertEqual(str(e.exception), "HTTP-error-code: 400, Error: The request is missing or has a bad parameter.")

    def test_400_error(self, mocked_parse_args, mocked_request, mock_verify_access):
        """
        Verify that `authed_get` raises 400 error with proper message.
        """
        mocked_request.return_value = get_response(400, raise_error = True)
        test_client = GithubClient(self.config)

        with self.assertRaises(tap_github.client.BadRequestException) as e:
            test_client.authed_get("", "")

        # Verifying the message formed for the custom exception
        self.assertEqual(str(e.exception), "HTTP-error-code: 400, Error: The request is missing or has a bad parameter.")

    def test_401_error(self, mocked_parse_args, mocked_request, mock_verify_access):
        """
        Verify that `authed_get` raises 401 error with proper message.
        """
        mocked_request.return_value = get_response(401, raise_error = True)
        test_client = GithubClient(self.config)

        with self.assertRaises(tap_github.client.BadCredentialsException) as e:
            test_client.authed_get("", "")

        # Verifying the message formed for the custom exception
        self.assertEqual(str(e.exception), "HTTP-error-code: 401, Error: Invalid authorization credentials.")

    def test_403_error(self, mocked_parse_args, mocked_request, mock_verify_access):
        """
        Verify that `authed_get` raises 403 error with proper message.
        """
        mocked_request.return_value = get_response(403, raise_error = True)
        test_client = GithubClient(self.config)
        
        with self.assertRaises(tap_github.client.AuthException) as e:
            test_client.authed_get("", "")

        # Verifying the message formed for the custom exception
        self.assertEqual(str(e.exception), "HTTP-error-code: 403, Error: User doesn't have permission to access the resource.")

    @mock.patch("tap_github.client.LOGGER.warning")
    def test_404_error(self, mock_logger,  mocked_parse_args, mocked_request, mock_verify_access):
        """
        Verify that `authed_get` skip 404 error and print the log message with the proper message.
        """
        json = {"message": "Not Found", "documentation_url": "https:/docs.github.com/"}
        mocked_request.return_value = get_response(404, json = json, raise_error = True)
        expected_message = "HTTP-error-code: 404, Error: The resource you have specified cannot be found. Alternatively the access_token is not valid for the resource. Please refer '{}' for more details.".format(json.get("documentation_url"))
        test_client = GithubClient(self.config)

        test_client.authed_get("", "")

        # Verifying the message formed for the custom exception
        self.assertEqual(mock_logger.mock_calls[0], mock.call(expected_message))

    def test_500_error(self, mocked_parse_args, mocked_request, mock_verify_access):
        """
        Verify that `authed_get` raises 500 error with proper message.
        """
        mocked_request.return_value = get_response(500, raise_error = True)
        test_client = GithubClient(self.config)

        with self.assertRaises(tap_github.client.InternalServerError) as e:
            test_client.authed_get("", "")

        # Verifying the message formed for the custom exception
        self.assertEqual(str(e.exception), "HTTP-error-code: 500, Error: An error has occurred at Github's end.")

    def test_301_error(self, mocked_parse_args, mocked_request, mock_verify_access):
        """
        Verify that `authed_get` raises 301 error with proper message.
        """
        mocked_request.return_value = get_response(301, raise_error = True)
        test_client = GithubClient(self.config)

        with self.assertRaises(tap_github.client.MovedPermanentlyError) as e:
            test_client.authed_get("", "")

        # Verifying the message formed for the custom exception
        self.assertEqual(str(e.exception), "HTTP-error-code: 301, Error: The resource you are looking for is moved to another URL.")

    def test_304_error(self, mocked_parse_args, mocked_request, mock_verify_access):
        """
        Verify that `authed_get` raises 304 error with proper message.
        """
        mocked_request.return_value = get_response(304, raise_error = True)
        test_client = GithubClient(self.config)

        with self.assertRaises(tap_github.client.NotModifiedError) as e:
            test_client.authed_get("", "")

        # Verifying the message formed for the custom exception
        self.assertEqual(str(e.exception), "HTTP-error-code: 304, Error: The requested resource has not been modified since the last time you accessed it.")

    def test_422_error(self, mocked_parse_args, mocked_request, mock_verify_access):
        """
        Verify that `authed_get` raises 422 error with proper message.
        """
        mocked_request.return_value = get_response(422, raise_error = True)
        test_client = GithubClient(self.config)

        with self.assertRaises(tap_github.client.UnprocessableError) as e:
            test_client.authed_get("", "")

        # Verifying the message formed for the custom exception
        self.assertEqual(str(e.exception), "HTTP-error-code: 422, Error: The request was not able to process right now.")

    def test_409_error(self, mocked_parse_args, mocked_request, mock_verify_access):
        """
        Verify that `authed_get` raises 409 error with proper message.
        """
        mocked_request.return_value = get_response(409, raise_error = True)
        test_client = GithubClient(self.config)

        with self.assertRaises(tap_github.client.ConflictError) as e:
            test_client.authed_get("", "")

        # Verifying the message formed for the custom exception
        self.assertEqual(str(e.exception), "HTTP-error-code: 409, Error: The request could not be completed due to a conflict with the current state of the server.")

    def test_200_success(self, mocked_parse_args, mocked_request, mock_verify_access):
        """
        Verify that `authed_get` doen not raises error for success response.
        """
        json = {"key": "value"}
        mocked_request.return_value = get_response(200, json)
        test_client = GithubClient(self.config)

        resp = test_client.authed_get("", "")

        # Verifying success response
        self.assertEqual(json, resp.json())
