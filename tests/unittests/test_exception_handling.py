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
class TestExceptionHandling(unittest.TestCase):
    def test_400_error(self, mocked_request):
        mocked_request.return_value = get_response(400, raise_error = True)
        
        try:
            tap_github.authed_get("", "")
        except tap_github.BadRequestExecption as e:
            self.assertEquals(str(e), "HTTP-error-code: 400, Error: A validation exception has occurred.")
    
    def test_401_error(self, mocked_request):
        mocked_request.return_value = get_response(401, raise_error = True)
        
        try:
            tap_github.authed_get("", "")
        except tap_github.BadCredentialsException as e:
            self.assertEquals(str(e), "HTTP-error-code: 401, Error: Invalid authorization credentials.")
    
    def test_403_error(self, mocked_request):
        mocked_request.return_value = get_response(403, raise_error = True)
        
        try:
            tap_github.authed_get("", "")
        except tap_github.AuthException as e:
            self.assertEquals(str(e), "HTTP-error-code: 403, Error: User doesn't have permission to access the resource.")
    
    def test_404_error(self, mocked_request):
        mocked_request.return_value = get_response(404, raise_error = True)

        try:
            tap_github.authed_get("", "")
        except tap_github.NotFoundException as e:
            self.assertEquals(str(e), "HTTP-error-code: 404, Error: The resource you have specified cannot be found.")

    def test_500_error(self, mocked_request):
        mocked_request.return_value = get_response(500, raise_error = True)

        try:
            tap_github.authed_get("", "")
        except tap_github.InternalServerError as e:
            self.assertEquals(str(e), "HTTP-error-code: 500, Error: An error has occurred at Github's end.")

    def test_200_error(self, mocked_request):
        json = {"key": "value"}
        mocked_request.return_value = get_response(200, json)

        resp = tap_github.authed_get("", "")
        self.assertEquals(json, resp.json())
