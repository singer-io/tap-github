import time
import requests
import backoff
from simplejson import JSONDecodeError
import singer
from singer import metrics

LOGGER = singer.get_logger()
DEFAULT_SLEEP_SECONDS = 600

# Set default timeout of 300 seconds
REQUEST_TIMEOUT = 300

class GithubException(Exception):
    pass

class BadCredentialsException(GithubException):
    pass

class AuthException(GithubException):
    pass

class NotFoundException(GithubException):
    pass

class BadRequestException(GithubException):
    pass

class InternalServerError(GithubException):
    pass

class UnprocessableError(GithubException):
    pass

class NotModifiedError(GithubException):
    pass

class MovedPermanentlyError(GithubException):
    pass

class ConflictError(GithubException):
    pass

class RateLimitExceeded(GithubException):
    pass


ERROR_CODE_EXCEPTION_MAPPING = {
    301: {
        "raise_exception": MovedPermanentlyError,
        "message": "The resource you are looking for is moved to another URL."
    },
    304: {
        "raise_exception": NotModifiedError,
        "message": "The requested resource has not been modified since the last time you accessed it."
    },
    400:{
        "raise_exception": BadRequestException,
        "message": "The request is missing or has a bad parameter."
    },
    401: {
        "raise_exception": BadCredentialsException,
        "message": "Invalid authorization credentials."
    },
    403: {
        "raise_exception": AuthException,
        "message": "User doesn't have permission to access the resource."
    },
    404: {
        "raise_exception": NotFoundException,
        "message": "The resource you have specified cannot be found. Alternatively the access_token is not valid for the resource"
    },
    409: {
        "raise_exception": ConflictError,
        "message": "The request could not be completed due to a conflict with the current state of the server."
    },
    422: {
        "raise_exception": UnprocessableError,
        "message": "The request was not able to process right now."
    },
    500: {
        "raise_exception": InternalServerError,
        "message": "An error has occurred at Github's end."
    }
}

def raise_for_error(resp, source, should_skip_404):
    """
    Retrieve the error code and the error message from the response and return custom exceptions accordingly.
    """
    error_code = resp.status_code
    try:
        response_json = resp.json()
    except JSONDecodeError:
        response_json = {}

    if error_code == 404 and should_skip_404:
        details = ERROR_CODE_EXCEPTION_MAPPING.get(error_code).get("message")
        if source == "teams":
            details += ' or it is a personal account repository'
        message = "HTTP-error-code: 404, Error: {}. Please refer \'{}\' for more details.".format(details, response_json.get("documentation_url"))
        LOGGER.warning(message)
        # Don't raise a NotFoundException
        return None

    message = "HTTP-error-code: {}, Error: {}".format(
        error_code, ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get("message", "Unknown Error") if response_json == {} else response_json)

    exc = ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get("raise_exception", GithubException)
    raise exc(message) from None

def calculate_seconds(epoch):
    """
    Calculate the seconds to sleep before making a new request.
    """
    current = time.time()
    return int(round((epoch - current), 0))

def rate_throttling(response, max_sleep_seconds):
    """
    For rate limit errors, get the remaining time before retrying and calculate the time to sleep before making a new request.
    """
    if int(response.headers['X-RateLimit-Remaining']) == 0:
        seconds_to_sleep = calculate_seconds(int(response.headers['X-RateLimit-Reset']))

        if seconds_to_sleep > max_sleep_seconds:
            message = "API rate limit exceeded, please try after {} seconds.".format(seconds_to_sleep)
            raise RateLimitExceeded(message) from None

        LOGGER.info("API rate limit exceeded. Tap will retry the data collection after %s seconds.", seconds_to_sleep)
        time.sleep(seconds_to_sleep)

class GithubClient:
    """
    The client class used for making REST calls to the Github API.
    """
    def __init__(self, config):
        self.config = config
        self.session = requests.Session()
        self.base_url = "https://api.github.com"
        self.max_sleep_seconds = self.config.get('max_sleep_seconds', DEFAULT_SLEEP_SECONDS)

    # Return the 'timeout'
    def get_request_timeout(self):
        """
        Get the request timeout from the config, if not present use the default 300 seconds.
        """
        # Get the value of request timeout from config
        config_request_timeout = self.config.get('request_timeout')

        # Only return the timeout value if it is passed in the config and the value is not 0, "0" or ""
        if config_request_timeout and float(config_request_timeout):
            # Return the timeout from config
            return float(config_request_timeout)

        # Return default timeout
        return REQUEST_TIMEOUT

    # pylint: disable=dangerous-default-value
    # During 'Timeout' error there is also possibility of 'ConnectionError',
    # hence added backoff for 'ConnectionError' too.
    @backoff.on_exception(backoff.expo, (requests.Timeout, requests.ConnectionError), max_tries=5, factor=2)
    def authed_get(self, source, url, headers={}, should_skip_404 = True):
        """
        Call rest API and return the response in case of status code 200.
        """
        with metrics.http_request_timer(source) as timer:
            self.session.headers.update(headers)
            resp = self.session.request(method='get', url=url, timeout=self.get_request_timeout())
            if resp.status_code != 200:
                raise_for_error(resp, source, should_skip_404)
            timer.tags[metrics.Tag.http_status_code] = resp.status_code
            rate_throttling(resp, self.max_sleep_seconds)
            if resp.status_code == 404:
                # Return an empty response body since we're not raising a NotFoundException
                resp._content = b'{}' # pylint: disable=protected-access
            return resp

    def authed_get_all_pages(self, source, url, headers={}):
        """
        Fetch all pages of records and return them.
        """
        while True:
            r = self.authed_get(source, url, headers)
            yield r

            # Fetch the next page if next found in the response.
            if 'next' in r.links:
                url = r.links['next']['url']
            else:
            # Break the loop if all pages are fetched.
                break

    def verify_repo_access(self, url_for_repo, repo):
        """
        Call rest API to verify that the user has sufficient permissions to access this repository.
        """
        try:
            self.authed_get("verifying repository access", url_for_repo, should_skip_404 = False)
        except NotFoundException:
            # Throwing user-friendly error message as it checks token access
            message = "HTTP-error-code: 404, Error: Please check the repository name \'{}\' or you do not have sufficient permissions to access this repository.".format(repo)
            raise NotFoundException(message) from None

    def verify_access_for_repo(self):
        """
        For all the repositories mentioned in the config, check the access for each repos.
        """
        access_token = self.config['access_token']
        self.session.headers.update({'authorization': 'token ' + access_token, 'per_page': '1', 'page': '1'})

        repositories = self.extract_repos_from_config()

        for repo in repositories:

            url_for_repo = "{}/repos/{}/commits".format(self.base_url, repo)
            LOGGER.info("Verifying access of repository: %s", repo)

            # Verifying for Repo access
            self.verify_repo_access(url_for_repo, repo)

    def extract_repos_from_config(self):
        """
        Extracts all repositories from the config and calls get_all_repos()
        for organizations using the wildcard 'org/*' format.
        """
        repo_paths = list(filter(None, self.config['repository'].split(' ')))

        orgs_with_all_repos = list(filter(lambda x: x.split('/')[1] == '*', repo_paths))

        if orgs_with_all_repos:
            # Remove any wildcard "org/*" occurrences from `repo_paths`
            repo_paths = list(set(repo_paths).difference(set(orgs_with_all_repos)))

            # Get all repositories for an org in the config
            all_repos = self.get_all_repos(orgs_with_all_repos)

            # Update repo_paths
            repo_paths.extend(all_repos)

        return repo_paths

    def get_all_repos(self, organizations: list):
        """
        Retrieves all repositories for the provided organizations and
        verifies basic access for them.

        Docs: https://docs.github.com/en/rest/reference/repos#list-organization-repositories
        """
        repos = []

        for org_path in organizations:
            org = org_path.split('/')[0]
            for response in self.authed_get_all_pages(
                'get_all_repos',
                '{}/orgs/{}/repos?sort=created&direction=desc'.format(self.base_url, org)
            ):
                org_repos = response.json()

                for repo in org_repos:
                    repo_full_name = repo.get('full_name')

                    self.verify_repo_access(
                        'https://api.github.com/repos/{}/commits'.format(repo_full_name),
                        repo
                    )

                    repos.append(repo_full_name)

        return repos
