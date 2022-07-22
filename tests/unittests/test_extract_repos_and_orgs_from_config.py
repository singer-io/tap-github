import unittest
from unittest import mock
from tap_github.client import GithubClient


@mock.patch('tap_github.client.GithubClient.verify_access_for_repo')
@mock.patch('tap_github.client.GithubClient.get_all_repos')
class TestExtractReposFromConfig(unittest.TestCase):
    """
    Test `extract_repos_from_config` method from client.
    """

    def test_single_repo(self, mocked_get_all_repos, mock_verify_access):
        """
        Test `extract_repos_from_config` if only one repo path is given in config.
        """
        config = {'repository': 'singer-io/test-repo', "access_token": "TOKEN"}
        test_client = GithubClient(config)
        expected_repositories = ['singer-io/test-repo']

        # Verify list of repo path with expected
        self.assertEqual(expected_repositories, test_client.extract_repos_from_config())

    def test_multiple_repos(self, mocked_get_all_repos, mock_verify_access):
        """
        Test `extract_repos_from_config` if multiple repo paths are given in config.
        """
        config = {'repository': 'singer-io/test-repo singer-io/tap-github', "access_token": "TOKEN"}
        test_client = GithubClient(config)
        expected_repositories = ['singer-io/test-repo', 'singer-io/tap-github']

        # Verify list of repo path with expected
        self.assertEqual(expected_repositories, test_client.extract_repos_from_config())

    def test_org_all_repos(self, mocked_get_all_repos, mock_verify_access):
        """
        Test `extract_repos_from_config` for taking all the repositories of organisation given in config.
        """
        config = {'repository': 'singer-io/test-repo test-org/*', "access_token": "TOKEN"}
        test_client = GithubClient(config)
        expected_repositories = [
            'singer-io/test-repo',
            'test-org/repo1',
            'test-org/repo2',
            'test-org/repo3'
            ]
        mocked_get_all_repos.return_value = [
            'test-org/repo1',
            'test-org/repo2',
            'test-org/repo3'
        ]

        # Verify list of repo path with expected
        self.assertEqual(expected_repositories, test_client.extract_repos_from_config())