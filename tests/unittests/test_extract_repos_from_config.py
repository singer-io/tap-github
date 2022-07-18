import unittest
from unittest import mock
from tap_github.client import GithubClient, GithubException

@mock.patch('tap_github.client.GithubClient.set_auth_in_session')
@mock.patch('tap_github.client.GithubClient.verify_access_for_repo')
@mock.patch('tap_github.client.GithubClient.get_all_repos')
class TestExtractReposFromConfig(unittest.TestCase):
    """
    Test `extract_repos_from_config` method from client.
    """

    def test_single_repo(self, mocked_get_all_repos, mock_verify_access, mock_set_auth_in_session):
        """
        Test `extract_repos_from_config` if only one repo path is given in config.
        """
        config = {'repository': 'singer-io/test-repo', "access_token": "TOKEN"}
        test_client = GithubClient(config)
        expected_repositories = ['singer-io/test-repo']

        # Verify list of repo path with expected
        self.assertEqual(sorted(expected_repositories), sorted(test_client.extract_repos_from_config()))

    def test_multiple_repos(self, mocked_get_all_repos, mock_verify_access, mock_set_auth_in_session):
        """
        Test `extract_repos_from_config` if multiple repo paths are given in config.
        """
        config = {'repository': 'singer-io/test-repo singer-io/tap-github', "access_token": "TOKEN"}
        test_client = GithubClient(config)
        expected_repositories = ['singer-io/test-repo', 'singer-io/tap-github']

        # Verify list of repo path with expected
        self.assertEqual(sorted(expected_repositories), sorted(test_client.extract_repos_from_config()))

    def test_org_all_repos(self, mocked_get_all_repos, mock_verify_access, mock_set_auth_in_session):
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
        self.assertEqual(sorted(expected_repositories), sorted(test_client.extract_repos_from_config()))

    def test_organization_without_repo_in_config(self, mocked_get_all_repos, mock_verify_access, mock_set_auth_in_session):
        """
        Verify that the tap throws an exception with proper error message when just organization is provided in
        the config without the repository name.
        """
        config = {'repository': 'singer-io'}
        test_client = GithubClient(config)
        expected_error_message = "Please provide proper organization/repository: ['singer-io']"
        with self.assertRaises(GithubException) as exc:
            test_client.extract_repos_from_config()

        # Verify that we get expected error message
        self.assertEqual(str(exc.exception), expected_error_message)

    def test_organization_without_repo_with_slash_in_config(self, mocked_get_all_repos, mock_verify_access, mock_set_auth_in_session):
        """
        Verify that the tap throws an exception with proper error message when just organization is provided in
        the config without the repository name.
        """
        config = {'repository': 'singer-io/'}
        test_client = GithubClient(config)
        expected_error_message = "Please provide proper organization/repository: ['singer-io/']"
        with self.assertRaises(GithubException) as exc:
            test_client.extract_repos_from_config()

        # Verify that we get expected error message
        self.assertEqual(str(exc.exception), expected_error_message)

    def test_organization_with_only_slash_in_config(self, mocked_get_all_repos, mock_verify_access, mock_set_auth_in_session):
        """
        Verify that the tap throws an exception with proper error message when only / is provided in config.
        """
        config = {'repository': '/'}
        test_client = GithubClient(config)
        expected_error_message = "Please provide proper organization/repository: ['/']"
        with self.assertRaises(GithubException) as exc:
            test_client.extract_repos_from_config()

        # Verify that we get expected error message
        self.assertEqual(str(exc.exception), expected_error_message)

    def test_organization_with_multiple_wrong_formatted_repo_path_in_config(self, mocked_get_all_repos, mock_verify_access, mock_set_auth_in_session):
        """
        Verify that the tap throws an exception with proper error message when multiple wrongly formatted repos are provided in config.
        """
        config = {'repository': 'singer-io/ /tap-github'}
        test_client = GithubClient(config)
        expected_error_message = "Please provide proper organization/repository: {}"
        with self.assertRaises(GithubException) as exc:
            expected_repos = test_client.extract_repos_from_config()

            # Verify that we get expected error message
            self.assertEqual(str(exc.exception), expected_error_message.format(expected_repos))

    @mock.patch('tap_github.client.LOGGER.warn')
    def test_organization_with_duplicate_repo_paths_in_config(self, mock_warn, mocked_get_all_repos, mock_verify_access, mock_set_auth_in_session):
        """
        Verify that the tap logs proper warning message for duplicate repos in config and returns list without duplicates
        """
        config = {'repository': 'singer-io/tap-github singer-io/tap-github singer-io/test-repo'}
        test_client = GithubClient(config)
        expected_repos = ['singer-io/tap-github', 'singer-io/test-repo']
        actual_repos = test_client.extract_repos_from_config()

        # Verify that the logger is called with expected error message
        mock_warn.assert_called_with("Duplicate repositories found: {} and will be synced only once.".format(['singer-io/tap-github']))

        # Verify that extract_repos_from_config() returns repos without duplicates
        self.assertEqual(sorted(expected_repos), sorted(actual_repos))