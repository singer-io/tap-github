import unittest
from unittest import mock
from tap_github.client import GithubClient, GithubException


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
        expected_organizations = {'singer-io'}

        # Verify list of repo path with expected
        self.assertEqual((expected_repositories, expected_organizations), test_client.extract_repos_from_config())

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
        expected_organizations = {
            'singer-io',
            'test-org'
            }
        mocked_get_all_repos.return_value = [
            'test-org/repo1',
            'test-org/repo2',
            'test-org/repo3'
        ]

        # Verify list of repo path with expected
        self.assertEqual((expected_repositories, expected_organizations), test_client.extract_repos_from_config())

    def test_organization_without_repo_in_config(self, mocked_get_all_repos, mock_verify_access):
        """
        Verify that the tap throws an exception with proper error message when just organization is provided in
        the config without the repository name.
        """
        config = {'repository': 'singer-io', "access_token": "TOKEN"}
        test_client = GithubClient(config)
        expected_error_message = "Please provide valid organization/repository for: ['singer-io']"
        with self.assertRaises(GithubException) as exc:
            test_client.extract_repos_from_config()

        # Verify that we get expected error message
        self.assertEqual(str(exc.exception), expected_error_message)

    def test_organization_without_repo_with_slash_in_config(self, mocked_get_all_repos, mock_verify_access):
        """
        Verify that the tap throws an exception with proper error message when just organization is provided in
        the config without the repository name.
        """
        config = {'repository': 'singer-io/', "access_token": "TOKEN"}
        test_client = GithubClient(config)
        expected_error_message = "Please provide valid organization/repository for: ['singer-io/']"
        with self.assertRaises(GithubException) as exc:
            test_client.extract_repos_from_config()

        # Verify that we get expected error message
        self.assertEqual(str(exc.exception), expected_error_message)

    def test_organization_with_only_slash_in_config(self, mocked_get_all_repos, mock_verify_access):
        """
        Verify that the tap throws an exception with proper error message when only / is provided in config.
        """
        config = {'repository': '/', "access_token": "TOKEN"}
        test_client = GithubClient(config)
        expected_error_message = "Please provide valid organization/repository for: ['/']"
        with self.assertRaises(GithubException) as exc:
            test_client.extract_repos_from_config()

        # Verify that we get expected error message
        self.assertEqual(str(exc.exception), expected_error_message)

    def test_organization_with_multiple_wrong_formatted_repo_path_in_config(self, mocked_get_all_repos, mock_verify_access):
        """
        Verify that the tap throws an exception with proper error message when multiple wrongly formatted repos are provided in config.
        """
        config = {'repository': 'singer-io/ /tap-github', "access_token": "TOKEN"}
        test_client = GithubClient(config)
        expected_error_message = "Please provide valid organization/repository for: {}"
        with self.assertRaises(GithubException) as exc:
            expected_repos, orgs = test_client.extract_repos_from_config()

            # Verify that we get expected error message
            self.assertEqual(str(exc.exception), expected_error_message.format(expected_repos))

    @mock.patch('tap_github.client.LOGGER.warning')
    def test_organization_with_duplicate_repo_paths_in_config(self, mock_warn, mocked_get_all_repos, mock_verify_access):
        """
        Verify that the tap logs proper warning message for duplicate repos in config and returns list without duplicates
        """
        config = {'repository': 'singer-io/tap-github singer-io/tap-github singer-io/test-repo', "access_token": "TOKEN"}
        test_client = GithubClient(config)
        expected_repos = ['singer-io/tap-github', 'singer-io/test-repo']
        actual_repos, orgs = test_client.extract_repos_from_config()
        expected_message = "Duplicate repositories found: %s and will be synced only once."

        # Verify that the logger is called with expected error message
        mock_warn.assert_called_with(expected_message, ['singer-io/tap-github'])

        # Verify that extract_repos_from_config() returns repos without duplicates
        self.assertEqual(sorted(expected_repos), sorted(actual_repos))