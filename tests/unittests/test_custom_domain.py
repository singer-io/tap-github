import unittest
from unittest import mock
from tap_github.client import GithubClient, DEFAULT_DOMAIN

@mock.patch('tap_github.GithubClient.verify_access_for_repo', return_value = None)
class TestCustomDomain(unittest.TestCase):
    """
    Test custom domain is supported in client
    """

    def test_config_without_domain(self, mock_verify_access):
        """
        Test if the domain is not given in the config
        """
        mock_config = {'repository': 'singer-io/test-repo', "access_token": ""}
        test_client = GithubClient(mock_config)

        # Verify domain in client is default
        self.assertEqual(test_client.base_url, DEFAULT_DOMAIN)
    
    def test_config_with_domain(self, mock_verify_access):
        """
        Test if the domain is given in the config
        """
        mock_config = {'repository': 'singer-io/test-repo', "base_url": "http://CUSTOM-git.com", "access_token": ""}
        test_client = GithubClient(mock_config)

        # Verify domain in client is from config
        self.assertEqual(test_client.base_url, mock_config["base_url"])

    def test_prepare_url(self, mock_verify_access):
        """
        Test if the correct params are added to url
        """
        mock_config = {'repository': 'singer-io/test-repo', "base_url": "http://CUSTOM-git.com", "access_token": "", "max_per_page": 35}
        test_client = GithubClient(mock_config)

        # Verify domain in client is from config
        self.assertEqual(test_client.prepare_url(test_client.base_url), f"{mock_config['base_url'].lower()}/?per_page=35")
        self.assertEqual(test_client.prepare_url('http://CUSTOM-git.com/?q=query'), 'http://custom-git.com/?q=query&per_page=35')

        del mock_config["max_per_page"]
        test_client2 = GithubClient(mock_config)
        self.assertEqual(test_client2.prepare_url(test_client2.base_url), f"{mock_config['base_url'].lower()}/?per_page=100")