import unittest
import requests
import requests_mock
import simplejson as json

import tap_github

from itertools import cycle, permutations, chain


SESSION = requests.Session()
ADAPTER = requests_mock.Adapter()
SESSION.mount('mock://', ADAPTER)


@unittest.mock.patch('tap_github.verify_repo_access')
@unittest.mock.patch('tap_github.authed_get_all_pages')
class TestGetAllRepos(unittest.TestCase):

    def test_single_organization(self, mocked_authed_get_all_pages, mocked_verify_repo_access):
        orgs = ['test-org/*']
        repos = ['repo1', 'repo2', 'repo3']

        mocked_url = 'mock://github.com/orgs/test-org/repos'
        mocked_response_body = [
            {'full_name': ''.join(r).replace('*', '')} for r in zip(cycle(orgs), repos)
            ]
        mocked_response_text = json.dumps(mocked_response_body)
        ADAPTER.register_uri(
            'GET',
            mocked_url,
            text=mocked_response_text)
        mocked_response = SESSION.get(mocked_url)

        expected_repositories = [
            'test-org/repo1',
            'test-org/repo2',
            'test-org/repo3'
            ]
        mocked_authed_get_all_pages.return_value = [mocked_response]

        self.assertEqual(expected_repositories, tap_github.get_all_repos(orgs))

    def test_multiple_organizations(self, mocked_authed_get_all_pages, mocked_verify_repo_access):
        orgs = ['test-org/*', 'singer-io/*']
        repos = ['repo1', 'repo2', 'repo3']

        mocked_url = 'mock://github.com/orgs/test-org/repos'
        orgs_repos_permutations = [list(zip(orgs, perm)) for perm in permutations(repos, len(orgs))]
        mocked_response_body = [
            {'full_name': ''.join(r).replace('*', '')} for r in set(chain(*orgs_repos_permutations))
            ]
        mocked_response_text = json.dumps(mocked_response_body)
        ADAPTER.register_uri(
            'GET',
            mocked_url,
            text=mocked_response_text)
        mocked_response = SESSION.get(mocked_url)

        expected_repositories = [
            'test-org/repo1',
            'test-org/repo2',
            'test-org/repo3',
            'singer-io/repo1',
            'singer-io/repo2',
            'singer-io/repo3'
            ]
        mocked_authed_get_all_pages.return_value = [mocked_response]

        self.assertSetEqual(set(expected_repositories), set(tap_github.get_all_repos(orgs)))
