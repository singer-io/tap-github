import unittest
import requests
import requests_mock
import simplejson as json

import tap_github

from itertools import cycle


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
        side_effect = []
        for org in orgs:
            mocked_response_body = [
                {'full_name': ''.join(r).replace('*', '')} for r in zip(cycle([org]), repos)
                ]
            ADAPTER.register_uri(
                'GET',
                mocked_url,
                text=json.dumps(mocked_response_body))
            mocked_response = SESSION.get(mocked_url)
            mocked_authed_get_all_pages.return_value = [mocked_response]

            call_response = tap_github.get_all_repos([org])

            side_effect.extend(call_response)

        expected_repositories = [
            'test-org/repo1',
            'test-org/repo2',
            'test-org/repo3',
            'singer-io/repo1',
            'singer-io/repo2',
            'singer-io/repo3'
            ]

        self.assertListEqual(expected_repositories, side_effect)
