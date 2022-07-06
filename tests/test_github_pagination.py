from tap_tester import runner, connections

from base import TestGithubBase

class GitHubPaginationTest(TestGithubBase):

    @staticmethod
    def name():
        return "tap_tester_github_pagination_test"

    def get_properties(self, original: bool = True):
        return_value = {
            'start_date' : '2020-01-01T00:00:00Z',
            'repository': self.repository_name
        }
        if original:
            return return_value

        # Reassign start and end dates
        return_value["start_date"] = self.START_DATE
        return return_value

    def test_run(self):
        # Pagination is not supported for team_memberships by Github API
        # https://docs.github.com/en/rest/teams/members#get-team-membership-for-a-user
        unsupported_pagination_streams = {
            'team_memberships'
        }
        
        # For the below streams RECORD count is <= 30
        unexpected_stream = {
            'teams'
        }
        
        # For some streams RECORD count were not > 30 in same test-repo. So, separated streams on the basis of RECORD count 
        self.repository_name = 'singer-io/tap-github'
        self.run_test({'comments', 'stargazers', 'commits', 'pull_requests', 'reviews', 'review_comments', 'pr_commits', 'issues'})
        
        self.repository_name = 'singer-io/test-repo'
        self.run_test({'issue_labels', 'events', 'collaborators', 'issue_events', 'team_members', 'assignees', 'commit_comments', 'projects', 'project_cards', 'project_columns', 'issue_milestones', 'releases'})
    
    def run_test(self, streams):
        # page size for "pull_requests"
        page_size = 30
        conn_id = connections.ensure_connection(self)
        
        expected_streams = streams
        
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs = [catalog for catalog in found_catalogs
                         if catalog.get('stream_name') in expected_streams]

        self.perform_and_verify_table_and_field_selection(conn_id, test_catalogs)

        record_count_by_stream = self.run_and_verify_sync(conn_id)

        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(expected_streams, synced_stream_names)

        for stream in expected_streams:
            with self.subTest(stream=stream):
                # expected values
                expected_primary_keys = self.expected_primary_keys()[stream]

                # collect information for assertions from syncs 1 & 2 base on expected values
                record_count_sync = record_count_by_stream.get(stream, 0)
                primary_keys_list = [tuple(message.get('data').get(expected_pk)
                                           for expected_pk in expected_primary_keys)
                                     for message in synced_records.get(stream).get('messages')
                                     if message.get('action') == 'upsert']

                # verify records are more than page size so multiple page is working
                self.assertGreater(record_count_sync, page_size)

                primary_keys_list_1 = primary_keys_list[:page_size]
                primary_keys_list_2 = primary_keys_list[page_size:2*page_size]

                primary_keys_page_1 = set(primary_keys_list_1)
                primary_keys_page_2 = set(primary_keys_list_2)

                # Verify by private keys that data is unique for page
                self.assertEqual(len(primary_keys_page_1), page_size)
                self.assertTrue(primary_keys_page_1.isdisjoint(primary_keys_page_2))
