from datetime import datetime as dt
from tap_tester import runner, connections, menagerie
from base import TestGithubBase

class SnapchatInterruptedSyncTest(TestGithubBase):

    def name(self):
        return "tap_tester_github_interrupted_sync_test"

    def get_properties(self, original: bool = True):
        return_value = {
            'start_date' : '2021-10-01T00:00:00Z',
            'repository': 'singer-io/test-repo singer-io/tap-github'
        }
        return return_value

    def test_run(self):
        """
        Scenario: A sync job is interrupted. The state is saved with `completed_repos`.
                  The next sync job kicks off and the tap picks back up on that `completed_repos` repo.
        Expected State Structure:
            {
                "completed_repos": ["repo-1"],
                "bookmarks": {
                    "repo-1": {
                        "projects": {
                            "since": "2018-11-14T13:21:20.700360Z"
                        },
                        "project_columns": {
                            "since": "2018-11-14T13:21:20.700360Z"
                        }
                        "project_cards": {
                            "since": "2018-11-14T13:21:20.700360Z"
                        },
                        "pull_requests": {
                            "since": "2018-11-14T13:21:20.700360Z"
                        }
                    },
                    "repo-2": {
                        "projects": {
                            "since": "2018-11-14T13:21:20.700360Z"
                        },
                        "project_columns": {
                            "since": "2018-11-14T13:21:20.700360Z"
                        }
                        "project_cards": {
                            "since": "2018-11-14T13:21:20.700360Z"
                        }
                    }
                }
            }
        Test Cases:
        - Verify that less RECORDS are collected in interrupted sync then full sync.
        - Verify that RECORDS collected in interrupted sync are of the repo/repo's from where the sync was interrupted.
        """

        expected_streams = {'projects', 'project_columns', 'project_cards', 'pull_requests'}
        
        full_sync_repo = 'singer-io/test-repo' 
        interrupted_sync_repo = 'singer-io/tap-github'

        conn_id = connections.ensure_connection(self, original_properties=True)
        
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs = [catalog for catalog in found_catalogs
                         if catalog.get('stream_name') in expected_streams]

        self.perform_and_verify_table_and_field_selection(conn_id, test_catalogs)

        # run sync
        record_count_by_stream_full_sync = self.run_and_verify_sync(conn_id)
        synced_records_full_sync = runner.get_records_from_target_output()
        full_sync_state = menagerie.get_state(conn_id)

        # Create and set interrupted state 
        interrupted_state = full_sync_state
        interrupted_state['completed_repos'] = list(full_sync_repo)
        interrupted_state.get('bookmarks', {}).get(interrupted_sync_repo, {}).pop('pull_requests', None)
        menagerie.set_state(conn_id, interrupted_state)

        # run sync
        record_count_by_stream_interrupted_sync = self.run_and_verify_sync(conn_id)
        synced_records_interrupted_sync = runner.get_records_from_target_output()
        final_state = menagerie.get_state(conn_id)
        
        # Checking resuming sync resulted in successfully saved state
        with self.subTest():

            # Verify bookmarks are saved
            self.assertIsNotNone(final_state.get('bookmarks'))

            # Verify at the end "completed_repos" becomes empty
            self.assertIsNone(final_state.get('completed_repos'))

            # Verify final_state is equal to uninterrupted sync's state
            # (This is what the value would have been without an interruption and proves resuming succeeds)
            self.assertDictEqual(final_state, full_sync_state)

        # stream-level assertions
        for stream in expected_streams:
            with self.subTest(stream=stream):

                # gather results
                full_records = [record.get('data') for record in
                                synced_records_full_sync.get(stream, {'messages': []}).get('messages')
                                if record.get('action') == 'upsert']
                full_records_interrupted_repo = [record.get('data') for record in
                                                      synced_records_full_sync.get(stream, {'messages': []}).get('messages')
                                                      if record.get('action') == 'upsert' and record.get('data', {}).get('_sdc_repository', None) == interrupted_sync_repo]
                interrupted_records = [record.get('data') for record in
                                       synced_records_interrupted_sync.get(stream, {'messages': []}).get('messages')
                                       if record.get('action') == 'upsert']

                # Verify that less RECORDS are collected in interrupted sync then full sync
                self.assertGreater(full_records, interrupted_records)
                
                # Verify that RECORDS collected in interrupted sync are of the repo/repo's from where the sync was interrupted
                for record in interrupted_records:
                    self.assertEqual(record.get('_sdc_repository'), interrupted_sync_repo, msg="Found {} repo RECORDS which was already completed when state got interrupted".format(full_sync_repo))
                
                if stream == 'pull_requests':
                    # Verify that for pull_requests stream RECORDS are collected same as uninterrupted sync for that repo 
                    self.assertEqual(full_records_interrupted_repo, interrupted_records)
