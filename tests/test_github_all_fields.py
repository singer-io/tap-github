import os

from tap_tester import runner, connections, menagerie

from base import TestGithubBase

KNOWN_MISSING_FIELDS = {
    'events': {
        'ref',
        'head',
        'push_id',
        'distinct_size',
        'size'
    },
    'project_cards': {
        'name',
        'cards_url'
    },
    'commits': {
        'id',
        'pr_number',
        'pr_id',
        'files'
    },
    'review_comments': {
        'assignees',
        'commits_url',
        'diff_url',
        'head',
        'review_comments_url',
        'comments_url',
        'issue_url',
        'assignee',
        'requested_teams',
        'patch_url',
        'milestone',
        'review_comment_url',
        'statuses_url',
        'requested_reviewers',
        'labels',
        'base',
        'merge_commit_sha',
        'locked'
    },
    'comments': {
        'home_url'
    },
    'pr_commits': {
        'files'
    }
}

class TestGithubAllFields(TestGithubBase):
    """Test that with all fields selected for a stream automatic and available fields are  replicated"""

    @staticmethod
    def name():
        return "tap_tester_github_all_fields"

    def test_run(self):
        """
        • Verify no unexpected streams were replicated
        • Verify that more than just the automatic fields are replicated for each stream. 
        • verify all fields for each stream are replicated
        """

        expected_streams = self.expected_streams()
        # instantiate connection
        conn_id = connections.ensure_connection(self)

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs_all_fields = [catalog for catalog in found_catalogs
                                    if catalog.get('stream_name') in expected_streams]
        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs_all_fields, select_all_fields=True,
        )

        # grab metadata after performing table-and-field selection to set expectations
        stream_to_all_catalog_fields = dict() # used for asserting all fields are replicated
        for catalog in test_catalogs_all_fields:
            stream_id, stream_name = catalog['stream_id'], catalog['stream_name']
            catalog_entry = menagerie.get_annotated_schema(conn_id, stream_id)
            fields_from_field_level_md = [md_entry['breadcrumb'][1]
                                          for md_entry in catalog_entry['metadata']
                                          if md_entry['breadcrumb'] != []]
            stream_to_all_catalog_fields[stream_name] = set(fields_from_field_level_md)

        # run initial sync
        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(expected_streams, synced_stream_names)

        for stream in expected_streams:
            with self.subTest(stream=stream):
                # expected values
                expected_automatic_keys = self.expected_automatic_keys().get(stream)

                # get all expected keys
                expected_all_keys = stream_to_all_catalog_fields[stream]

                messages = synced_records.get(stream)
                # collect actual values
                actual_all_keys = set()
                for message in messages['messages']:
                    if message['action'] == 'upsert':
                        actual_all_keys.update(message['data'].keys())
                    
                expected_all_keys = expected_all_keys - KNOWN_MISSING_FIELDS.get(stream, set())

                # verify all fields for a stream were replicated
                self.assertGreater(len(expected_all_keys), len(expected_automatic_keys))
                self.assertTrue(expected_automatic_keys.issubset(expected_all_keys), msg=f'{expected_automatic_keys-expected_all_keys} is not in "expected_all_keys"')
                self.assertSetEqual(expected_all_keys, actual_all_keys)
