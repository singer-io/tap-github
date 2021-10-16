"""
Test that with no fields selected for a stream automatic fields are still replicated
"""
from tap_tester import runner, connections

from base import TestGithubBase


class TestGithubAutomaticFields(TestGithubBase):
    """Test that with no fields selected for a stream automatic fields are still replicated"""

    @staticmethod
    def name():
        return "tap_tester_github_automatic_fields"

    def test_run(self):
        """
        Verify that for each stream you can get multiple pages of data
        when no fields are selected and only the automatic fields are replicated.
        """

        expected_streams = set([
            'assignees',
            'collaborators',
            'comments',
            'commit_comments',
            'commits',
            'events',
            'issue_labels',
            'issue_milestones',
            'issue_events',
            'issues',
            'pr_commits',
            'project_cards',
            'project_columns',
            'projects',
            'pull_requests',
            'releases',
            'review_comments',
            'reviews',
            'stargazers',
            'team_members',
            'teams'
        ])

        # instantiate connection
        conn_id = connections.ensure_connection(self)

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs_automatic_fields = [catalog for catalog in found_catalogs
                                          if catalog.get('stream_name') in expected_streams]

        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs_automatic_fields, select_all_fields=False,
        )

        # run initial sync
        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):
                # expected values
                expected_keys = self.expected_primary_keys().get(stream)

                # collect actual values
                data = synced_records.get(stream, {})
                record_messages_keys = [set(row.get('data').keys()) for row in data.get('messages', {})]

                # Verify that you get some records for each stream
                self.assertGreater(
                    record_count_by_stream.get(stream, -1), 0,
                    msg="The number of records is not over the stream max limit for the {} stream".format(stream))

                # Verify that only the automatic fields are sent to the target
                for actual_keys in record_messages_keys:
                    self.assertSetEqual(expected_keys, actual_keys)
