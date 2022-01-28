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
        - Verify that for each stream you can get multiple pages of data
            when no fields are selected.
        - Verify that only the automatic fields are sent to the target.
        - Verify that all replicated records have unique primary key values.
        """
        # Exclude collaborators stream due to access issues in circle
        expected_streams = self.expected_streams() - {'collaborators'}

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
                primary_keys_list = [
                    tuple(message.get('data').get(expected_pk) for expected_pk in expected_keys)
                    for message in data.get('messages')
                    if message.get('action') == 'upsert']
                unique_primary_keys_list = set(primary_keys_list)

                # Verify that you get some records for each stream
                self.assertGreater(
                    record_count_by_stream.get(stream, -1), 0,
                    msg="The number of records is not over the stream max limit for the {} stream".format(stream))

                # Verify that only the automatic fields are sent to the target
                for actual_keys in record_messages_keys:
                    self.assertSetEqual(expected_keys, actual_keys)

                # BUG-TDL-17507 An org can have multiple teams with overlapping membership
                if stream != 'team_members':
                    # Verify that all replicated records have unique primary key values.
                    self.assertEqual(
                        len(primary_keys_list),
                        len(unique_primary_keys_list),
                        msg="Replicated record does not have unique primary key values.")
