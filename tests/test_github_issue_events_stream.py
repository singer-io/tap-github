import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import re

from base import TestGithubBase


def select_all_streams_and_fields(conn_id, catalogs):
    """Select all streams and all fields within streams"""
    for catalog in catalogs:
        schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])

        connections.select_catalog_and_fields_via_metadata(conn_id, catalog, schema)


class TestGithubIssueEventsStreams(TestGithubBase):
    def name(self):
        return "tap_tester_github_issue_events_streams"


    def test_run(self):
        conn_id = connections.ensure_connection(self)

        #run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        #verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))
        self.assertEqual(len(found_catalogs),
                         len(self.expected_check_streams()),
                         msg="Expected {} streams, actual was {} for connection {},"
                         " actual {}".format(
                             len(self.expected_check_streams()),
                             len(found_catalogs),
                             found_catalogs,
                             conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))
        self.assertEqual(set(self.expected_check_streams()),
                         set(found_catalog_names),
                         msg="Expected streams don't match actual streams")

        # Verify stream names follow naming convention
        # streams should only have lowercase alphas and underscores
        self.assertTrue(all([re.fullmatch(r"[a-z_]+", name) for name in found_catalog_names]),
                        msg="One or more streams don't follow standard naming")

        diff = self.expected_check_streams().symmetric_difference(found_catalog_names)
        self.assertEqual(len(diff), 0, msg="discovered schemas do not match: {}".format(diff))
        print("discovered schemas are OK")

        # Select the 3 projects streams
        catalogs_to_test = ['issue_events']
        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get('tap_stream_id') in catalogs_to_test]
        select_all_streams_and_fields(conn_id, our_catalogs)

        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify actual rows were synced
        synced_records = runner.get_records_from_target_output()

        for catalog_name in catalogs_to_test:
            synced_messages = synced_records.get(catalog_name, {}).get('messages', [])
            if catalog_name == 'issue_events':
                import ipdb; ipdb.set_trace()
                1+1
            self.assertGreater(len(synced_messages), 0, msg="Expect synced_messages to sync data for stream {}".format(catalog_name))


        
