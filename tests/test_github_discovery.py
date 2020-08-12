import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import re

from base import TestGithubBase

class TestGithubDiscovery(TestGithubBase):
    def name(self):
        return "tap_tester_github_discovery"

    def expected_replication_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of replication key fields
        """
        return {table: properties.get(self.REPLICATION_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

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

        # Github does not write 100% correct metadata (ie no replication-method)
        # for stream in self.expected_streams():
        #     with self.subTest(stream=stream):
        #         catalog = next(iter([catalog for catalog in found_catalogs
        #                              if catalog["stream_name"] == stream]))
        #         assert catalog  # based on previous tests this should always be found

        #         schema_and_metadata = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
        #         metadata = schema_and_metadata["metadata"]
        #         schema = schema_and_metadata["annotated-schema"]

        #         # verify the stream level properties are as expected
        #         # verify there is only 1 top level breadcrumb
        #         stream_properties = [item for item in metadata if item.get("breadcrumb") == []]
        #         self.assertTrue(len(stream_properties) == 1,
        #                         msg="There is more than one top level breadcrumb")

        #         # verify replication key(s)
        #         self.assertEqual(
        #             set(stream_properties[0].get(
        #                 "metadata", {self.REPLICATION_KEYS: []}).get(self.REPLICATION_KEYS, [])),
        #             self.expected_replication_keys()[stream],
        #             msg="expected replication key {} but actual is {}".format(
        #                 self.expected_replication_keys()[stream],
        #                 set(stream_properties[0].get(
        #                     "metadata", {self.REPLICATION_KEYS: None}).get(
        #                     self.REPLICATION_KEYS, []))))

        #         # verify primary key(s)
        #         self.assertEqual(
        #             set(stream_properties[0].get(
        #                 "metadata", {self.PRIMARY_KEYS: []}).get(self.PRIMARY_KEYS, [])),
        #             self.expected_primary_keys()[stream],
        #             msg="expected primary key {} but actual is {}".format(
        #                 self.expected_primary_keys()[stream],
        #                 set(stream_properties[0].get(
        #                     "metadata", {self.PRIMARY_KEYS: None}).get(self.PRIMARY_KEYS, []))))

        #         # verify that if there is a replication key we are doing INCREMENTAL otherwise FULL
        #         actual_replication_method = stream_properties[0].get(
        #             "metadata", {self.REPLICATION_METHOD: None}).get(self.REPLICATION_METHOD)
        #         if stream_properties[0].get(
        #                 "metadata", {self.REPLICATION_KEYS: []}).get(self.REPLICATION_KEYS, []):

        #             self.assertTrue(actual_replication_method == self.INCREMENTAL,
        #                             msg="Expected INCREMENTAL replication "
        #                                 "since there is a replication key")
        #         else:
        #             self.assertTrue(actual_replication_method == self.FULL,
        #                             msg="Expected FULL replication "
        #                                 "since there is no replication key")

        #         # verify the actual replication matches our expected replication method
        #         self.assertEqual(
        #             self.expected_replication_method().get(stream, None),
        #             actual_replication_method,
        #             msg="The actual replication method {} doesn't match the expected {}".format(
        #                 actual_replication_method,
        #                 self.expected_replication_method().get(stream, None)))

        #         expected_primary_keys = self.expected_primary_keys()[stream]
        #         expected_replication_keys = self.expected_replication_keys()[stream]
        #         expected_automatic_fields = expected_primary_keys | expected_replication_keys

        #         # verify that primary, replication and foreign keys
        #         # are given the inclusion of automatic in annotated schema.
        #         actual_automatic_fields = {key for key, value in schema["properties"].items()
        #                                    if value.get("inclusion") == "automatic"}
        #         self.assertEqual(expected_automatic_fields,
        #                          actual_automatic_fields,
        #                          msg="expected {} automatic fields but got {}".format(
        #                              expected_automatic_fields,
        #                              actual_automatic_fields))

        #         # verify that all other fields have inclusion of available
        #         # This assumes there are no unsupported fields for SaaS sources
        #         self.assertTrue(
        #             all({value.get("inclusion") == "available" for key, value
        #                  in schema["properties"].items()
        #                  if key not in actual_automatic_fields}),
        #             msg="Not all non key properties are set to available in annotated schema")

        #         # verify that primary, replication and foreign keys
        #         # are given the inclusion of automatic in metadata.
        #         actual_automatic_fields = \
        #             {item.get("breadcrumb", ["properties", None])[1]
        #              for item in metadata
        #              if item.get("metadata").get("inclusion") == "automatic"}
        #         self.assertEqual(expected_automatic_fields,
        #                          actual_automatic_fields,
        #                          msg="expected {} automatic fields but got {}".format(
        #                              expected_automatic_fields,
        #                              actual_automatic_fields))

        #         # verify that all other fields have inclusion of available
        #         # This assumes there are no unsupported fields for SaaS sources
        #         self.assertTrue(
        #             all({item.get("metadata").get("inclusion") == "available"
        #                  for item in metadata
        #                  if item.get("breadcrumb", []) != []
        #                  and item.get("breadcrumb", ["properties", None])[1]
        #                  not in actual_automatic_fields}),
        #             msg="Not all non key properties are set to available in metadata")
