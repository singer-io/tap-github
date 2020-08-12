import os
import unittest
from datetime import datetime as dt
from datetime import timedelta

import tap_tester.menagerie   as menagerie
import tap_tester.connections as connections


class TestGithubBase(unittest.TestCase):
    REPLICATION_KEYS = "valid-replication-keys"
    PRIMARY_KEYS = "table-key-properties"
    FOREIGN_KEYS = "table-foreign-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    API_LIMIT = "max-row-limit"
    INCREMENTAL = "INCREMENTAL"
    FULL = "FULL_TABLE"
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z" # %H:%M:%SZ
    REPORTS_START_DATE = "2016-06-02T00:00:00Z" # test data for ad_reports is static
    REPORTS_END_DATE = "2016-06-06T00:00:00Z"

    def setUp(self):
        missing_envs = [x for x in [
            "TAP_GITHUB_TOKEN"
        ] if os.getenv(x) is None]
        if missing_envs:
            raise Exception("Missing environment variables: {}".format(missing_envs))

    @staticmethod
    def get_type():
        return "platform.github"

    @staticmethod
    def tap_name():
        return "tap-github"

    def get_properties(self, original: bool = True):
        """
        Maintain states for start_date and end_date
        :param original: set to false to change the start_date or end_date
        """
        return_value = {
            'start_date' : dt.strftime(dt.utcnow()-timedelta(days=5), self.START_DATE_FORMAT),
            'repository': 'singer-io/tap-github'
        }
        if original:
            return return_value

        # Reassign start and end dates
        return_value["start_date"] = self.START_DATE
        return return_value

    def get_credentials(self):
        return {
            'access_token': os.getenv("TAP_GITHUB_TOKEN")
        }

    @staticmethod
    def expected_check_streams():
        return {
            'assignees',
            'collaborators',
            'comments',
            'commit_comments',
            'commits',
            'events',
            'issue_labels',
            'issue_milestones',
            'issues',
            'pr_commits',
            'project_cards',
            'project_columns',
            'projects',
            'pull_request_reviews',
            'pull_requests',
            'releases',
            'review_comments',
            'reviews',
            'stargazers',
            'team_members',
            'team_memberships',
            'teams'
        }

    def expected_metadata(self):
        """The expected streams and metadata about the streams"""

        return {}

    def expected_replication_method(self):
        """return a dictionary with key of table name and value of replication method"""
        return {table: properties.get(self.REPLICATION_METHOD, None)
                for table, properties
                in self.expected_metadata().items()}

    def expected_incremental_streams(self):
        return set(stream for stream, rep_meth in self.expected_replication_method().items()
                   if rep_meth == self.INCREMENTAL)

    def expected_full_table_streams(self):
        return set(stream for stream, rep_meth in self.expected_replication_method().items()
                   if rep_meth == self.FULL)

    def expected_streams(self):
        """A set of expected stream names"""
        return set(self.expected_metadata().keys())

    def expected_primary_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of primary key fields
        """
        return {table: properties.get(self.PRIMARY_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}


    def expected_foreign_keys(self):
        """
        return dictionary with key of table name and
        value is set of foreign keys
        """
        return {}


    def select_all_streams_and_fields(self, conn_id, catalogs, select_all_fields: bool = True, exclude_streams=[]):
        """Select all streams and all fields within streams"""
        for catalog in catalogs:
            if exclude_streams and catalog.get('stream_name') in exclude_streams:
                continue
            schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
            non_selected_properties = []
            if not select_all_fields:
                # get a list of all properties so that none are selected
                non_selected_properties = schema.get('annotated-schema', {}).get(
                    'properties', {})
                # remove properties that are automatic
                for prop in self.expected_automatic_fields().get(catalog['stream_name'], []):
                    if prop in non_selected_properties:
                        del non_selected_properties[prop]
                non_selected_properties = non_selected_properties.keys()
            additional_md = []

            connections.select_catalog_and_fields_via_metadata(
                conn_id, catalog, schema, additional_md=additional_md,
                non_selected_fields=non_selected_properties
            )
