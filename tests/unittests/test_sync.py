import unittest
from unittest import mock
from tap_github.sync import sync, write_schemas



def get_stream_catalog(stream_name, is_selected = False):
    """Return catalog for stream"""
    return {
                "schema":{},
                "tap_stream_id": stream_name,
                "metadata": [
                        {
                            "breadcrumb": [],
                            "metadata":{
                                "selected": is_selected
                            }
                        }
                    ],
                "key_properties": []
            }


@mock.patch("singer.write_state")
@mock.patch("tap_github.sync.write_schemas")
@mock.patch("tap_github.streams.IncrementalStream.sync_endpoint")
class TestSyncFunctions(unittest.TestCase):
    """
    Test `sync` function
    """

    @mock.patch("tap_github.streams.IncrementalOrderedStream.sync_endpoint")
    def test_sync_all_parents(self, mock_inc_ordered, mock_incremental, mock_write_schemas, mock_write_state):
        """
        Test sync function with only all parents selected
        """

        mock_catalog = {"streams": [
            get_stream_catalog("projects", True),
            get_stream_catalog("pull_requests", True)
        ]}

        client = mock.Mock()
        client.extract_repos_from_config.return_value = ["test-repo"]
        client.authed_get_all_pages.return_value = []
        client.extract_orgs_from_config.return_value = ["singer-io"]

        sync(client, {'start_date': ""}, {}, mock_catalog)

        # Verify write schema is called for selected streams
        self.assertEqual(mock_write_schemas.call_count, 2)

        self.assertEqual(mock_write_schemas.mock_calls[0], mock.call("projects", mock.ANY, mock.ANY))
        self.assertEqual(mock_write_schemas.mock_calls[1], mock.call("pull_requests", mock.ANY, mock.ANY))

    @mock.patch("tap_github.streams.IncrementalOrderedStream.sync_endpoint")
    def test_sync_only_child(self, mock_inc_ordered, mock_incremental, mock_write_schemas, mock_write_state):
        """
        Test sync function with only all children selected
        """

        mock_catalog = {"streams": [
            get_stream_catalog("projects"),
            get_stream_catalog("project_columns"),
            get_stream_catalog("project_cards", True),
            get_stream_catalog("pull_requests"),
            get_stream_catalog("review_comments", True)
        ]}

        client = mock.Mock()
        client.extract_repos_from_config.return_value = ["test-repo"]
        client.authed_get_all_pages.return_value = []
        client.extract_orgs_from_config.return_value = ["singer-io"]

        sync(client, {'start_date': "2019-01-01T00:00:00Z"}, {}, mock_catalog)

        # Verify write schema is called for selected streams
        self.assertEqual(mock_write_schemas.call_count, 2)

        self.assertEqual(mock_write_schemas.mock_calls[0], mock.call("projects", mock.ANY, mock.ANY))
        self.assertEqual(mock_write_schemas.mock_calls[1], mock.call("pull_requests", mock.ANY, mock.ANY))

    @mock.patch("tap_github.streams.FullTableStream.sync_endpoint")
    def test_sync_only_mid_child(self, mock_full_table, mock_incremental, mock_write_schemas, mock_write_state):
        """
        Test sync function with only all mid child selected
        """

        mock_catalog = {"streams": [
            get_stream_catalog("projects"),
            get_stream_catalog("project_columns", True),
            get_stream_catalog("project_cards"),
            get_stream_catalog("teams"),
            get_stream_catalog("team_members", True),
            get_stream_catalog("team_memberships")
        ]}

        client = mock.Mock()
        client.extract_repos_from_config.return_value = ["test-repo"]
        client.authed_get_all_pages.return_value = []
        client.extract_orgs_from_config.return_value = ["singer-io"]

        sync(client, {'start_date': ""}, {}, mock_catalog)

        # Verify write schema is called for selected streams
        self.assertEqual(mock_write_schemas.call_count, 2)

        self.assertEqual(mock_write_schemas.mock_calls[0], mock.call("teams", mock.ANY, mock.ANY))
        self.assertEqual(mock_write_schemas.mock_calls[1], mock.call("projects", mock.ANY, mock.ANY))

@mock.patch("singer.write_schema")
class TestWriteSchemas(unittest.TestCase):

    mock_catalog = {"streams": [
        get_stream_catalog("projects"),
        get_stream_catalog("project_columns"),
        get_stream_catalog("project_cards")
    ]}

    def test_parents_selected(self, mock_write_schema):
        write_schemas("projects", self.mock_catalog, ["projects"])
        mock_write_schema.assert_called_with("projects", mock.ANY, mock.ANY)

    def test_mid_child_selected(self, mock_write_schema):
        write_schemas("project_columns", self.mock_catalog, ["project_columns"])
        mock_write_schema.assert_called_with("project_columns", mock.ANY, mock.ANY)

    def test_nested_child_selected(self, mock_write_schema):
        write_schemas("project_cards", self.mock_catalog, ["project_cards"])
        mock_write_schema.assert_called_with("project_cards", mock.ANY, mock.ANY)
