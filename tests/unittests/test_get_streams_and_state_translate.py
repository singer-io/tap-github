import unittest
from tap_github.sync import get_selected_streams, translate_state, get_stream_to_sync

def get_stream_catalog(stream_name, selected_in_schema = False, selected_in_metadata = False):
    """Return catalog for stream"""
    return {
                "schema":{"selected": selected_in_schema},
                "tap_stream_id": stream_name,
                "key_properties": [],
                "metadata": [
                        {
                            "breadcrumb": [],
                            "metadata":{"selected": selected_in_metadata}
                        }
                    ]
            }

class TestGetSelectedStreams(unittest.TestCase):
    """
    Testcase for `get_selected_streams` in sync
    """

    def test_selected_in_schema(self):
        """Verify if stream is selected in schema"""
        catalog = {
            "streams": [
                get_stream_catalog("assignees", selected_in_schema=True),
                get_stream_catalog("releases"),
            ]
        }
        self.assertListEqual(["assignees"],get_selected_streams(catalog))

    def test_selected_in_metadata(self):
        """Verify if stream is selected in metadata"""
        catalog = {
            "streams": [
                get_stream_catalog("assignees", selected_in_metadata=True),
                get_stream_catalog("comments"),
                get_stream_catalog("commits", selected_in_metadata=True)
            ]
        }

        self.assertListEqual(["assignees", "commits"], get_selected_streams(catalog))

class TestTranslateState(unittest.TestCase):
    """
    Testcase for `translate_state` in sync
    """

    catalog = {
        "streams": [
            get_stream_catalog("comments", selected_in_schema=True),
            get_stream_catalog("releases"),
            get_stream_catalog("issue_labels"),
            get_stream_catalog("issue_events")
        ]
    }

    def test_state_with_repo_name(self):
        """Verify for state with repo name"""
        state = {
            "bookmarks": {
                "org/test-repo" : {
                        "comments": {"since": "2019-01-01T00:00:00Z"}
                    },
                "org/test-repo2" : {}
            }
        }

        final_state = translate_state(state, self.catalog, ["org/test-repo", "org/test-repo2"])
        self.assertEqual(state, dict(final_state))

    def test_state_without_repo_name(self):
        """Verify for state without repo name"""
        state = {
            "bookmarks": {
                "comments": {"since": "2019-01-01T00:00:00Z"}
            }
        }
        expected_state =  {
            "bookmarks": {
                "org/test-repo" : {
                        "comments": {"since": "2019-01-01T00:00:00Z"}
                    },
                "org/test-repo2" : {
                        "comments": {"since": "2019-01-01T00:00:00Z"}
                    }
            }
        }
        final_state = translate_state(state, self.catalog, ["org/test-repo", "org/test-repo2"])
        self.assertEqual(expected_state, dict(final_state))

    def test_with_empty_state(self):
        """Verify for empty state"""

        final_state = translate_state({}, self.catalog, ["org/test-repo"])

        self.assertEqual({}, dict(final_state))

class TestGetStreamsToSync(unittest.TestCase):
    """
    Testcase for `get_stream_to_sync` in sync
    """

    def get_catalog(self, parent=False, mid_child = False, child = False):
        return {
            "streams": [
                get_stream_catalog("projects", selected_in_schema=parent),
                get_stream_catalog("project_columns", selected_in_schema=mid_child),
                get_stream_catalog("project_cards", selected_in_schema=child),
                get_stream_catalog("teams", selected_in_schema=parent),
                get_stream_catalog("team_members", selected_in_schema=mid_child),
                get_stream_catalog("team_memberships", selected_in_schema=child),
                get_stream_catalog("assignees", selected_in_schema=parent),
            ]
        }

    def test_parent_selected(self):
        """Test if only parents selected"""
        expected_streams = ["assignees", "projects", "teams"]
        catalog = self.get_catalog(parent=True)
        sync_streams = get_stream_to_sync(catalog)
        
        self.assertEqual(sync_streams, expected_streams)
    
    def test_mid_child_selected(self):
        """Test if only mid child selected"""
        expected_streams = ["projects", "project_columns", "teams", "team_members"]
        catalog = self.get_catalog(mid_child=True)
        sync_streams = get_stream_to_sync(catalog)
        
        self.assertEqual(sync_streams, expected_streams)
    
    def test_lowest_child_selected(self):
        """Test if only lower child selected"""
        expected_streams = ["projects", "project_columns", "project_cards", 
                            "teams", "team_members", "team_memberships"]
        catalog = self.get_catalog(child=True)
        sync_streams = get_stream_to_sync(catalog)
        
        self.assertEqual(sync_streams, expected_streams)
