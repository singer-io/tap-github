import unittest
from unittest import mock
from tap_github.sync import sync, write_schemas, translate_state



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

        mock_catalog = {"streams": [get_stream_catalog("pull_requests", True)]}

        client = mock.Mock()
        client.extract_repos_from_config.return_value = (["test-repo"], set())
        client.authed_get_all_pages.return_value = []
        client.not_accessible_repos = {}

        sync(client, {'start_date': ""}, {}, mock_catalog)

        # Verify write schema is called for selected streams
        self.assertEqual(mock_write_schemas.call_count, 1)

        self.assertEqual(mock_write_schemas.mock_calls[0], mock.call("pull_requests", mock.ANY, mock.ANY))

    @mock.patch("tap_github.streams.IncrementalOrderedStream.sync_endpoint")
    def test_sync_only_child(self, mock_inc_ordered, mock_incremental, mock_write_schemas, mock_write_state):
        """
        Test sync function with only all children selected
        """

        mock_catalog = {"streams": [
            get_stream_catalog("pull_requests"),
            get_stream_catalog("review_comments", True)
        ]}

        client = mock.Mock()
        client.extract_repos_from_config.return_value = (["test-repo"], {"org"})
        client.authed_get_all_pages.return_value = []
        client.not_accessible_repos = {}

        sync(client, {'start_date': "2019-01-01T00:00:00Z"}, {}, mock_catalog)

        # Verify write schema is called for selected streams
        self.assertEqual(mock_write_schemas.call_count, 1)

        self.assertEqual(mock_write_schemas.mock_calls[0], mock.call("pull_requests", mock.ANY, mock.ANY))

    @mock.patch("tap_github.streams.FullTableStream.sync_endpoint")
    def test_sync_only_mid_child(self, mock_full_table, mock_incremental, mock_write_schemas, mock_write_state):
        """
        Test sync function with only all mid child selected
        """

        mock_catalog = {"streams": [
            get_stream_catalog("teams"),
            get_stream_catalog("team_members", True),
            get_stream_catalog("team_memberships")
        ]}

        client = mock.Mock()
        client.extract_repos_from_config.return_value = (["test-repo"], {"org"})
        client.authed_get_all_pages.return_value = []
        client.not_accessible_repos = {}

        sync(client, {'start_date': ""}, {}, mock_catalog)

        # Verify write schema is called for selected streams
        self.assertEqual(mock_write_schemas.call_count, 1)

        self.assertEqual(mock_write_schemas.mock_calls[0], mock.call("teams", mock.ANY, mock.ANY))

    @mock.patch("tap_github.sync.get_stream_to_sync", return_value = [])
    @mock.patch("tap_github.sync.get_selected_streams", return_value = [])
    @mock.patch("tap_github.sync.update_currently_syncing_repo")
    def test_no_streams_selected(self, mock_update_curr_sync, mock_selected_streams, mock_sync_streams,
                                 mock_incremental, mock_write_schemas, mock_write_state):
        """
        Test if no streams are selected then the state does not update,
        and `update_currently_syncing_repo` function is not called.
        """

        state = {
                    "currently_syncing_repo": "singer-io/test-repo",
                    "bookmarks": {},
                    "currently_syncing": "teams"
                }
        mock_catalog = {"streams": [
            get_stream_catalog("teams"),
            get_stream_catalog("team_members", True)
        ]}

        expected_state = {
                            "currently_syncing_repo": "singer-io/test-repo",
                            "bookmarks": {},
                            "currently_syncing": "teams"
                        }
        client = mock.Mock()
        client.extract_repos_from_config.return_value = ["test-repo"], ["org1"]
        sync(client, {'start_date': ""}, state, mock_catalog)

        # Verify state is not changed
        self.assertEqual(state, expected_state)

        # Verify updated_currently_syncing_repo was not called
        self.assertFalse(mock_update_curr_sync.called)

# Projects parent and child streams were deprecated by Github. Test commented out 07/21/25
# @mock.patch("singer.write_schema")
# class TestWriteSchemas(unittest.TestCase):

#     mock_catalog = {"streams": [
#         get_stream_catalog("projects"),
#         get_stream_catalog("project_columns"),
#         get_stream_catalog("project_cards")
#     ]}

#     def test_parents_selected(self, mock_write_schema):
#         write_schemas("projects", self.mock_catalog, ["projects"])
#         mock_write_schema.assert_called_with("projects", mock.ANY, mock.ANY)

#     def test_mid_child_selected(self, mock_write_schema):
#         write_schemas("project_columns", self.mock_catalog, ["project_columns"])
#         mock_write_schema.assert_called_with("project_columns", mock.ANY, mock.ANY)

#     def test_nested_child_selected(self, mock_write_schema):
#         write_schemas("project_cards", self.mock_catalog, ["project_cards"])
#         mock_write_schema.assert_called_with("project_cards", mock.ANY, mock.ANY)


class TestTranslateState(unittest.TestCase):
    """Tests for `translate_state`

    There are many combinations of test cases due to:
    - 2 versions of the state structure
      - "Old style" repo-stream
      - "New style" stream-repo
    - 2 possibilities of a stream being in/not-in catalog
      - stream in catalog
      - stream not in catalog
    - 2 possibilities of a repo being in/not-in state
      - repo in state
      - repo not in state
    """

    def test_repo_stream_state_is_translated(self):
        state = {
            "bookmarks": {
                "singer-io/tap-adwords": {
                    "commits": {
                        "since": "2018-11-14T13:21:20.700360Z"
                        }
                    },
                "singer-io/tap-salesforce": {
                    "commits": {
                        "since": "2018-11-14T13:21:20.700360Z"
                    }
                }
            }
        }

        catalog = {"streams": [{"tap_stream_id": "commits"}]}
        repos = ["singer-io/tap-adwords"]

        actual = translate_state(state, catalog, repos)
        expected = {
            "bookmarks": {
                "commits": {
                    "singer-io/tap-adwords": {
                        "since": "2018-11-14T13:21:20.700360Z"
                    },
                    "singer-io/tap-salesforce": {
                        "since": "2018-11-14T13:21:20.700360Z"
                    }
                }
            }
        }

        assert actual == expected

    def test_stream_repo_state_is_not_translated(self):
        state = {
            "bookmarks": {
                "commits": {
                    "singer-io/tap-adwords": {
                        "since": "2018-11-14T13:21:20.700360Z"
                    },
                    "singer-io/tap-salesforce": {
                        "since": "2018-11-14T13:21:20.700360Z"
                    }
                }
            }
        }
        catalog = {"streams": [{"tap_stream_id": "commits"}]}
        repos = ["singer-io/tap-adwords"]

        actual = translate_state(state, catalog, repos)
        expected = {
            "bookmarks": {
                "commits": {
                    "singer-io/tap-adwords": {
                        "since": "2018-11-14T13:21:20.700360Z"
                    },
                    "singer-io/tap-salesforce": {
                        "since": "2018-11-14T13:21:20.700360Z"
                    }
                }
            }
        }

        assert actual == expected

    def test_stream_repo_state_and_not_selected_is_not_translated(self):
        state = {
            "bookmarks": {
                "commits": {
                    "singer-io/tap-adwords": {
                        "since": "2018-11-14T13:21:20.700360Z"
                    },
                    "singer-io/tap-salesforce": {
                        "since": "2018-11-14T13:21:20.700360Z"
                    }
                }
            }
        }
        catalog = {"streams": [{"tap_stream_id": "issues"}]}
        repos = ["singer-io/tap-adwords"]

        actual = translate_state(state, catalog, repos)
        expected = {
            "bookmarks": {
                "commits": {
                    "singer-io/tap-adwords": {
                        "since": "2018-11-14T13:21:20.700360Z"
                    },
                    "singer-io/tap-salesforce": {
                        "since": "2018-11-14T13:21:20.700360Z"
                    }
                }
            }
        }
        assert actual == expected

    def test_real_sceanario(self):
        state = {
            "bookmarks": {
                "issue_events": {
                    "singer-io/tap-github": {
                        "since": "2025-09-24T13:50:18Z"
                    }
                }
            }
        }

        catalog = {"streams": [{"tap_stream_id": "commits"}]}
        repos = ["singer-io/tap-github"]
        actual = translate_state(state, catalog, repos)
        expected = {
            "bookmarks": {
                "issue_events": {
                    "singer-io/tap-github": {
                        "since": "2025-09-24T13:50:18Z"
                    }
                }
            }
        }
        assert actual == expected

    def test__old_style__stream_in_catalog__repo_in_state(self):
        """
        We have a bookmark and know that the repo is in the wrong layer
        and the stream is in the wrong layer. This means we should
        translate the shape
        """
        state = {
            "bookmarks": {
                "singer-io/tap-fake-repo": {
                    "fake_stream": {
                        "since": "2025-09-24T13:50:18Z"
                    }
                }
            }
        }

        catalog = {"streams": [{"tap_stream_id": "fake_stream"}]}
        repos = ["singer-io/tap-fake-repo"]
        actual = translate_state(state, catalog, repos)
        expected = {
            "bookmarks": {
                "fake_stream": {
                    "singer-io/tap-fake-repo": {
                        "since": "2025-09-24T13:50:18Z"
                    }
                }
            }
        }
        assert actual == expected

    def test__old_style__stream_in_catalog__repo_not_in_state(self):
        """
        We have a bookmark and know that the stream is in the wrong
        layer. We have to assume the unknown layer is a repo. This
        means we should translate the shape
        """

        state = {
            "bookmarks": {
                "singer-io/tap-fake-repo-a": {
                    "fake_stream": {
                        "since": "2025-09-24T13:50:18Z"
                    }
                }
            }
        }

        catalog = {"streams": [{"tap_stream_id": "fake_stream"}]}
        repos = ["singer-io/tap-fake-repo-b"]
        actual = translate_state(state, catalog, repos)
        expected = {
            "bookmarks": {
                "fake_stream": {
                    "singer-io/tap-fake-repo-a": {
                        "since": "2025-09-24T13:50:18Z"
                    }
                }
            }
        }
        assert actual == expected

    def test__old_style__stream_not_in_catalog__repo_in_state(self):
        """
        We have a bookmark and know that the repo is in the wrong
        layer. We have to assume the unknown layer is a stream.  This
        means we should translate the shape
        """

        state = {
            "bookmarks": {
                "singer-io/tap-fake-repo": {
                    "fake_stream_a": {
                        "since": "2025-09-24T13:50:18Z"
                    }
                }
            }
        }

        catalog = {"streams": [{"tap_stream_id": "fake_stream_b"}]}
        repos = ["singer-io/tap-fake-repo"]
        actual = translate_state(state, catalog, repos)
        expected = {
            "bookmarks": {
                "fake_stream_a": {
                    "singer-io/tap-fake-repo": {
                        "since": "2025-09-24T13:50:18Z"
                    }
                }
            }
        }
        assert actual == expected

    def test__old_style__stream_not_in_catalog__repo_not_in_state(self):
        """
        We have a bookmark and don't know anything about the two
        layers. This means we should not translate the shape
        """

        state = {
            "bookmarks": {
                "singer-io/tap-fake-repo-a": {
                    "fake_stream_a": {
                        "since": "2025-09-24T13:50:18Z"
                    }
                }
            }
        }

        catalog = {"streams": [{"tap_stream_id": "fake_stream_b"}]}
        repos = ["singer-io/tap-fake-repo-b"]
        actual = translate_state(state, catalog, repos)
        expected = {
            "bookmarks": {
                "singer-io/tap-fake-repo-a": {
                    "fake_stream_a": {
                        "since": "2025-09-24T13:50:18Z"
                    }
                }
            }
        }
        assert actual == expected

    def test__new_style__stream_in_catalog__repo_in_state(self):
        """
        We have a bookmark and know that the repo is in the right
        layer and the stream is in the right layer. This means we
        should not translate the shape
        """

        state = {
            "bookmarks": {
                "fake_stream": {
                    "singer-io/tap-fake-repo": {
                        "since": "2025-09-24T13:50:18Z"
                    }
                }
            }
        }

        catalog = {"streams": [{"tap_stream_id": "fake_stream"}]}
        repos = ["singer-io/tap-fake-repo"]
        actual = translate_state(state, catalog, repos)
        expected = {
            "bookmarks": {
                "fake_stream": {
                    "singer-io/tap-fake-repo": {
                        "since": "2025-09-24T13:50:18Z"
                    }
                }
            }
        }
        assert actual == expected

    def test__new_style___stream_in_catalog__repo_not_state(self):
        """
        We have a bookmark and know that the stream is in the right
        layer. We assume the unknown layer is a repo. This means we
        should not translate the shape
        """

        state = {
            "bookmarks": {
                "fake_stream": {
                    "singer-io/tap-fake-repo-a": {
                        "since": "2025-09-24T13:50:18Z"
                    }
                }
            }
        }

        catalog = {"streams": [{"tap_stream_id": "fake_stream"}]}
        repos = ["singer-io/tap-fake-repo-b"]
        actual = translate_state(state, catalog, repos)
        expected = {
            "bookmarks": {
                "fake_stream": {
                    "singer-io/tap-fake-repo-a": {
                        "since": "2025-09-24T13:50:18Z"
                    }
                }
            }
        }
        assert actual == expected

    def test__new_style__stream_not_in_catalog__repo_in_state(self):
        """
        We have a bookmark and know that the repo is in the right
        layer. We assume the unknown layer is a stream. This means we
        should not translate the shape
        """

        state = {
            "bookmarks": {
                "fake_stream_a": {
                    "singer-io/tap-fake-repo": {
                        "since": "2025-09-24T13:50:18Z"
                    }
                }
            }
        }

        catalog = {"streams": [{"tap_stream_id": "fake_stream_b"}]}
        repos = ["singer-io/tap-fake-repo"]
        actual = translate_state(state, catalog, repos)
        expected = {
            "bookmarks": {
                "fake_stream_a": {
                    "singer-io/tap-fake-repo": {
                        "since": "2025-09-24T13:50:18Z"
                    }
                }
            }
        }
        assert actual == expected

    def test__new_style__stream_not_in_catalog__repo_not_in_state(self):
        """
        We have a bookmark and don't know anything about the two
        layers. This means we should not translate the shape
        """

        state = {
            "bookmarks": {
                "fake_stream_a": {
                    "singer-io/tap-fake-repo-a": {
                        "since": "2025-09-24T13:50:18Z"
                    }
                }
            }
        }

        catalog = {"streams": [{"tap_stream_id": "fake_stream_b"}]}
        repos = ["singer-io/tap-fake-repo-b"]
        actual = translate_state(state, catalog, repos)
        expected = {
            "bookmarks": {
                "fake_stream_a": {
                    "singer-io/tap-fake-repo-a": {
                        "since": "2025-09-24T13:50:18Z"
                    }
                }
            }
        }
        assert actual == expected
