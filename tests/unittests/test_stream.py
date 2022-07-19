import unittest
from unittest import mock
from tap_github.streams import Comments, ProjectColumns, Projects, Reviews, TeamMemberships, Teams, PullRequests, get_schema, get_child_full_url, get_bookmark


class TestGetSchema(unittest.TestCase):
    """
    Test `get_schema` method of stream class
    """

    def test_get_schema(self):
        """Verify function returns expected schema"""
        catalog = [
            {"tap_stream_id": "projects"},
            {"tap_stream_id": "comments"},
            {"tap_stream_id": "events"},
        ]
        expected_schema = {"tap_stream_id": "comments"}
        
        # Verify returned schema is same as exected schema 
        self.assertEqual(get_schema(catalog, "comments"), expected_schema)


class TestGetBookmark(unittest.TestCase):
    """
    Test `get_bookmark` method
    """

    test_stream = Comments()
    
    def test_with_out_repo_path(self):
        """
        Test if state does not contains repo path
        """
        state = {
            "bookmarks": {
                "projects": {"since": "2022-01-01T00:00:00Z"}
            }
        }
        returned_bookmark = get_bookmark(state, "org/test-repo", "projects", "since", "2021-01-01T00:00:00Z")
        self.assertEqual(returned_bookmark, "2021-01-01T00:00:00Z")
        
    def test_with_repo_path(self):
        """
        Test if state does contains repo path
        """
        state = {
            "bookmarks": {
                "org/test-repo": {
                    "projects": {"since": "2022-01-01T00:00:00Z"}
                }
            }
        }
        returned_bookmark = get_bookmark(state, "org/test-repo", "projects", "since", "2021-01-01T00:00:00Z")
        self.assertEqual(returned_bookmark, "2022-01-01T00:00:00Z")


class TestBuildUrl(unittest.TestCase):
    """
    Test `build_url` method of stream class
    """
    
    def test_stream_with_filter_params(self):
        """
        Test for stream with filter param
        """
        test_streams = Comments()
        expected_url = "https://api.github.com/repos/org/test-repo/issues/comments?sort=updated&direction=desc?since=2022-01-01T00:00:00Z"
        full_url = test_streams.build_url("org/test-repo", "2022-01-01T00:00:00Z")

        # verify returned url is expected
        self.assertEqual(expected_url, full_url)

    def test_stream_with_organization(self):
        """
        Test for stream that uses organization
        """
        test_streams = Teams()
        expected_url = "https://api.github.com/orgs/org/teams"
        full_url = test_streams.build_url("org", "2022-01-01T00:00:00Z")

        # verify returned url is expected
        self.assertEqual(expected_url, full_url)


class GetMinBookmark(unittest.TestCase):
    """
    Test `get_min_bookmark` method of stream class
    """

    state = {
        "bookmarks": {
            "org/test-repo": {
                "projects": {"since": "2022-03-29T00:00:00Z"},
                "project_columns": {"since": "2022-03-01T00:00:00Z"},
                "project_cards": {"since": "2022-03-14T00:00:00Z"},
                "pull_requests": {"since": "2022-04-01T00:00:00Z"},
                "review_comments": {"since": "2022-03-01T00:00:00Z"},
                "pr_commits": {"since": "2022-02-01T00:00:00Z"},
                "reviews": {"since": "2022-05-01T00:00:00Z"}
            }
        }
    }

    def test_multiple_children(self):
        """
        Test for stream with multiple children
        """
        test_stream = PullRequests()
        bookmark = test_stream.get_min_bookmark("pull_requests", ["pull_requests","review_comments", "pr_commits"],
                                     "2022-04-01T00:00:00Z", "org/test-repo", "2020-04-01T00:00:00Z", self.state)

        # Verify returned bookmark is expected
        self.assertEqual(bookmark, "2022-02-01T00:00:00Z")
    
    def test_children_with_only_parent_selected(self):
        """
        Test for stream with multiple children and only parent is selected
        """
        test_stream = PullRequests()
        bookmark = test_stream.get_min_bookmark("pull_requests", ["pull_requests"],
                                     "2022-04-01T00:00:00Z", "org/test-repo", "2020-04-01T00:00:00Z", self.state)

        # Verify returned bookmark is expected
        self.assertEqual(bookmark, "2022-04-01T00:00:00Z")
    
    def test_for_mid_child_in_stream(self):
        """
        Test for stream with multiple children and mid_child is selected
        """
        test_stream = Projects()
        bookmark = test_stream.get_min_bookmark("projects", ["projects", "project_columns"],
                                     "2022-03-29T00:00:00Z", "org/test-repo", "2020-04-01T00:00:00Z", self.state)

        # Verify returned bookmark is expected
        self.assertEqual(bookmark, "2022-03-01T00:00:00Z")
    
    def test_nested_child_bookmark(self):
        """
        Test for stream with multiple children and nested child is selected
        """
        test_stream = PullRequests()
        bookmark = test_stream.get_min_bookmark("projects", ["projects", "project_cards"],
                                     "2022-03-29T00:00:00Z", "org/test-repo", "2020-04-01T00:00:00Z", self.state)

        # Verify returned bookmark is expected
        self.assertEqual(bookmark, "2022-03-14T00:00:00Z")


@mock.patch("singer.write_bookmark")
class TestWriteBookmark(unittest.TestCase):
    """
    Test `write_bookmarks` method of stream class
    """

    state = {
        "bookmarks": {
            "org/test-repo": {
                "projects": {"since": "2021-03-29T00:00:00Z"},
                "project_columns": {"since": "2021-03-01T00:00:00Z"},
                "project_cards": {"since": "2021-03-14T00:00:00Z"},
                "pull_requests": {"since": "2021-04-01T00:00:00Z"},
                "review_comments": {"since": "2021-03-01T00:00:00Z"},
                "pr_commits": {"since": "2021-02-01T00:00:00Z"},
                "reviews": {"since": "2021-05-01T00:00:00Z"}
            }
        }
    }

    def test_multiple_child(self, mock_write_bookmark):
        """
        Test for stream with multiple children is selected
        """
        test_stream = PullRequests()
        test_stream.write_bookmarks("pull_requests", ["pull_requests","review_comments", "pr_commits"],
                                     "2022-04-01T00:00:00Z", "org/test-repo", self.state)

        expected_calls = [
            mock.call(mock.ANY, mock.ANY, "pull_requests", {"since": "2022-04-01T00:00:00Z"}),
            mock.call(mock.ANY, mock.ANY, "pr_commits", {"since": "2022-04-01T00:00:00Z"}),
            mock.call(mock.ANY, mock.ANY, "review_comments", {"since": "2022-04-01T00:00:00Z"}),
        ]

        # Verify `write_bookmark` is called for all selected streams 
        self.assertEqual(mock_write_bookmark.call_count, 3)

        self.assertIn(mock_write_bookmark.mock_calls[0], expected_calls)
        self.assertIn(mock_write_bookmark.mock_calls[1], expected_calls)
        self.assertIn(mock_write_bookmark.mock_calls[2], expected_calls)

    def test_nested_child(self, mock_write_bookmark):
        """
        Test for stream if nested child is selected
        """
        test_stream = Projects()
        test_stream.write_bookmarks("projects", ["project_cards"],
                                     "2022-04-01T00:00:00Z", "org/test-repo", self.state)

        # Verify `write_bookmark` is called for all selected streams 
        self.assertEqual(mock_write_bookmark.call_count, 1)
        mock_write_bookmark.assert_called_with(mock.ANY, mock.ANY, 
                                               "project_cards", {"since": "2022-04-01T00:00:00Z"})


class TestGetChildUrl(unittest.TestCase):
    """
    Test `get_child_full_url` method of stream class
    """

    def test_child_stream(self):
        """
        Test for stream with one child
        """
        child_stream = ProjectColumns()
        expected_url = "https://api.github.com/projects/1309875/columns"
        full_url = get_child_full_url(child_stream, "org1/test-repo",
                                                       None, (1309875,))
        self.assertEqual(expected_url, full_url)

    def test_child_is_repository(self):
        """
        Test for child stream with reposatory
        """
        child_stream = Reviews()
        expected_url = "https://api.github.com/repos/org1/test-repo/pulls/11/reviews"
        full_url = get_child_full_url(child_stream, "org1/test-repo",
                                                       (11,), None)
        self.assertEqual(expected_url, full_url)

    def test_child_is_organization(self):
        """
        Test for child stream with organization
        """
        child_stream = TeamMemberships()
        expected_url = "https://api.github.com/orgs/org1/teams/dev-team/memberships/demo-user-1"
        full_url = get_child_full_url(child_stream, "org1/test-repo",
                                                       ("dev-team",), ("demo-user-1",))
        self.assertEqual(expected_url, full_url)
