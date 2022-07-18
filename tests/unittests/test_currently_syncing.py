import unittest
from unittest import mock
from tap_github.sync import (update_currently_syncing_repo, update_currently_syncing,
                             get_ordered_stream_list, get_ordered_repos)


class TestGetOrderedStreamList(unittest.TestCase):
    
    """
    Test `get_ordered_stream_list` function to get ordered list od streams
    """
    
    def test_for_interrupted_sync(self):
        """Test if sync was interrupted"""
        expected_list = ['releases', 'review_comments', 'reviews', 'stargazers', 'team_members',
                         'team_memberships', 'teams', 'assignees', 'collaborators', 'comments',
                         'commit_comments', 'commits', 'events', 'issue_events', 'issue_labels',
                         'issue_milestones', 'issues', 'pr_commits', 'project_cards',
                         'project_columns', 'projects', 'pull_requests']
        final_list = get_ordered_stream_list("releases")

        # Verify with expected ordered list of streams
        self.assertEqual(final_list, expected_list)

    def test_for_completed_sync(self):
        """Test if sync was not interrupted"""
        expected_list = ['assignees', 'collaborators', 'comments', 'commit_comments',
                         'commits', 'events', 'issue_events', 'issue_labels', 'issue_milestones',
                         'issues', 'pr_commits', 'project_cards', 'project_columns', 'projects',
                         'pull_requests', 'releases', 'review_comments', 'reviews', 'stargazers',
                         'team_members', 'team_memberships', 'teams']
        final_list = get_ordered_stream_list(None)

        # Verify with expected ordered list of streams
        self.assertEqual(final_list, expected_list)

class TestGetOrderedRepos(unittest.TestCase):

    """
    Test `get_ordered_repos` function to get ordered list repositories
    """

    repo_list = ["org/repo1", "org/repo2", "org/repo3", "org/repo4", "org/repo5"]
    
    def test_for_interupted_sync(self):
        """Test if sync was interrupted"""
        state = {"currently_syncing_repo": "org/repo3"}
        expected_list = ["org/repo3", "org/repo4", "org/repo5", "org/repo1", "org/repo2"]
        final_repo_list = get_ordered_repos(state, self.repo_list)

        # Verify with expected ordered list of repos
        self.assertEqual(final_repo_list, expected_list)
    
    def test_for_completed_sync(self):
        """Test if sync was not interrupted"""
        state = {}
        final_repo_list = get_ordered_repos(state, self.repo_list)

        # Verify with expected ordered list of repos
        self.assertEqual(final_repo_list, self.repo_list)

@mock.patch("tap_github.sync.update_currently_syncing")
class TestUpdateCurrentlySyncingRepo(unittest.TestCase):

    """
    Test `update_currently_syncing_repo` function of sync
    """

    def test_adding_repo(self, mock_currently_syncing):
        """Test for adding currently syncing repo in state"""
        state = {"currently_syncing_repo": None}
        update_currently_syncing_repo(state, "org/test-repo")
 
        # Verify with expected state
        self.assertEqual(state, {"currently_syncing_repo": "org/test-repo"})

    def test_flush_completed_repo(self, mock_currently_syncing):
        """Test for removing currently syncing repo from state"""
        state = {"currently_syncing_repo": "org/test-repo"}
        update_currently_syncing_repo(state, None)
 
        # Verify with expected state
        self.assertEqual(state, {})


class TestUpdateCurrentlySyncing(unittest.TestCase):

    """
    Test `update_currently_syncing` function of sync
    """

    def test_update_syncing_stream(self):
        """Test for adding currently syncing stream in state"""
        state = {"currently_syncing": "assignees"}
        update_currently_syncing(state, "issues")
 
        # Verify with expected state
        self.assertEqual(state, {"currently_syncing": "issues"})

    def test_flush_currently_syncing(self):
        """Test for removing currently syncing stream from state"""
        state = {"currently_syncing": "assignees"}
        update_currently_syncing(state, None)
 
        # Verify with expected state
        self.assertEqual(state, {})
