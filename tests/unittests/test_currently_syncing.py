import unittest
from unittest import mock
from tap_github.sync import (update_currently_syncing_repo, update_currently_syncing,
                             get_ordered_stream_list, get_ordered_repos)


class TestGetOrderedStreamList(unittest.TestCase):
    
    def test_for_interrupted_sync(self):
        expected_list = ['releases', 'review_comments', 'reviews', 'stargazers', 'team_members',
                         'team_memberships', 'teams', 'assignees', 'collaborators', 'comments',
                         'commit_comments', 'commits', 'events', 'issue_events', 'issue_labels',
                         'issue_milestones', 'issues', 'pr_commits', 'project_cards', 'project_columns',
                         'projects', 'pull_requests']
        final_list = get_ordered_stream_list("releases")
        self.assertEqual(final_list, expected_list)

    def test_for_completed_sync(self):
        expected_list = ['assignees', 'collaborators', 'comments', 'commit_comments',
                         'commits', 'events', 'issue_events', 'issue_labels', 'issue_milestones',
                         'issues', 'pr_commits', 'project_cards', 'project_columns', 'projects',
                         'pull_requests', 'releases', 'review_comments', 'reviews', 'stargazers',
                         'team_members', 'team_memberships', 'teams']
        final_list = get_ordered_stream_list(None)
        self.assertEqual(final_list, expected_list)

class TestGetOrderedRepos(unittest.TestCase):
    
    repo_list = ["org/repo1", "org/repo2", "org/repo3", "org/repo4", "org/repo5"]
    
    def test_for_interupted_sync(self):
        state = {"currently_syncing_repo": "org/repo3"}
        expected_list = ["org/repo3", "org/repo4", "org/repo5", "org/repo1", "org/repo2"]
        final_repo_list = get_ordered_repos(state, self.repo_list)
        self.assertEqual(final_repo_list, expected_list)
    
    def test_for_completed_sync(self):
        state = {}
        final_repo_list = get_ordered_repos(state, self.repo_list)
        self.assertEqual(final_repo_list, self.repo_list)

@mock.patch("tap_github.sync.update_currently_syncing")
class TestUpdateCurrentlySyncingRepo(unittest.TestCase):
    
    def test_appending_repo(self, mock_currently_syncing):
        state = {"currently_syncing_repo": None}
        update_currently_syncing_repo(state, "org/test-repo")
        self.assertEqual(state, {"currently_syncing_repo": "org/test-repo"})
    
    def test_flush_completed_repo(self, mock_currently_syncing):
        state = {"currently_syncing_repo": "org/test-repo"}
        update_currently_syncing_repo(state, None)
        self.assertEqual(state, {})


class TestUpdateCurrentlySyncing(unittest.TestCase):
    
    def test_update_syncing_stream(self):
        state = {"currently_syncing": "assignees"}
        update_currently_syncing(state, "issues")
        self.assertEqual(state, {"currently_syncing": "issues"})

    def test_flush_currently_syncing(self):
        state = {"currently_syncing": "assignees"}
        update_currently_syncing(state, None)
        self.assertEqual(state, {})
