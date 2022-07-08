from tap_tester import runner, connections
from base import TestGithubBase

class GithubParentChildIndependentTest(TestGithubBase):

    def name(self):
        return "tap_tester_github_parent_child_test"

    def test_first_level_child_streams(self):
        """
            Test case to verify that tap is working fine if only first level child streams are selected
        """
        # select first_level_child_streams only and run test
        first_level_child_streams = {"commits", "comments", "issues", "assignees", "collaborators", "pull_requests", "releases", "stargazers", "events", "issue_events", "issue_milestones", "issue_labels", "projects", "commit_comments", "teams"}
        self.run_test(first_level_child_streams)

    def test_second_level_child_streams(self):
        """
            Test case to verify that tap is working fine if only second level child streams are selected
        """
        # select second_level_child_streams only and run test
        second_level_child_streams = {"team_members", "project_cards", "reviews", "review_comments", "pr_commits"}
    
    def test_third_level_child_streams(self):
        """
            Test case to verify that tap is working fine if only third level child streams are selected
        """
        # select third_level_child_streams only and run test
        third_level_child_streams = {"team_memberships", "project_columns"}
        
    def run_test(self, child_streams):
        """
            Testing that tap is working fine if only child streams are selected
            - Verify that if only child streams are selected then only child stream are replicated.
        """
        # instantiate connection
        conn_id = connections.ensure_connection(self)

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs = [catalog for catalog in found_catalogs
                         if catalog.get('stream_name') in child_streams]

        self.perform_and_verify_table_and_field_selection(conn_id, test_catalogs)

        # run initial sync
        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(child_streams, synced_stream_names)