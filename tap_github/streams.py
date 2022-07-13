import copy
from datetime import datetime
import singer
from singer import (metrics, bookmarks, metadata)

LOGGER = singer.get_logger()

def get_bookmark(state, repo, stream_name, bookmark_key, start_date):
    """
    Return bookmark value if available in the state otherwise return start date
    """
    repo_stream_dict = bookmarks.get_bookmark(state, repo, stream_name)
    if repo_stream_dict:
        return repo_stream_dict.get(bookmark_key)

    return start_date

def get_schema(catalog, stream_id):
    """
    Return catalog of the specified stream.
    """
    stream_catalog = [cat for cat in catalog if cat['tap_stream_id'] == stream_id ][0]
    return stream_catalog

def get_child_full_url(child_object, repo_path, parent_id, grand_parent_id):
    """
    Build the child stream's URL based on the parent and the grandparent's ids.
    """

    if child_object.is_repository:
        # The `is_repository` represents that the url contains /repos and the repository name.
        full_url = '{}/repos/{}/{}'.format(
            child_object.url,
            repo_path,
            child_object.path).format(*parent_id)

    elif child_object.is_organization:
        # The `is_organization` represents that the url contains the organization name.
        org = repo_path.split('/')[0]
        full_url = '{}/{}'.format(
            child_object.url,
            child_object.path).format(org, *parent_id, *grand_parent_id)

    else:
        full_url = '{}/{}'.format(
            child_object.url,
            child_object.path).format(*grand_parent_id)
    LOGGER.info(full_url)

    return full_url


class Stream:
    tap_stream_id = None
    replication_method = None
    replication_keys = None
    key_properties = []
    path = None
    filter_param = False
    id_keys = []
    is_organization = False
    children = []
    is_repository = False
    headers = None
    parent = None
    url = "https://api.github.com"

    def add_fields_at_1st_level(self, rec, parent_record):
        pass

    def build_url(self, repo_path, bookmark):
        """
        Build the full url with parameters and attributes.
        """
        if self.filter_param:
            # Add the since parameter for incremental streams
            query_string = '?since={}'.format(bookmark)
        else:
            query_string = ''

        if self.is_organization:
            org = repo_path.split('/')[0]
            full_url = '{}/{}'.format(
                self.url,
                self.path).format(org)
        else:
            full_url = '{}/repos/{}/{}{}'.format(
                self.url,
                repo_path,
                self.path,
                query_string)
        LOGGER.info(full_url)

        return full_url

    def get_min_bookmark(self, stream, selected_streams, bookmark, repo_path, start_date, state):
        """
        Get the minimum bookmark from the parent and its corresponding child bookmarks.
        """

        stream_obj = STREAMS[stream]()
        min_bookmark = bookmark
        if stream in selected_streams:
            min_bookmark = min(min_bookmark, get_bookmark(state, repo_path, stream, "since", start_date))

        for child in stream_obj.children:
            min_bookmark = min(min_bookmark, self.get_min_bookmark(child, selected_streams, min_bookmark, repo_path, start_date, state))

        return min_bookmark

    def write_bookmarks(self, stream, selected_streams, bookmark_value, repo_path, state):
        """Write the bookmark in the state corresponding to the stream."""
        stream_obj = STREAMS[stream]()

        # If the stream is selected, write the bookmark.
        if stream in selected_streams:
            singer.write_bookmark(state, repo_path, stream_obj.tap_stream_id, {"since": bookmark_value})

        # For the each child, write the bookmark if it is selected.
        for child in stream_obj.children:
            self.write_bookmarks(child, selected_streams, bookmark_value, repo_path, state)

    # pylint: disable=no-self-use
    def get_child_records(self,
                          client,
                          catalog,
                          child_stream,
                          grand_parent_id,
                          repo_path,
                          state,
                          start_date,
                          bookmark_dttm,
                          stream_to_sync,
                          selected_stream_ids,
                          parent_id = None,
                          parent_record = None):
        """
        Retrieve and write all the child records for each updated parent based on the parent record and its ids.
        """
        child_object = STREAMS[child_stream]()

        if not parent_id:
            parent_id = grand_parent_id

        full_url = get_child_full_url(child_object, repo_path, parent_id, grand_parent_id)
        stream_catalog = get_schema(catalog, child_object.tap_stream_id)

        with metrics.record_counter(child_object.tap_stream_id) as counter:
            for response in client.authed_get_all_pages(
                child_object.tap_stream_id,
                full_url
            ):
                records = response.json()
                extraction_time = singer.utils.now()

                if isinstance(records, list):
                    # Loop through all the records
                    for record in records:
                        record['_sdc_repository'] = repo_path
                        child_object.add_fields_at_1st_level(record, parent_record)

                        # Loop thru each child and nested child in the parent and fetch all the child records.
                        for nested_child in child_object.children:
                            if nested_child in stream_to_sync:
                                child_id = tuple(record.get(key) for key in STREAMS[nested_child]().id_keys)
                                child_object.get_child_records(client, catalog, nested_child, child_id, repo_path, state, start_date, bookmark_dttm, stream_to_sync, selected_stream_ids, grand_parent_id, record)

                        with singer.Transformer() as transformer:

                            rec = transformer.transform(record, stream_catalog['schema'], metadata=metadata.to_map(stream_catalog['metadata']))

                            if child_object.tap_stream_id in selected_stream_ids:
                                singer.write_record(child_object.tap_stream_id, rec, time_extracted=extraction_time)
                                counter.increment()

                else:
                    records['_sdc_repository'] = repo_path
                    child_object.add_fields_at_1st_level(records, parent_record)

                    with singer.Transformer() as transformer:

                        rec = transformer.transform(records, stream_catalog['schema'], metadata=metadata.to_map(stream_catalog['metadata']))

                        singer.write_record(child_object.tap_stream_id, rec, time_extracted=extraction_time)

class FullTableStream(Stream):
    def sync_endpoint(self,
                        client,
                        state,
                        catalog,
                        repo_path,
                        start_date,
                        selected_stream_ids,
                        stream_to_sync
                        ):
        """
        A common function sync full table streams and incremental streams.
        """

        # build full url
        full_url = self.build_url(repo_path, None)

        headers = {}
        if self.headers:
            headers = self.headers

        stream_catalog = get_schema(catalog, self.tap_stream_id)

        with metrics.record_counter(self.tap_stream_id) as counter:
            for response in client.authed_get_all_pages(
                    self.tap_stream_id,
                    full_url,
                    headers
            ):
                records = response.json()
                extraction_time = singer.utils.now()
                # Loop through all records
                for record in records:

                    record['_sdc_repository'] = repo_path
                    parent_record = copy.copy(record)
                    self.add_fields_at_1st_level(record, {})

                    with singer.Transformer() as transformer:
                        rec = transformer.transform(record, stream_catalog['schema'], metadata=metadata.to_map(stream_catalog['metadata']))
                        if self.tap_stream_id in selected_stream_ids:

                            singer.write_record(self.tap_stream_id, rec, time_extracted=extraction_time)

                            counter.increment()

                    for child in self.children:
                        if child in stream_to_sync:

                            parent_id = tuple(parent_record.get(key) for key in STREAMS[child]().id_keys)

                            self.get_child_records(client,
                                                catalog,
                                                child,
                                                parent_id,
                                                repo_path,
                                                state,
                                                start_date,
                                                parent_record.get(self.replication_keys),
                                                stream_to_sync,
                                                selected_stream_ids,
                                                parent_record = parent_record)

        return state

class IncrementalStream(Stream):
    def sync_endpoint(self,
                      client,
                      state,
                      catalog,
                      repo_path,
                      start_date,
                      selected_stream_ids,
                      stream_to_sync
                      ):

        """
        A common function sync full table streams and incremental streams. Sync an incremental stream for which records are not
        in descending order. For, incremental streams iterate all records, write only newly updated records and
        write the latest bookmark value.
        """

        current_time = datetime.today().strftime('%Y-%m-%dT%H:%M:%SZ')
        min_bookmark_value = self.get_min_bookmark(self.tap_stream_id, selected_stream_ids, current_time, repo_path, start_date, state)
        max_bookmark_value = min_bookmark_value
        # build full url
        full_url = self.build_url(repo_path, min_bookmark_value)

        headers = {}
        if self.headers:
            headers = self.headers

        stream_catalog = get_schema(catalog, self.tap_stream_id)

        with metrics.record_counter(self.tap_stream_id) as counter:
            for response in client.authed_get_all_pages(
                    self.tap_stream_id,
                    full_url,
                    headers
            ):
                records = response.json()
                extraction_time = singer.utils.now()
                # Loop through all records
                for record in records:

                    record['_sdc_repository'] = repo_path
                    self.add_fields_at_1st_level(record, {})
                    parent_record = copy.copy(record)

                    with singer.Transformer() as transformer:
                        if record.get(self.replication_keys):
                            if record[self.replication_keys] >= max_bookmark_value:
                                # Update max_bookmark_value
                                max_bookmark_value = record[self.replication_keys]

                            bookmark_dttm = record[self.replication_keys]

                            # Keep only records whose bookmark is after the last_datetime
                            if bookmark_dttm >= min_bookmark_value:

                                if self.tap_stream_id in selected_stream_ids:
                                    rec = transformer.transform(record, stream_catalog['schema'], metadata=metadata.to_map(stream_catalog['metadata']))

                                    singer.write_record(self.tap_stream_id, rec, time_extracted=extraction_time)
                                    counter.increment()

                                for child in self.children:
                                    if child in stream_to_sync:

                                        parent_id = tuple(parent_record.get(key) for key in STREAMS[child]().id_keys)

                                        self.get_child_records(client,
                                                            catalog,
                                                            child,
                                                            parent_id,
                                                            repo_path,
                                                            state,
                                                            start_date,
                                                            parent_record.get(self.replication_keys),
                                                            stream_to_sync,
                                                            selected_stream_ids,
                                                            parent_record = parent_record)
                        else:
                            LOGGER.warning("Skipping this record for %s stream with %s = %s as it is missing replication key %s.",
                                        self.tap_stream_id, self.key_properties, record[self.key_properties], self.replication_keys)


            # Write bookmark for incremental stream.
            self.write_bookmarks(self.tap_stream_id, selected_stream_ids, max_bookmark_value, repo_path, state)

        return state

class IncrementalOrderedStream(Stream):

    def sync_endpoint(self,
                      client,
                      state,
                      catalog,
                      repo_path,
                      start_date,
                      selected_stream_ids,
                      stream_to_sync
                      ):
        """
        A sync function for streams that have records in the descending order of replication key value. For such streams,
        iterate only the latest records.
        """
        bookmark_value = get_bookmark(state, repo_path, self.tap_stream_id, "since", start_date)
        current_time = datetime.today().strftime('%Y-%m-%dT%H:%M:%SZ')

        max_bookmark_value = self.get_min_bookmark(self.tap_stream_id, selected_stream_ids, current_time, repo_path, start_date, state)
        bookmark_time = singer.utils.strptime_to_utc(max_bookmark_value)

        # Build full url
        full_url = self.build_url(repo_path, bookmark_value)
        synced_all_records = False
        stream_catalog = get_schema(catalog, self.tap_stream_id)

        with metrics.record_counter(self.tap_stream_id) as counter:
            for response in client.authed_get_all_pages(
                    self.tap_stream_id,
                    full_url
            ):
                records = response.json()
                extraction_time = singer.utils.now()
                for record in records:
                    record['_sdc_repository'] = repo_path
                    self.add_fields_at_1st_level(record, {})
                    parent_record = copy.copy(record)

                    updated_at = record.get(self.replication_keys)

                    if counter.value == 0:
                        # Consider replication key value of 1st record as bookmark value.
                        # Because all records are in descending order of replication key value
                        bookmark_value = updated_at

                    if updated_at:
                        if bookmark_time and singer.utils.strptime_to_utc(updated_at) < bookmark_time:
                            # Skip all records from now onwards because the bookmark value of the current record is less than
                            # last saved bookmark value and all records from now onwards will have bookmark value less than last
                            # saved bookmark value.
                            synced_all_records = True
                            break

                        if self.tap_stream_id in selected_stream_ids:

                            # Transform and write record
                            with singer.Transformer() as transformer:
                                rec = transformer.transform(record, stream_catalog['schema'], metadata=metadata.to_map(stream_catalog['metadata']))
                                singer.write_record(self.tap_stream_id, rec, time_extracted=extraction_time)
                                counter.increment()

                        for child in self.children:
                            if child in stream_to_sync:
                                parent_id = tuple(parent_record.get(key) for key in STREAMS[child]().id_keys)

                                self.get_child_records(client,
                                                    catalog,
                                                    child,
                                                    parent_id,
                                                    repo_path,
                                                    state,
                                                    start_date,
                                                    parent_record.get(self.replication_keys),
                                                    stream_to_sync,
                                                    selected_stream_ids,
                                                    parent_record = parent_record)
                    else:
                        LOGGER.warning("Skipping this record for %s stream with %s = %s as it is missing replication key %s.",
                                    self.tap_stream_id, self.key_properties, record[self.key_properties], self.replication_keys)

                if synced_all_records:
                    break

            # Write bookmark for incremental stream.
            self.write_bookmarks(self.tap_stream_id, selected_stream_ids, bookmark_value, repo_path, state)

        return state

class Reviews(IncrementalStream):
    '''
    https://docs.github.com/en/rest/reference/pulls#list-reviews-for-a-pull-request
    '''
    tap_stream_id = "reviews"
    replication_method = "INCREMENTAL"
    replication_keys = "submitted_at"
    key_properties = ["id"]
    path = "pulls/{}/reviews"
    is_repository = True
    id_keys = ['number']
    parent = 'pull_requests'

class ReviewComments(IncrementalOrderedStream):
    '''
    https://docs.github.com/en/rest/reference/pulls#list-review-comments-in-a-repository
    '''
    tap_stream_id = "review_comments"
    replication_method = "INCREMENTAL"
    replication_keys = "updated_at"
    key_properties = ["id"]
    path = "pulls/{}/comments?sort=updated_at&direction=desc"
    is_repository = True
    id_keys = ['number']
    parent = 'pull_requests'

class PRCommits(IncrementalStream):
    '''
    https://docs.github.com/en/rest/reference/pulls#list-commits-on-a-pull-request
    '''
    tap_stream_id = "pr_commits"
    replication_method = "INCREMENTAL"
    replication_keys = "updated_at"
    key_properties = ["id"]
    path = "pulls/{}/commits"
    is_repository = True
    id_keys = ['number']
    parent = 'pull_requests'

    def add_fields_at_1st_level(self, rec, parent_record):
        rec['updated_at'] = rec['commit']['committer']['date']

        rec['pr_number'] = parent_record.get('number')
        rec['pr_id'] = parent_record.get('id')
        rec['id'] = '{}-{}'.format(parent_record.get('id'), rec.get('sha'))

class PullRequests(IncrementalOrderedStream):
    '''
    https://developer.github.com/v3/pulls/#list-pull-requests
    '''
    tap_stream_id = "pull_requests"
    replication_method = "INCREMENTAL"
    replication_keys = "updated_at"
    key_properties = ["id"]
    path = "pulls?state=all"
    children = ['reviews', 'review_comments', 'pr_commits']

class ProjectCards(IncrementalStream):
    '''
    https://docs.github.com/en/rest/reference/projects#list-project-cards
    '''
    tap_stream_id = "project_cards"
    replication_method = "INCREMENTAL"
    replication_keys = "updated_at"
    key_properties = ["id"]
    path = "projects/columns/{}/cards"
    tap_stream_id = "project_cards"
    parent = 'projects'
    id_keys = ['id']

class ProjectColumns(IncrementalStream):
    '''
    https://docs.github.com/en/rest/reference/projects#list-project-columns
    '''
    tap_stream_id = "project_columns"
    replication_method = "INCREMENTAL"
    replication_keys = "updated_at"
    key_properties = ["id"]
    path = "projects/{}/columns"
    children = ["project_cards"]
    parent = "projects"
    id_keys = ['id']
    has_children = True

class Projects(IncrementalStream):
    '''
    https://docs.github.com/en/rest/reference/projects#list-repository-projects
    '''
    tap_stream_id = "projects"
    replication_method = "INCREMENTAL"
    replication_keys = "updated_at"
    key_properties = ["id"]
    path = "projects?state=all"
    tap_stream_id = "projects"
    children = ["project_columns"]
    child_objects = [ProjectColumns()]

class TeamMemberships(FullTableStream):
    '''
    https://docs.github.com/en/rest/reference/teams#get-team-membership-for-a-user
    '''
    tap_stream_id = "team_memberships"
    replication_method = "FULL_TABLE"
    key_properties = ["url"]
    path = "orgs/{}/teams/{}/memberships/{}"
    is_organization = True
    parent = 'teams'
    id_keys = ["login"]

class TeamMembers(FullTableStream):
    '''
    https://docs.github.com/en/rest/reference/teams#list-team-members
    '''
    tap_stream_id = "team_members"
    replication_method = "FULL_TABLE"
    key_properties = ["team_slug", "id"]
    path = "orgs/{}/teams/{}/members"
    is_organization = True
    id_keys = ['slug']
    children= ["team_memberships"]
    has_children = True
    parent = 'teams'

    def add_fields_at_1st_level(self, rec, parent_record):
        rec['team_slug'] = parent_record['slug']

class Teams(FullTableStream):
    '''
    https://docs.github.com/en/rest/reference/teams#list-teams
    '''
    tap_stream_id = "teams"
    replication_method = "FULL_TABLE"
    key_properties = ["id"]
    path = "orgs/{}/teams"
    is_organization = True
    children= ["team_members"]

class Commits(IncrementalStream):
    '''
    https://developer.github.com/v3/repos/commits/#list-commits-on-a-repository
    '''
    tap_stream_id = "commits"
    replication_method = "INCREMENTAL"
    replication_keys = "updated_at"
    key_properties = ["sha"]
    path = "commits"
    filter_param = True

    def add_fields_at_1st_level(self, rec, parent_record):
        rec['updated_at'] = rec['commit']['committer']['date']

class Comments(IncrementalOrderedStream):
    '''
    https://developer.github.com/v3/issues/comments/#list-comments-in-a-repository
    '''
    tap_stream_id = "comments"
    replication_method = "INCREMENTAL"
    replication_keys = "updated_at"
    key_properties = ["id"]
    filter_param = True
    path = "issues/comments?sort=updated&direction=desc"

class Issues(IncrementalOrderedStream):
    '''
    https://developer.github.com/v3/issues/#list-issues-for-a-repository
    '''
    tap_stream_id = "issues"
    replication_method = "INCREMENTAL"
    replication_keys = "updated_at"
    key_properties = ["id"]
    filter_param = True
    path = "issues?state=all&sort=updated&direction=desc"

class Assignees(FullTableStream):
    '''
    https://developer.github.com/v3/issues/assignees/#list-assignees
    '''
    tap_stream_id = "assignees"
    replication_method = "FULL_TABLE"
    key_properties = ["id"]
    path = "assignees"

class Releases(FullTableStream):
    '''
    https://docs.github.com/en/rest/reference/pulls#list-reviews-for-a-pull-request
    '''
    tap_stream_id = "releases"
    replication_method = "FULL_TABLE"
    key_properties = ["id"]
    path = "releases?sort=created_at&direction=desc"

class IssueLabels(FullTableStream):
    '''
    https://developer.github.com/v3/issues/labels/
    '''
    tap_stream_id = "issue_labels"
    replication_method = "FULL_TABLE"
    key_properties = ["id"]
    path = "labels"

class IssueEvents(IncrementalOrderedStream):
    '''
    https://docs.github.com/en/rest/reference/issues#list-issue-events-for-a-repository
    '''
    tap_stream_id = "issue_events"
    replication_method = "INCREMENTAL"
    replication_keys = "created_at"
    key_properties = ["id"]
    path = "issues/events?sort=created_at&direction=desc"

class Events(IncrementalStream):
    '''
    https://developer.github.com/v3/issues/events/#list-events-for-a-repository
    '''
    tap_stream_id = "events"
    replication_method = "INCREMENTAL"
    replication_keys = "created_at"
    key_properties = ["id"]
    path = "events"

class CommitComments(IncrementalStream):
    '''
    https://developer.github.com/v3/repos/comments/
    '''
    tap_stream_id = "commit_comments"
    replication_method = "INCREMENTAL"
    replication_keys = "updated_at"
    key_properties = ["id"]
    path = "comments"

class IssueMilestones(IncrementalOrderedStream):
    '''
    https://developer.github.com/v3/issues/milestones/#list-milestones-for-a-repository
    '''
    tap_stream_id = "issue_milestones"
    replication_method = "INCREMENTAL"
    replication_keys = "updated_at"
    key_properties = ["id"]
    path = "milestones?direction=desc&sort=updated_at"

class Collaborators(FullTableStream):
    '''
    https://developer.github.com/v3/repos/collaborators/#list-collaborators
    '''
    tap_stream_id = "collaborators"
    replication_method = "FULL_TABLE"
    key_properties = ["id"]
    path = "collaborators"

class StarGazers(FullTableStream):
    '''
    https://developer.github.com/v3/activity/starring/#list-stargazers
    '''
    tap_stream_id = "stargazers"
    replication_method = "FULL_TABLE"
    key_properties = ["user_id"]
    path = "stargazers"
    headers = {'Accept': 'application/vnd.github.v3.star+json'}

    def add_fields_at_1st_level(self, rec, parent_record):
        rec['user_id'] = rec['user']['id']


# Dictionary of the stream classes
STREAMS = {
    "commits": Commits,
    "comments": Comments,
    "issues": Issues,
    "assignees": Assignees,
    "releases": Releases,
    "issue_labels": IssueLabels,
    "issue_events": IssueEvents,
    "events": Events,
    "commit_comments": CommitComments,
    "issue_milestones": IssueMilestones,
    "projects": Projects,
    "project_columns": ProjectColumns,
    "project_cards": ProjectCards,
    "pull_requests": PullRequests,
    "reviews": Reviews,
    "review_comments": ReviewComments,
    "pr_commits": PRCommits,
    "teams": Teams,
    "team_members": TeamMembers,
    "team_memberships": TeamMemberships,
    "collaborators": Collaborators,
    "stargazers": StarGazers
}
