import collections
import singer
from singer import bookmarks
from tap_github.streams import STREAMS

LOGGER = singer.get_logger()
STREAM_TO_SYNC_FOR_ORGS = ['teams', 'team_members', 'team_memberships']

def get_selected_streams(catalog):
    '''
    Gets selected streams.  Checks schema's 'selected'
    first -- and then checks metadata, looking for an empty
    breadcrumb and mdata with a 'selected' entry
    '''
    selected_streams = []
    for stream in catalog['streams']:
        stream_metadata = stream['metadata']
        for entry in stream_metadata:
            # Stream metadata will have an empty breadcrumb
            if not entry['breadcrumb'] and entry['metadata'].get('selected',None):
                selected_streams.append(stream['tap_stream_id'])

    return selected_streams

def update_currently_syncing(state, stream_name):
    """
    Updates currently syncing stream in the state.
    """
    if not stream_name and singer.get_currently_syncing(state):
        del state['currently_syncing']
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)

def update_currently_syncing_repo(state, repo_path):
    """
     Updates currently syncing repository in the state.
    and flushes `currently_syncing_repo` when all repositories are synced.
    """
    if (not repo_path) and ('currently_syncing_repo' in state):
        del state['currently_syncing_repo']
    else:
        state['currently_syncing_repo'] = repo_path
    singer.write_state(state)

def get_ordered_stream_list(currently_syncing, streams_to_sync):
    """
    Get an ordered list of remaining streams to sync other streams followed by synced streams.
    """
    stream_list = list(sorted(streams_to_sync))
    if currently_syncing in stream_list:
        index = stream_list.index(currently_syncing)
        stream_list = stream_list[index:] + stream_list[:index]
    return stream_list

def get_ordered_repos(state, repositories):
    """Get an ordered list of remaining repos to sync followed by synced repos.

    The tap supports multiple repos, this is the previous format
    of bookmarks in state, which has the stream keys under the repo:


    In qcdi the stream keys need to be after bookmarks, for standardized
    table level resets to occur. so, this function should be called at the
    beginning of each run to ensure the state is translated to the new
    format:

    """
    syncing_repo = state.get("currently_syncing_repo")
    if syncing_repo in repositories:
        index = repositories.index(syncing_repo)
        repositories = repositories[index:] + repositories[:index]
    return repositories

def translate_state(state, catalog, repositories):
    '''
    The tap supports multiple repos. This was the previous format
    of bookmarks in state, which has the stream keys under the repo:
    {
      "bookmarks": {
        "singer-io/tap-adwords": {
          "commits": {
            "since": "2018-11-14T13:21:20.700360Z"
          }
        }
        "singer-io/tap-salesforce": {
          "commits": {
            "since": "2018-11-14T13:21:20.700360Z"
          }
        }
      }
    }
    In qcdi the stream keys need to be after bookmarks for standardized
    table level resets to occur. This function should be called at the
    beginning of each run to ensure the state is translated to the new
    format:
    {
      "bookmarks": {
        "commits" : {
          "singer-io/tap-adwords": {
            "since": "2018-11-14T13:21:20.700360Z"
          },
          "singer-io/tap-salesforce": {
            "since": "2018-11-14T13:21:20.700360Z"
          }
        },
        "issues" : {
          "singer-io/tap-adwords": {
            "since": "2018-11-14T13:21:20.700360Z"
          },
          "singer-io/tap-salesforce": {
            "since": "2018-11-14T13:21:20.700360Z"
          }
        }
      }
    }

    '''
    nested_dict = lambda: collections.defaultdict(nested_dict)
    new_state = nested_dict()

    # Collect keys(stream_name for update state or repo_name for older state) from state available in the `bookmarks``
    previous_state_keys = state.get('bookmarks', {}).keys()
    # Collect stream names from the catalog
    stream_names = [stream['tap_stream_id'] for stream in catalog['streams']]

    for key in previous_state_keys:
        # Loop through each key of `bookmarks` available in the previous state.

        # Case 1:
        # Older connections `bookmarks` contain stream names so check if it is the stream name or not.
        # If the previous state's key is found in the stream name list then continue to check other keys. Because we want
        # to migrate each stream's bookmark into the repo name as mentioned below:
        # Example: {`bookmarks`: {`stream_a`: `bookmark_a`}} to {`bookmarks`: {`repo_a`: {`stream_a`: `bookmark_a`}}}

        # Case 2:
        # Check if the key is available in the list of currently selected repo's list or not. Newer format `bookmarks` contain repo names.
        # Return the state if the previous state's key is not found in the repo name list or stream name list.

        # If the state contains a bookmark for `repo_a` and `repo_b` and the user deselects these both repos and adds another repo
        # then in that case this function was returning an empty state. Now this change will return the existing state instead of the empty state.

        # old state
        # {
        #     "bookmarks": {
        #         "org/test-repo3": {
        #             "comments": {"since": "2019-01-01T00:00:00Z"}
        #          }
        #     }
        # }
        # for each repo, check each stream under the repo. If the stream is not in stream names or repositories return state.
        # stream should always be in stream_names

        # new state
        # {
        #     "bookmarks": {
        #         "comments" : {
        #             "org/test-repo3": {"since": "2019-01-01T00:00:00Z"},
        #         },
        #     }
        # }
        # for each stream, loop over repos in stream. If the repo is not a stream name (it wont be) or is not is the list of repos, reutrn state. This could happen, and is the case we are checking for. If the repositories are not selected, new ones will get added the new bookmark way.

        for inner_key in state['bookmarks'][key].keys():
            if inner_key not in stream_names and inner_key not in repositories:
                # Return the existing state if all repos from the previous state are deselected(not found) in the current sync.
                return state

    for stream in catalog['streams']:
        stream_name = stream['tap_stream_id']
        for repo in repositories:
            if bookmarks.get_bookmark(state, stream_name, repo):
                return state
            if bookmarks.get_bookmark(state, repo, stream_name):
                new_state['bookmarks'][stream_name][repo] = bookmarks.get_bookmark(state, repo, stream_name)

    return new_state

def get_stream_to_sync(catalog):
    """
    Get the streams for which the sync function should be called(the parent in case of selected child streams).
    """
    streams_to_sync = []
    selected_streams = get_selected_streams(catalog)
    for stream_name, stream_obj in STREAMS.items():
        if stream_name in selected_streams or is_any_child_selected(stream_obj, selected_streams):
            # Append the selected stream or deselected parent stream into the list, if its child or nested child is selected.
            streams_to_sync.append(stream_name)
    return streams_to_sync

def is_any_child_selected(stream_obj,selected_streams):
    """
    Check if any of the child streams is selected for the parent.
    """
    if stream_obj.children:
        for child in stream_obj.children:
            if child in selected_streams:
                return True

            if STREAMS[child].children:
                return is_any_child_selected(STREAMS[child], selected_streams)
    return False

def write_schemas(stream_id, catalog, selected_streams):
    """
    Write the schemas for each stream.
    """
    stream_obj = STREAMS[stream_id]()

    if stream_id in selected_streams:
        # Get catalog object for particular stream.
        stream = [cat for cat in catalog['streams'] if cat['tap_stream_id'] == stream_id ][0]
        singer.write_schema(stream_id, stream['schema'], stream['key_properties'])

    for child in stream_obj.children:
        write_schemas(child, catalog, selected_streams)

def sync(client, config, state, catalog):
    """
    Sync selected streams.
    """

    start_date = config['start_date']

    # Get selected streams, make sure stream dependencies are met
    selected_stream_ids = get_selected_streams(catalog)

    streams_to_sync = get_stream_to_sync(catalog)
    LOGGER.info('Sync stream %s', streams_to_sync)

    repositories, organizations = client.extract_repos_from_config()

    state = translate_state(state, catalog, repositories)
    singer.write_state(state)

    # Sync `teams`, `team_members`and `team_memberships` streams just single time for any organization.
    streams_to_sync_for_orgs = set(streams_to_sync).intersection(STREAM_TO_SYNC_FOR_ORGS)
    # Loop through all organizations
    if selected_stream_ids:
        for orgs in organizations:
            LOGGER.info("Starting sync of organization: %s", orgs)
            do_sync(catalog, streams_to_sync_for_orgs, selected_stream_ids, client, start_date, state, orgs)

        # Sync other streams for all repos
        streams_to_sync_for_repos = set(streams_to_sync) - streams_to_sync_for_orgs
        # pylint: disable=too-many-nested-blocks
        # Sync repositories only if any streams are selected
        for repo in get_ordered_repos(state, repositories):
            update_currently_syncing_repo(state, repo)
            LOGGER.info("Starting sync of repository: %s", repo)
            do_sync(catalog, streams_to_sync_for_repos, selected_stream_ids, client, start_date, state, repo)

            if client.not_accessible_repos:
                # Give warning messages for a repo that is not accessible by a stream or is invalid.
                message = "Please check the repository name \'{}\' or you do not have sufficient permissions to access this repository for following streams {}.".format(repo, ", ".join(client.not_accessible_repos))
                LOGGER.warning(message)
                client.not_accessible_repos = set()
        update_currently_syncing_repo(state, None)

def do_sync(catalog, streams_to_sync, selected_stream_ids, client, start_date, state, repo):
    """
    Sync all other streams except teams, team_members and team_memberships for each repo.
    """
    currently_syncing = singer.get_currently_syncing(state)
    for stream_id in get_ordered_stream_list(currently_syncing, streams_to_sync):
        stream_obj = STREAMS[stream_id]()

        # If it is a "sub_stream", it will be synced as part of the parent stream
        if stream_id in streams_to_sync and not stream_obj.parent:
            write_schemas(stream_id, catalog, selected_stream_ids)
            update_currently_syncing(state, stream_id)

            state = stream_obj.sync_endpoint(client = client,
                                              state = state,
                                              catalog = catalog['streams'],
                                              repo_path = repo,
                                              start_date = start_date,
                                              selected_stream_ids = selected_stream_ids,
                                              stream_to_sync = streams_to_sync
                                            )

            singer.write_state(state)
        update_currently_syncing(state, None)
