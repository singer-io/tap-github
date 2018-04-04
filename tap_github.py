import argparse
import os
import json
import requests
import singer
import singer.metrics as metrics

session = requests.Session()
logger = singer.get_logger()

REQUIRED_CONFIG_KEYS = ['access_token', 'repository']

KEY_PROPERTIES = {
    'commits': ['sha'],
    'issues': ['id'],
    'assignees': ['id'],
    'collaborators': ['id'],
    'pull_requests':['id'],
    'stargazers': ['user_id']
}

def authed_get(source, url, headers={}):
    with metrics.http_request_timer(source) as timer:
        session.headers.update(headers)
        resp = session.request(method='get', url=url)
        timer.tags[metrics.Tag.http_status_code] = resp.status_code
        return resp

def authed_get_all_pages(source, url, headers={}):
    while True:
        r = authed_get(source, url, headers)
        yield r
        if 'next' in r.links:
            url = r.links['next']['url']
        else:
            break

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def load_schemas():
    schemas = {}

    for filename in os.listdir(get_abs_path('tap_github')):
        path = get_abs_path('tap_github') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = json.load(file)

    return schemas


def write_metadata(metadata, values, breadcrumb):
    metadata.append(
        {
            'metadata': values,
            'breadcrumb': breadcrumb
        }
    )

def populate_metadata(schema, metadata, breadcrumb, key_properties):

    # if object, recursively populate object's 'properties'
    if 'object' in schema['type']:
        for prop_name, prop_schema in schema['properties'].items():
            prop_breadcrumb = breadcrumb + ['properties', prop_name]
            populate_metadata(prop_schema, metadata, prop_breadcrumb, key_properties)

    # otherwise, mark as available unless a key property, then automatic
    else:
        prop_name = breadcrumb[-1]
        inclusion = 'automatic'
        # for field selection
        #inclusion = 'automatic' if prop_name in key_properties else 'available'
        values = {'inclusion': inclusion}
        write_metadata(metadata, values, breadcrumb)

def get_catalog():
    raw_schemas = load_schemas()
    streams = []

    for schema_name, schema in raw_schemas.items():

        # get metadata for each field
        metadata = []
        populate_metadata(schema, metadata, [], KEY_PROPERTIES[schema_name])

        # create and add catalog entry
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata' : metadata,
            'key_properties': KEY_PROPERTIES[schema_name],
        }
        streams.append(catalog_entry)

    return {'streams': streams}

def do_discover():
    catalog = get_catalog()
    # dump catalog
    print(json.dumps(catalog, indent=2))

def get_all_pull_requests(stream, config, state):
    '''
    https://developer.github.com/v3/pulls/#list-pull-requests
    '''
    repo_path = config['repository']
    with metrics.record_counter('pull_requests') as counter:
        for response in authed_get_all_pages('pull_requests', 'https://api.github.com/repos/{}/pulls?state=all'.format(repo_path)):
            pull_requests = response.json()
            extraction_time = singer.utils.now()
            for pr in pull_requests:
                rec = singer.transform(pr, stream)
                singer.write_record('pull_requests', rec, time_extracted=extraction_time)
                counter.increment()

    return state

def get_all_assignees(stream, config, state):
    '''
    https://developer.github.com/v3/issues/assignees/#list-assignees
    '''
    repo_path = config['repository']
    with metrics.record_counter('assignees') as counter:
        for response in authed_get_all_pages('assignees', 'https://api.github.com/repos/{}/assignees'.format(repo_path)):
            assignees = response.json()
            extraction_time = singer.utils.now()
            for assignee in assignees:
                rec = singer.transform(assignee, stream)
                singer.write_record('assignees', rec, time_extracted=extraction_time)
                counter.increment()

    return state

def get_all_collaborators(stream, config, state):
    '''
    https://developer.github.com/v3/repos/collaborators/#list-collaborators
    '''
    repo_path = config['repository']
    with metrics.record_counter('collaborators') as counter:
        for response in authed_get_all_pages('collaborators', 'https://api.github.com/repos/{}/collaborators'.format(repo_path)):
            collaborators = response.json()
            extraction_time = singer.utils.now()
            for collaborator in collaborators:
                rec = singer.transform(collaborator, stream)
                singer.write_record('collaborators', rec, time_extracted=extraction_time)
                counter.increment()

    return state

def get_all_commits(stream, config,  state):
    '''
    https://developer.github.com/v3/repos/commits/#list-commits-on-a-repository
    '''
    repo_path = config['repository']
    if 'commits' in state and state['commits'] is not None:
        query_string = '?since={}'.format(state['commits'])
    else:
        query_string = ''

    latest_commit_time = None

    with metrics.record_counter('commits') as counter:
        for response in authed_get_all_pages('commits', 'https://api.github.com/repos/{}/commits{}'.format(repo_path, query_string)):
            commits = response.json()
            extraction_time = singer.utils.now()
            for commit in commits:
                rec = singer.transform(commit, stream)
                singer.write_record('commits', rec, time_extracted=extraction_time)
                counter.increment()

    return state

def get_all_issues(stream, config,  state):
    '''
    https://developer.github.com/v3/issues/#list-issues-for-a-repository
    '''
    repo_path = config['repository']
    if 'issues' in state and state['issues'] is not None:
        query_string = '&since={}'.format(state['issues'])
    else:
        query_string = ''

    last_issue_time = None
    with metrics.record_counter('issues') as counter:
        for response in authed_get_all_pages('issues', 'https://api.github.com/repos/{}/issues?sort=updated&direction=asc{}'.format(repo_path, query_string)):
            issues = response.json()
            extraction_time = singer.utils.now()
            for issue in issues:
                rec = singer.transform(issue, stream)
                singer.write_record('issues', rec, time_extracted=extraction_time)
                counter.increment()
    return state

def get_all_stargazers(stream, config, state):
    '''
    https://developer.github.com/v3/activity/starring/#list-stargazers
    '''
    repo_path = config['repository']
    if 'stargazers' in state and state['stargazers'] is not None:
        query_string = '&since={}'.format(state['stargazers'])
    else:
        query_string = ''

    stargazers_headers = {'Accept': 'application/vnd.github.v3.star+json'}
    last_stargazer_time = None
    with metrics.record_counter('stargazers') as counter:
        for response in authed_get_all_pages('stargazers', 'https://api.github.com/repos/{}/stargazers?sort=updated&direction=asc{}'.format(repo_path, query_string), stargazers_headers):
            stargazers = response.json()
            extraction_time = singer.utils.now()
            for stargazer in stargazers:
                rec = singer.transform(stargazer, stream)
                rec['user_id'] = rec['user']['id']
                singer.write_record('stargazers', rec, time_extracted=extraction_time)
                counter.increment()

    return state

def get_selected_streams(catalog):
    '''
    Gets selected streams.  Uses metadata, looking for an empty
    breadcrumb and mdata with a 'selected' entry
    '''
    selected_streams = []
    for stream in catalog['streams']:
        stream_metadata = stream['metadata']
        for entry in stream_metadata:
            # stream metadata will have empty breadcrumb
            if not entry['breadcrumb'] and entry['metadata'].get('selected',None):
                selected_streams.append(stream['tap_stream_id'])

    return selected_streams



SYNC_FUNCTIONS = {
    'commits': get_all_commits,
    'issues': get_all_issues,
    'assignees': get_all_assignees,
    'collaborators': get_all_collaborators,
    'pull_requests': get_all_pull_requests,
    'stargazers': get_all_stargazers
}

def do_sync(config, state, catalog):
    access_token = config['access_token']
    session.headers.update({'authorization': 'token ' + access_token})

    # get selected streams
    selected_stream_ids = get_selected_streams(catalog)

    # loop over streams
    for catalog_entry in catalog['streams']:
        stream_id = catalog_entry['tap_stream_id']
        stream_schema = catalog_entry['schema']

        # if stream is selected, write schema and sync
        if stream_id in selected_stream_ids:
            singer.write_schema(
                stream_id,
                catalog_entry['schema'],
                catalog_entry['key_properties']
            )
            sync_func = SYNC_FUNCTIONS[stream_id]
            sync_func(stream_schema, config, state)

    singer.write_state(state)

@singer.utils.handle_top_exception(logger)
def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.discover:
        do_discover()
    else:
        catalog = args.properties if args.properties else get_catalog()
        do_sync(args.config, args.state, catalog)

if __name__ == '__main__':
    main()
