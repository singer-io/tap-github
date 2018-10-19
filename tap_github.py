import argparse
import os
import json
import requests
import singer
import singer.bookmarks as bookmarks
import singer.metrics as metrics

from singer import metadata

session = requests.Session()
logger = singer.get_logger()

REQUIRED_CONFIG_KEYS = ['access_token', 'repository']

KEY_PROPERTIES = {
    'commits': ['sha'],
    'issues': ['id'],
    'assignees': ['id'],
    'collaborators': ['id'],
    'pull_requests':['id'],
    'stargazers': ['user_id'],
    'reviews': ['id'],
    'review_comments': ['id']
}

class AuthException(Exception):
    pass

class NotFoundException(Exception):
    pass

def authed_get(source, url, headers={}):
    with metrics.http_request_timer(source) as timer:
        session.headers.update(headers)
        resp = session.request(method='get', url=url)
        if resp.status_code == 401:
            raise AuthException(resp.text)
        if resp.status_code == 404:
            raise NotFoundException(resp.text)

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

class DependencyException(Exception):
    pass

def validate_dependencies(selected_stream_ids):
    errs = []
    msg_tmpl = ("Unable to extract {0} data. "
                "To receive {0} data, you also need to select {1}.")

    if 'reviews' in selected_stream_ids and 'pull_requests' not in selected_stream_ids:
        errs.append(msg_tmpl.format('reviews','pull_requests'))

    if 'review_comments' in selected_stream_ids and 'pull_requests' not in selected_stream_ids:
        errs.append(msg_tmpl.format('review_comments','pull_requests'))

    if errs:
        raise DependencyException(" ".join(errs))


def write_metadata(metadata, values, breadcrumb):
    metadata.append(
        {
            'metadata': values,
            'breadcrumb': breadcrumb
        }
    )

def populate_metadata(schema_name, schema):
    mdata = metadata.new()
    #mdata = metadata.write(mdata, (), 'forced-replication-method', KEY_PROPERTIES[schema_name])
    mdata = metadata.write(mdata, (), 'table-key-properties', KEY_PROPERTIES[schema_name])

    for field_name in schema['properties'].keys():
        if field_name in KEY_PROPERTIES[schema_name]:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
        else:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

    return mdata

def get_catalog():
    raw_schemas = load_schemas()
    streams = []

    for schema_name, schema in raw_schemas.items():

        # get metadata for each field
        mdata = populate_metadata(schema_name, schema)

        # create and add catalog entry
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata' : metadata.to_list(mdata),
            'key_properties': KEY_PROPERTIES[schema_name],
        }
        streams.append(catalog_entry)

    return {'streams': streams}

def do_discover():
    catalog = get_catalog()
    # dump catalog
    print(json.dumps(catalog, indent=2))

def get_all_pull_requests(schemas, config, state, mdata):
    '''
    https://developer.github.com/v3/pulls/#list-pull-requests
    '''
    repo_path = config['repository']
    with metrics.record_counter('pull_requests') as counter:
        with metrics.record_counter('reviews') as reviews_counter:
            for response in authed_get_all_pages(
                    'pull_requests',
                    'https://api.github.com/repos/{}/pulls?state=all'.format(repo_path)
            ):
                pull_requests = response.json()
                extraction_time = singer.utils.now()
                for pr in pull_requests:
                    pr_num = pr.get('number')

                    # transform and write pull_request record
                    with singer.Transformer() as transformer:
                        rec = transformer.transform(pr, schemas['pull_requests'], metadata=metadata.to_map(mdata))
                    singer.write_record('pull_requests', rec, time_extracted=extraction_time)
                    singer.write_bookmark(state, 'pull_requests', 'since', singer.utils.strftime(extraction_time))
                    counter.increment()

                    # sync reviews if that schema is present (only there if selected)
                    if schemas.get('reviews'):
                        for review_rec in get_reviews_for_pr(pr_num, schemas['reviews'], config, state, mdata):
                            singer.write_record('reviews', review_rec, time_extracted=extraction_time)
                            singer.write_bookmark(state, 'reviews', 'since', singer.utils.strftime(extraction_time))
                            reviews_counter.increment()

                    # sync review comments if that schema is present (only there if selected)
                    if schemas.get('review_comments'):
                        for review_comment_rec in get_review_comments_for_pr(pr_num, schemas['review_comments'], config, state, mdata):
                            singer.write_record('review_comments', review_comment_rec, time_extracted=extraction_time)
                            singer.write_bookmark(state, 'review_comments', 'since', singer.utils.strftime(extraction_time))

    return state

def get_reviews_for_pr(pr_number, schema, config, state, mdata):
    repo_path = config['repository']
    for response in authed_get_all_pages(
            'reviews',
            'https://api.github.com/repos/{}/pulls/{}/reviews'.format(repo_path,pr_number)
    ):
        reviews = response.json()
        extraction_time = singer.utils.now()
        for review in reviews:
            with singer.Transformer() as transformer:
                rec = transformer.transform(review, schema, metadata=metadata.to_map(mdata))
            yield rec


        return state

def get_review_comments_for_pr(pr_number, schema, config, state, mdata):
    repo_path = config['repository']
    for response in authed_get_all_pages(
            'comments',
            'https://api.github.com/repos/{}/pulls/{}/comments'.format(repo_path,pr_number)
    ):
        review_comments = response.json()
        extraction_time = singer.utils.now()
        for comment in review_comments:
            with singer.Transformer() as transformer:
                rec = transformer.transform(comment, schema, metadata=metadata.to_map(mdata))
            yield rec


        return state

def get_all_assignees(schema, config, state, mdata):
    '''
    https://developer.github.com/v3/issues/assignees/#list-assignees
    '''
    repo_path = config['repository']
    with metrics.record_counter('assignees') as counter:
        for response in authed_get_all_pages(
                'assignees',
                'https://api.github.com/repos/{}/assignees'.format(repo_path)
        ):
            assignees = response.json()
            extraction_time = singer.utils.now()
            for assignee in assignees:
                with singer.Transformer() as transformer:
                    rec = transformer.transform(assignee, schema, metadata=metadata.to_map(mdata))
                singer.write_record('assignees', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, 'assignees', 'since', singer.utils.strftime(extraction_time))
                counter.increment()

    return state

def get_all_collaborators(schema, config, state, mdata):
    '''
    https://developer.github.com/v3/repos/collaborators/#list-collaborators
    '''
    repo_path = config['repository']
    with metrics.record_counter('collaborators') as counter:
        for response in authed_get_all_pages(
                'collaborators',
                'https://api.github.com/repos/{}/collaborators'.format(repo_path)
        ):
            collaborators = response.json()
            extraction_time = singer.utils.now()
            for collaborator in collaborators:
                with singer.Transformer() as transformer:
                    rec = transformer.transform(collaborator, schema, metadata=metadata.to_map(mdata))
                singer.write_record('collaborators', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, 'collaborator', 'since', singer.utils.strftime(extraction_time))
                counter.increment()

    return state

def get_all_commits(schema, config,  state, mdata):
    '''
    https://developer.github.com/v3/repos/commits/#list-commits-on-a-repository
    '''
    repo_path = config['repository']
    if bookmarks.get_bookmark(state, "commits", 'since'):
        query_string = '?since={}'.format(bookmarks.get_bookmark(state, "commits", 'since'))
    else:
        query_string = ''

    latest_commit_time = None

    with metrics.record_counter('commits') as counter:
        for response in authed_get_all_pages(
                'commits',
                'https://api.github.com/repos/{}/commits{}'.format(repo_path, query_string)
        ):
            commits = response.json()
            extraction_time = singer.utils.now()
            for commit in commits:
                with singer.Transformer() as transformer:
                    rec = transformer.transform(commit, schema, metadata=metadata.to_map(mdata))
                singer.write_record('commits', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, 'commits', 'since', singer.utils.strftime(extraction_time))
                counter.increment()

    return state

def get_all_issues(schema, config,  state, mdata):
    '''
    https://developer.github.com/v3/issues/#list-issues-for-a-repository
    '''
    repo_path = config['repository']

    if bookmarks.get_bookmark(state, "issues", 'since'):
        query_string = '&since={}'.format(bookmarks.get_bookmark(state, "issues", 'since'))
    else:
        query_string = ''

    last_issue_time = None
    with metrics.record_counter('issues') as counter:
        for response in authed_get_all_pages(
                'issues',
                'https://api.github.com/repos/{}/issues?sort=updated&direction=asc{}'.format(repo_path, query_string)
        ):
            issues = response.json()
            extraction_time = singer.utils.now()
            for issue in issues:
                with singer.Transformer() as transformer:
                    rec = transformer.transform(issue, schema, metadata=metadata.to_map(mdata))
                singer.write_record('issues', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, 'issues', 'since', singer.utils.strftime(extraction_time))
                counter.increment()
    return state

def get_all_stargazers(schema, config, state, mdata):
    '''
    https://developer.github.com/v3/activity/starring/#list-stargazers
    '''
    repo_path = config['repository']
    if bookmarks.get_bookmark(state, "stargazers", 'since'):
        query_string = '&since={}'.format(bookmarks.get_bookmark(state, "stargazers", 'since'))
    else:
        query_string = ''

    stargazers_headers = {'Accept': 'application/vnd.github.v3.star+json'}
    last_stargazer_time = None
    with metrics.record_counter('stargazers') as counter:
        for response in authed_get_all_pages(
                'stargazers',
                'https://api.github.com/repos/{}/stargazers?sort=updated&direction=asc{}'.format(repo_path, query_string), stargazers_headers
        ):
            stargazers = response.json()
            extraction_time = singer.utils.now()
            for stargazer in stargazers:
                with singer.Transformer() as transformer:
                    rec = transformer.transform(stargazer, schema, metadata=metadata.to_map(mdata))
                rec['user_id'] = rec['user']['id']
                singer.write_record('stargazers', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, 'stargazers', 'since', singer.utils.strftime(extraction_time))
                counter.increment()

    return state

def get_selected_streams(catalog):
    '''
    Gets selected streams.  Checks schema's 'selected'
    first -- and then checks metadata, looking for an empty
    breadcrumb and mdata with a 'selected' entry
    '''
    selected_streams = []
    for stream in catalog['streams']:
        stream_metadata = stream['metadata']
        if stream['schema'].get('selected', False):
            selected_streams.append(stream['tap_stream_id'])
        else:
            for entry in stream_metadata:
                # stream metadata will have empty breadcrumb
                if not entry['breadcrumb'] and entry['metadata'].get('selected',None):
                    selected_streams.append(stream['tap_stream_id'])

    return selected_streams

def get_stream_from_catalog(stream_id, catalog):
    for stream in catalog['streams']:
        if stream['tap_stream_id'] == stream_id:
            return stream
    return None

SYNC_FUNCTIONS = {
    'commits': get_all_commits,
    'issues': get_all_issues,
    'assignees': get_all_assignees,
    'collaborators': get_all_collaborators,
    'pull_requests': get_all_pull_requests,
    'stargazers': get_all_stargazers
}

SUB_STREAMS = {
    'pull_requests': ['reviews'],
    'pull_requests': ['review_comments']
}

def do_sync(config, state, catalog):
    access_token = config['access_token']
    session.headers.update({'authorization': 'token ' + access_token})

    # get selected streams, make sure stream dependencies are met
    selected_stream_ids = get_selected_streams(catalog)
    validate_dependencies(selected_stream_ids)

    for stream in catalog['streams']:
        stream_id = stream['tap_stream_id']
        stream_schema = stream['schema']
        mdata = stream['metadata']

        # if it is a "sub_stream", it will be sync'd by its parent
        if not SYNC_FUNCTIONS.get(stream_id):
            continue

        # if stream is selected, write schema and sync
        if stream_id in selected_stream_ids:
            singer.write_schema(stream_id, stream_schema,stream['key_properties'])

            # get sync function and any sub streams
            sync_func = SYNC_FUNCTIONS[stream_id]
            sub_stream_ids = SUB_STREAMS.get(stream_id, None)

            # sync stream
            if not sub_stream_ids:
                state = sync_func(stream_schema, config, state, mdata)

            # handle streams with sub streams
            else:
                stream_schemas = {stream_id: stream_schema}

                # get and write selected sub stream schemas
                for sub_stream_id in sub_stream_ids:
                    if sub_stream_id in selected_stream_ids:
                        sub_stream = get_stream_from_catalog(sub_stream_id, catalog)
                        stream_schemas[sub_stream_id] = sub_stream['schema']
                        singer.write_schema(sub_stream_id, sub_stream['schema'],
                                            sub_stream['key_properties'])

                # sync stream and it's sub streams
                state = sync_func(stream_schemas, config, state, mdata)

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
