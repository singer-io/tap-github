import argparse
import os
import json
import collections
import time
import requests
import singer
import singer.bookmarks as bookmarks
import singer.metrics as metrics
import base64
import difflib
import asyncio

from .gitlocal import GitLocal

from singer import metadata

session = requests.Session()
logger = singer.get_logger()

REQUIRED_CONFIG_KEYS = ['start_date', 'access_token', 'repository']

KEY_PROPERTIES = {
    'branches': ['repo_name'],
    'commits': ['sha'],
    'commit_files': ['id'],
    'comments': ['id'],
    'issues': ['id'],
    'assignees': ['id'],
    'collaborators': ['id'],
    'pull_requests':['id'],
    'stargazers': ['user_id'],
    'releases': ['id'],
    'reviews': ['id'],
    'review_comments': ['id'],
    'events': ['id'],
    'issue_events': ['id'],
    'issue_labels': ['id'],
    'issue_milestones': ['id'],
    'commit_comments': ['id'],
    'projects': ['id'],
    'project_columns': ['id'],
    'project_cards': ['id'],
    'repos': ['id'],
    'teams': ['id'],
    'team_members': ['id'],
    'team_memberships': ['url']
}

class GithubException(Exception):
    pass

class BadCredentialsException(GithubException):
    pass

class AuthException(GithubException):
    pass

class NotFoundException(GithubException):
    pass

class BadRequestException(GithubException):
    pass

class InternalServerError(GithubException):
    pass

class UnprocessableError(GithubException):
    pass

class NotModifiedError(GithubException):
    pass

class MovedPermanentlyError(GithubException):
    pass

class ConflictError(GithubException):
    pass

class RateLimitExceeded(GithubException):
    pass

ERROR_CODE_EXCEPTION_MAPPING = {
    301: {
        "raise_exception": MovedPermanentlyError,
        "message": "The resource you are looking for is moved to another URL."
    },
    304: {
        "raise_exception": NotModifiedError,
        "message": "The requested resource has not been modified since the last time you accessed it."
    },
    400:{
        "raise_exception": BadRequestException,
        "message": "The request is missing or has a bad parameter."
    },
    401: {
        "raise_exception": BadCredentialsException,
        "message": "Invalid authorization credentials."
    },
    403: {
        "raise_exception": AuthException,
        "message": "User doesn't have permission to access the resource."
    },
    404: {
        "raise_exception": NotFoundException,
        "message": "The resource you have specified cannot be found"
    },
    409: {
        "raise_exception": ConflictError,
        "message": "The request could not be completed due to a conflict with the current state of the server."
    },
    422: {
        "raise_exception": UnprocessableError,
        "message": "The request was not able to process right now."
    },
    500: {
        "raise_exception": InternalServerError,
        "message": "An error has occurred at Github's end."
    }
}

def utf8_hook(data, typ, schema):
    if typ != 'string':
        return data

    # Don't need to handle these either
    if schema.get('format') == 'date_time' or schema.get('format') == 'singer.decimal':
        return data

    # Without this, the string 'None' gets passed through
    if data is None:
        return data

    try:
        # Replace null bytes to make valid utf-8 text
        decodedStr = bytes(str(data), 'utf-8').decode('utf-8', 'ignore').replace('\u0000','')
        return decodedStr
    except UnicodeDecodeError:
        return None

def translate_state(state, catalog, repositories):
    '''
    This tap used to only support a single repository, in which case the
    state took the shape of:
    {
      "bookmarks": {
        "commits": {
          "since": "2018-11-14T13:21:20.700360Z"
        }
      }
    }
    The tap now supports multiple repos, so this function should be called
    at the beginning of each run to ensure the state is translate to the
    new format:
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
    '''
    nested_dict = lambda: collections.defaultdict(nested_dict)
    new_state = nested_dict()

    for stream in catalog['streams']:
        stream_name = stream['tap_stream_id']
        for repo in repositories:
            if bookmarks.get_bookmark(state, repo, stream_name):
                return state
            if bookmarks.get_bookmark(state, stream_name, 'since'):
                new_state['bookmarks'][repo][stream_name]['since'] = bookmarks.get_bookmark(state, stream_name, 'since')

    return new_state


def get_bookmark(state, repo, stream_name, bookmark_key, start_date=None):
    repo_stream_dict = bookmarks.get_bookmark(state, repo, stream_name)
    if repo_stream_dict:
        return repo_stream_dict.get(bookmark_key)
    if start_date:
        return start_date
    return None

def raise_for_error(resp, source, url):
    error_code = resp.status_code
    try:
        response_json = resp.json()
    except Exception:
        response_json = {}

    if error_code == 404:
        details = ERROR_CODE_EXCEPTION_MAPPING.get(error_code).get("message")
        if source == "teams":
            details += ' or it is a personal account repository'
        message = "HTTP-error-code: 404, URL: {}. Error: {}. Please refer \'{}\' for more details." \
            .format(url, details, response_json.get("documentation_url"))
    else:
        message = "HTTP-error-code: {}, URL: {}. Error: {}".format(
            error_code, url, ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get("message", "Unknown Error") if response_json == {} else response_json)

    exc = ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get("raise_exception", GithubException)
    raise exc(message) from None

def calculate_seconds(epoch):
    current = time.time()
    return int(round((epoch - current), 0))

def rate_throttling(response):
    if int(response.headers['X-RateLimit-Remaining']) < 10:
        seconds_to_sleep = calculate_seconds(int(response.headers['X-RateLimit-Reset']))

        #if seconds_to_sleep > 600:
        #    message = "API rate limit exceeded, please try after {} seconds.".format(seconds_to_sleep)
        #    raise RateLimitExceeded(message) from None

        logger.info("API rate limit exceeded. Tap will retry the data collection after %s seconds.", seconds_to_sleep)
        time.sleep(seconds_to_sleep + 10)

# pylint: disable=dangerous-default-value
MAX_RETRY_TIME = 3600   # Retry for up to an hour, then die
RETRY_WAIT = 15  # Wait between requests when the server is struggling
def authed_get(source, url, headers={}):
    with metrics.http_request_timer(source) as timer:
        session.headers.update(headers)
        retry_time = 0
        while True:
            resp = session.request(method='get', url=url)
            if resp.status_code >= 500:
                if retry_time >= MAX_RETRY_TIME:
                    raise InternalServerError('Internal server error {} persisted after '\
                        'attempting to retry for {} seconds.'.format(resp.status_code,
                        MAX_RETRY_TIME))
                else:
                    logger.info('Encountered internal server error code {}, waiting {} seconds ' \
                        'and then retrying.'.format(resp.status_code, RETRY_WAIT))
                    retry_time += RETRY_WAIT
                    time.sleep(RETRY_WAIT)
            elif resp.status_code != 200:
                raise_for_error(resp, source, url)
            else:
                break

        timer.tags[metrics.Tag.http_status_code] = resp.status_code
        rate_throttling(resp)
        return resp

def authed_get_yield(source, url, headers={}):
    response = authed_get(source, url, headers)
    yield response

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


def generate_pr_commit_schema(commit_schema):
    pr_commit_schema = commit_schema.copy()
    pr_commit_schema['properties']['pr_number'] = {
        "type":  ["null", "integer"]
    }
    pr_commit_schema['properties']['pr_id'] = {
        "type": ["null", "string"]
    }
    pr_commit_schema['properties']['id'] = {
        "type": ["null", "string"]
    }

    return pr_commit_schema

def load_schemas():
    schemas = {}

    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = json.load(file)
    return schemas

class DependencyException(Exception):
    pass

def validate_dependencies(selected_stream_ids):
    errs = []
    msg_tmpl = ("Unable to extract '{0}' data, "
                "to receive '{0}' data, you also need to select '{1}'.")

    for main_stream, sub_streams in SUB_STREAMS.items():
        if main_stream not in selected_stream_ids:
            for sub_stream in sub_streams:
                if sub_stream in selected_stream_ids:
                    errs.append(msg_tmpl.format(sub_stream, main_stream))

    if errs:
        raise DependencyException(" ".join(errs))


def write_metadata(mdata, values, breadcrumb):
    mdata.append(
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

    # This minimizes diffs when there are changes
    def sortFunc(val):
        return val['stream']
    streams.sort(key=sortFunc)

    return {'streams': streams}

def verify_repo_access(url_for_repo, repo):
    try:
        authed_get("verifying repository access", url_for_repo)
    except NotFoundException:
        # throwing user-friendly error message as it checks token access
        message = "HTTP-error-code: 404, Error: Please check the repository name \'{}\' or you do not have sufficient permissions to access this repository.".format(repo)
        raise NotFoundException(message) from None

def verify_access_for_repo(config):

    access_token = config['access_token']
    session.headers.update({'authorization': 'token ' + access_token, 'per_page': '1', 'page': '1'})

    repositories = list(filter(None, config['repository'].split(' ')))

    for repo in repositories:
        logger.info("Verifying access of repository: %s", repo)

        url_for_repo = "https://api.github.com/repos/{}/commits".format(repo)

        # Verifying for Repo access
        verify_repo_access(url_for_repo, repo)

def do_discover(config):
    verify_access_for_repo(config)
    catalog = get_catalog()
    # dump catalog
    print(json.dumps(catalog, indent=2))

def get_all_teams(schemas, repo_path, state, mdata, _start_date):
    org = repo_path.split('/')[0]
    with metrics.record_counter('teams') as counter:
        try:
            for response in authed_get_all_pages(
                    'teams',
                    'https://api.github.com/orgs/{}/teams?sort=created_at&direction=desc'.format(org)
            ):
                teams = response.json()
                extraction_time = singer.utils.now()

                for r in teams:
                    team_slug = r.get('slug')
                    r['_sdc_repository'] = repo_path

                    # transform and write release record
                    with singer.Transformer(pre_hook=utf8_hook) as transformer:
                        rec = transformer.transform(r, schemas, metadata=metadata.to_map(mdata))
                    singer.write_record('teams', rec, time_extracted=extraction_time)
                    singer.write_bookmark(state, repo_path, 'teams', {'since': singer.utils.strftime(extraction_time)})
                    counter.increment()

                    if schemas.get('team_members'):
                        for team_members_rec in get_all_team_members(team_slug, schemas['team_members'], repo_path, state, mdata):
                            singer.write_record('team_members', team_members_rec, time_extracted=extraction_time)
                            singer.write_bookmark(state, repo_path, 'team_members', {'since': singer.utils.strftime(extraction_time)})

                    if schemas.get('team_memberships'):
                        for team_memberships_rec in get_all_team_memberships(team_slug, schemas['team_memberships'], repo_path, state, mdata):
                            singer.write_record('team_memberships', team_memberships_rec, time_extracted=extraction_time)
        except AuthException as err:
            # Original error:
            # {'message': 'Must have admin rights to Repository.', 'documentation_url':
            # 'https://docs.github.com/rest/reference/teams#list-teams'}
            logger.info('Received 403 unauthorized while trying to access teams. You must' \
                    ' have admin access to load teams, skipping stream for repo {}.'\
                    .format(repo_path))

    return state

def get_all_team_members(team_slug, schemas, repo_path, state, mdata):
    org = repo_path.split('/')[0]
    with metrics.record_counter('team_members') as counter:
        for response in authed_get_all_pages(
                'team_members',
                'https://api.github.com/orgs/{}/teams/{}/members?sort=created_at&direction=desc'.format(org, team_slug)
        ):
            team_members = response.json()
            for r in team_members:
                r['_sdc_repository'] = repo_path

                # transform and write release record
                with singer.Transformer(pre_hook=utf8_hook) as transformer:
                    rec = transformer.transform(r, schemas, metadata=metadata.to_map(mdata))
                counter.increment()

                yield rec

    return state

def get_all_team_memberships(team_slug, schemas, repo_path, state, mdata):
    org = repo_path.split('/')[0]
    for response in authed_get_all_pages(
            'team_members',
            'https://api.github.com/orgs/{}/teams/{}/members?sort=created_at&direction=desc'.format(org, team_slug)
        ):
        team_members = response.json()
        with metrics.record_counter('team_memberships') as counter:
            for r in team_members:
                username = r['login']
                for res in authed_get_all_pages(
                        'memberships',
                        'https://api.github.com/orgs/{}/teams/{}/memberships/{}'.format(org, team_slug, username)
                ):
                    team_membership = res.json()
                    team_membership['_sdc_repository'] = repo_path
                    with singer.Transformer(pre_hook=utf8_hook) as transformer:
                        rec = transformer.transform(team_membership, schemas, metadata=metadata.to_map(mdata))
                    counter.increment()
                    yield rec
    return state


def get_all_issue_events(schemas, repo_path, state, mdata, start_date):
    bookmark_value = get_bookmark(state, repo_path, "issue_events", "since", start_date)
    if bookmark_value:
        bookmark_time = singer.utils.strptime_to_utc(bookmark_value)
    else:
        bookmark_time = 0


    with metrics.record_counter('issue_events') as counter:
        for response in authed_get_all_pages(
                'issue_events',
                'https://api.github.com/repos/{}/issues/events?per_page=100&sort=created_at&direction=desc'.format(repo_path)
        ):
            events = response.json()
            extraction_time = singer.utils.now()
            for event in events:
                event['_sdc_repository'] = repo_path
                # skip records that haven't been updated since the last run
                # the GitHub API doesn't currently allow a ?since param for pulls
                # once we find the first piece of old data we can return, thanks to
                # the sorting
                updated_at = event.get('created_at') if event.get('updated_at') is None else event.get('updated_at')
                if bookmark_time and singer.utils.strptime_to_utc(updated_at) < bookmark_time:
                    return state

                # transform and write release record
                with singer.Transformer(pre_hook=utf8_hook) as transformer:
                    rec = transformer.transform(event, schemas, metadata=metadata.to_map(mdata))
                singer.write_record('issue_events', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, repo_path, 'issue_events', {'since': singer.utils.strftime(extraction_time)})
                counter.increment()

    return state


def get_all_events(schemas, repo_path, state, mdata, start_date):
    # Incremental sync off `created_at`
    # https://developer.github.com/v3/issues/events/#list-events-for-a-repository
    # 'https://api.github.com/repos/{}/issues/events?sort=created_at&direction=desc'.format(repo_path)

    bookmark_value = get_bookmark(state, repo_path, "events", "since", start_date)
    if bookmark_value:
        bookmark_time = singer.utils.strptime_to_utc(bookmark_value)
    else:
        bookmark_time = 0

    with metrics.record_counter('events') as counter:
        for response in authed_get_all_pages(
                'events',
                'https://api.github.com/repos/{}/events?per_page=100&sort=created_at&direction=desc'.format(repo_path)
        ):
            events = response.json()
            extraction_time = singer.utils.now()
            for r in events:
                r['_sdc_repository'] = repo_path

                # skip records that haven't been updated since the last run
                # the GitHub API doesn't currently allow a ?since param for pulls
                # once we find the first piece of old data we can return, thanks to
                # the sorting
                updated_at = r.get('created_at') if r.get('updated_at') is None else r.get('updated_at')
                if bookmark_time and singer.utils.strptime_to_utc(updated_at) < bookmark_time:
                    return state

                # transform and write release record
                with singer.Transformer(pre_hook=utf8_hook) as transformer:
                    rec = transformer.transform(r, schemas, metadata=metadata.to_map(mdata))
                singer.write_record('events', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, repo_path, 'events', {'since': singer.utils.strftime(extraction_time)})
                counter.increment()

    return state

def get_all_issue_milestones(schemas, repo_path, state, mdata, start_date):
    # Incremental sync off `due on` ??? confirm.
    # https://developer.github.com/v3/issues/milestones/#list-milestones-for-a-repository
    # 'https://api.github.com/repos/{}/milestones?sort=created_at&direction=desc'.format(repo_path)
    bookmark_value = get_bookmark(state, repo_path, "issue_milestones", "since", start_date)
    if bookmark_value:
        bookmark_time = singer.utils.strptime_to_utc(bookmark_value)
    else:
        bookmark_time = 0

    with metrics.record_counter('issue_milestones') as counter:
        for response in authed_get_all_pages(
                'milestones',
                'https://api.github.com/repos/{}/milestones?per_page=100&direction=desc'.format(repo_path)
        ):
            milestones = response.json()
            extraction_time = singer.utils.now()
            for r in milestones:
                r['_sdc_repository'] = repo_path

                # skip records that haven't been updated since the last run
                # the GitHub API doesn't currently allow a ?since param for pulls
                # once we find the first piece of old data we can return, thanks to
                # the sorting
                if bookmark_time and r.get("due_on") and singer.utils.strptime_to_utc(r.get("due_on")) < bookmark_time:
                    continue

                # transform and write release record
                with singer.Transformer(pre_hook=utf8_hook) as transformer:
                    rec = transformer.transform(r, schemas, metadata=metadata.to_map(mdata))
                singer.write_record('issue_milestones', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, repo_path, 'issue_milestones', {'since': singer.utils.strftime(extraction_time)})
                counter.increment()

    return state

def get_all_issue_labels(schemas, repo_path, state, mdata, _start_date):
    # https://developer.github.com/v3/issues/labels/
    # not sure if incremental key
    # 'https://api.github.com/repos/{}/labels?sort=created_at&direction=desc'.format(repo_path)

    with metrics.record_counter('issue_labels') as counter:
        for response in authed_get_all_pages(
                'issue_labels',
                'https://api.github.com/repos/{}/labels?per_page=100'.format(repo_path)
        ):
            issue_labels = response.json()
            extraction_time = singer.utils.now()
            for r in issue_labels:
                r['_sdc_repository'] = repo_path

                # transform and write release record
                with singer.Transformer(pre_hook=utf8_hook) as transformer:
                    rec = transformer.transform(r, schemas, metadata=metadata.to_map(mdata))
                singer.write_record('issue_labels', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, repo_path, 'issue_labels', {'since': singer.utils.strftime(extraction_time)})
                counter.increment()

    return state

def get_all_commit_comments(schemas, repo_path, state, mdata, start_date):
    # https://developer.github.com/v3/repos/comments/
    # updated_at? incremental
    # 'https://api.github.com/repos/{}/comments?sort=created_at&direction=desc'.format(repo_path)
    bookmark_value = get_bookmark(state, repo_path, "commit_comments", "since", start_date)
    if bookmark_value:
        bookmark_time = singer.utils.strptime_to_utc(bookmark_value)
    else:
        bookmark_time = 0

    with metrics.record_counter('commit_comments') as counter:
        for response in authed_get_all_pages(
                'commit_comments',
                'https://api.github.com/repos/{}/comments?per_page=100&sort=created_at&direction=desc'.format(repo_path)
        ):
            commit_comments = response.json()
            extraction_time = singer.utils.now()
            for r in commit_comments:
                r['_sdc_repository'] = repo_path

                # skip records that haven't been updated since the last run
                # the GitHub API doesn't currently allow a ?since param for pulls
                # once we find the first piece of old data we can return, thanks to
                # the sorting
                if bookmark_time and singer.utils.strptime_to_utc(r.get('updated_at')) < bookmark_time:
                    return state

                # transform and write release record
                with singer.Transformer(pre_hook=utf8_hook) as transformer:
                    rec = transformer.transform(r, schemas, metadata=metadata.to_map(mdata))
                singer.write_record('commit_comments', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, repo_path, 'commit_comments', {'since': singer.utils.strftime(extraction_time)})
                counter.increment()

    return state

def get_all_projects(schemas, repo_path, state, mdata, start_date):
    bookmark_value = get_bookmark(state, repo_path, "projects", "since", start_date)
    if bookmark_value:
        bookmark_time = singer.utils.strptime_to_utc(bookmark_value)
    else:
        bookmark_time = 0

    with metrics.record_counter('projects') as counter:
        #pylint: disable=too-many-nested-blocks
        for response in authed_get_all_pages(
                'projects',
                'https://api.github.com/repos/{}/projects?per_page=100&sort=created_at&direction=desc'.format(repo_path),
                { 'Accept': 'application/vnd.github.inertia-preview+json' }
        ):
            projects = response.json()
            extraction_time = singer.utils.now()
            for r in projects:
                r['_sdc_repository'] = repo_path

                # skip records that haven't been updated since the last run
                # the GitHub API doesn't currently allow a ?since param for pulls
                # once we find the first piece of old data we can return, thanks to
                # the sorting
                if bookmark_time and singer.utils.strptime_to_utc(r.get('updated_at')) < bookmark_time:
                    return state

                # transform and write release record
                with singer.Transformer(pre_hook=utf8_hook) as transformer:
                    rec = transformer.transform(r, schemas, metadata=metadata.to_map(mdata))
                singer.write_record('projects', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, repo_path, 'projects', {'since': singer.utils.strftime(extraction_time)})
                counter.increment()

                project_id = r.get('id')



                # sync project_columns if that schema is present (only there if selected)
                if schemas.get('project_columns'):
                    for project_column_rec in get_all_project_columns(project_id, schemas['project_columns'], repo_path, state, mdata, start_date):
                        singer.write_record('project_columns', project_column_rec, time_extracted=extraction_time)
                        singer.write_bookmark(state, repo_path, 'project_columns', {'since': singer.utils.strftime(extraction_time)})

                        # sync project_cards if that schema is present (only there if selected)
                        if schemas.get('project_cards'):
                            column_id = project_column_rec['id']
                            for project_card_rec in get_all_project_cards(column_id, schemas['project_cards'], repo_path, state, mdata, start_date):
                                singer.write_record('project_cards', project_card_rec, time_extracted=extraction_time)
                                singer.write_bookmark(state, repo_path, 'project_cards', {'since': singer.utils.strftime(extraction_time)})
    return state


def get_all_project_cards(column_id, schemas, repo_path, state, mdata, start_date):
    bookmark_value = get_bookmark(state, repo_path, "project_cards", "since", start_date)
    if bookmark_value:
        bookmark_time = singer.utils.strptime_to_utc(bookmark_value)
    else:
        bookmark_time = 0

    with metrics.record_counter('project_cards') as counter:
        for response in authed_get_all_pages(
                'project_cards',
                'https://api.github.com/projects/columns/{}/cards?per_page=100&sort=created_at&direction=desc'.format(column_id)
        ):
            project_cards = response.json()
            for r in project_cards:
                r['_sdc_repository'] = repo_path

                # skip records that haven't been updated since the last run
                # the GitHub API doesn't currently allow a ?since param for pulls
                # once we find the first piece of old data we can return, thanks to
                # the sorting
                if bookmark_time and singer.utils.strptime_to_utc(r.get('updated_at')) < bookmark_time:
                    return state

                # transform and write release record
                with singer.Transformer(pre_hook=utf8_hook) as transformer:
                    rec = transformer.transform(r, schemas, metadata=metadata.to_map(mdata))
                counter.increment()
                yield rec

    return state

def get_all_project_columns(project_id, schemas, repo_path, state, mdata, start_date):
    bookmark_value = get_bookmark(state, repo_path, "project_columns", "since", start_date)
    if bookmark_value:
        bookmark_time = singer.utils.strptime_to_utc(bookmark_value)
    else:
        bookmark_time = 0

    with metrics.record_counter('project_columns') as counter:
        for response in authed_get_all_pages(
                'project_columns',
                'https://api.github.com/projects/{}/columns?per_page=100&sort=created_at&direction=desc'.format(project_id)
        ):
            project_columns = response.json()
            for r in project_columns:
                r['_sdc_repository'] = repo_path

                # skip records that haven't been updated since the last run
                # the GitHub API doesn't currently allow a ?since param for pulls
                # once we find the first piece of old data we can return, thanks to
                # the sorting
                if bookmark_time and singer.utils.strptime_to_utc(r.get('updated_at')) < bookmark_time:
                    return state

                # transform and write release record
                with singer.Transformer(pre_hook=utf8_hook) as transformer:
                    rec = transformer.transform(r, schemas, metadata=metadata.to_map(mdata))
                counter.increment()
                yield rec

    return state

def get_all_releases(schemas, repo_path, state, mdata, _start_date):
    # Releases doesn't seem to have an `updated_at` property, yet can be edited.
    # For this reason and since the volume of release can safely be considered low,
    #    bookmarks were ignored for releases.

    with metrics.record_counter('releases') as counter:
        for response in authed_get_all_pages(
                'releases',
                'https://api.github.com/repos/{}/releases?per_page=100&sort=created_at&direction=desc'.format(repo_path)
        ):
            releases = response.json()
            extraction_time = singer.utils.now()
            for r in releases:
                r['_sdc_repository'] = repo_path

                # transform and write release record
                with singer.Transformer(pre_hook=utf8_hook) as transformer:
                    rec = transformer.transform(r, schemas, metadata=metadata.to_map(mdata))
                singer.write_record('releases', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, repo_path, 'releases', {'since': singer.utils.strftime(extraction_time)})
                counter.increment()

    return state

PR_CACHE = {}
def get_all_pull_requests(schemas, repo_path, state, mdata, start_date):
    '''
    https://developer.github.com/v3/pulls/#list-pull-requests
    '''

    cur_cache = {}
    PR_CACHE[repo_path] = cur_cache

    bookmark_value = get_bookmark(state, repo_path, "pull_requests", "since", start_date)
    if bookmark_value:
        bookmark_time = singer.utils.strptime_to_utc(bookmark_value)
    else:
        bookmark_time = 0

    with metrics.record_counter('pull_requests') as counter:
        with metrics.record_counter('reviews') as reviews_counter:
            for response in authed_get_all_pages(
                    'pull_requests',
                    'https://api.github.com/repos/{}/pulls?per_page=100&state=all&sort=updated&direction=desc'.format(repo_path)
            ):
                pull_requests = response.json()
                extraction_time = singer.utils.now()
                for pr in pull_requests:
                    # Skip records that haven't been updated since the last run because
                    # the GitHub API doesn't currently allow a ?since param for pulls.
                    # Return early in this case to stop querying pages of pull requests.
                    # PRs get "updated" when the head or base changes, so those commits will have
                    # been fetched in a previous run.
                    if bookmark_time and singer.utils.strptime_to_utc(pr.get('updated_at')) < bookmark_time:
                        return state

                    pr_num = pr.get('number')
                    pr_id = pr.get('id')
                    pr['_sdc_repository'] = repo_path

                    # Cache the commit into for commit fetching
                    cur_cache[str(pr_num)] = {
                        'pr_num': str(pr_num),
                        'base_sha': pr['base']['sha'],
                        'base_ref': pr['base']['ref'],
                        'head_sha': pr['head']['sha'],
                        'head_ref': pr['head']['ref']
                    }

                    # transform and write pull_request record
                    with singer.Transformer(pre_hook=utf8_hook) as transformer:
                        rec = transformer.transform(pr, schemas['pull_requests'], metadata=metadata.to_map(mdata))
                    singer.write_record('pull_requests', rec, time_extracted=extraction_time)
                    # BUGBUG? What if there's a failure to load the reviews, review comments, or pr
                    # commits for this PR? Wouldn't they then not be fetched later?
                    singer.write_bookmark(state, repo_path, 'pull_requests', {'since': singer.utils.strftime(extraction_time)})
                    counter.increment()

                    # sync reviews if that schema is present (only there if selected)
                    if schemas.get('reviews'):
                        for review_rec in get_reviews_for_pr(pr_num, schemas['reviews'], repo_path, state, mdata):
                            singer.write_record('reviews', review_rec, time_extracted=extraction_time)
                            singer.write_bookmark(state, repo_path, 'reviews', {'since': singer.utils.strftime(extraction_time)})

                            reviews_counter.increment()

                    # sync review comments if that schema is present (only there if selected)
                    if schemas.get('review_comments'):
                        for review_comment_rec in get_review_comments_for_pr(pr_num, schemas['review_comments'], repo_path, state, mdata):
                            singer.write_record('review_comments', review_comment_rec, time_extracted=extraction_time)
                            singer.write_bookmark(state, repo_path, 'review_comments', {'since': singer.utils.strftime(extraction_time)})

                    # We eliminated pr_commits entirely since they can now be obtained by following
                    # the head commit from a PR.

    return state

def get_reviews_for_pr(pr_number, schema, repo_path, state, mdata):
    for response in authed_get_all_pages(
            'reviews',
            'https://api.github.com/repos/{}/pulls/{}/reviews?per_page=100'.format(repo_path,pr_number)
    ):
        reviews = response.json()
        for review in reviews:
            review['_sdc_repository'] = repo_path
            with singer.Transformer(pre_hook=utf8_hook) as transformer:
                rec = transformer.transform(review, schema, metadata=metadata.to_map(mdata))
            yield rec


        return state

def get_review_comments_for_pr(pr_number, schema, repo_path, state, mdata):
    for response in authed_get_all_pages(
            'comments',
            'https://api.github.com/repos/{}/pulls/{}/comments?per_page=100'.format(repo_path,pr_number)
    ):
        review_comments = response.json()
        for comment in review_comments:
            comment['_sdc_repository'] = repo_path
            with singer.Transformer(pre_hook=utf8_hook) as transformer:
                rec = transformer.transform(comment, schema, metadata=metadata.to_map(mdata))
            yield rec


        return state

def get_all_assignees(schema, repo_path, state, mdata, _start_date):
    '''
    https://developer.github.com/v3/issues/assignees/#list-assignees

    No "since" parameter available, so have to get all of them each time, which can be a lot
    (thousands) for very large repositories with a lot of contributors.
    '''
    with metrics.record_counter('assignees') as counter:
        for response in authed_get_all_pages(
                'assignees',
                'https://api.github.com/repos/{}/assignees?per_page=100'.format(repo_path)
        ):
            assignees = response.json()
            extraction_time = singer.utils.now()
            for assignee in assignees:
                assignee['_sdc_repository'] = repo_path
                with singer.Transformer(pre_hook=utf8_hook) as transformer:
                    rec = transformer.transform(assignee, schema, metadata=metadata.to_map(mdata))
                singer.write_record('assignees', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, repo_path, 'assignees', {'since': singer.utils.strftime(extraction_time)})
                counter.increment()

    return state

def get_all_collaborators(schema, repo_path, state, mdata, _start_date):
    '''
    https://developer.github.com/v3/repos/collaborators/#list-collaborators
    '''
    with metrics.record_counter('collaborators') as counter:
        try:
            for response in authed_get_all_pages(
                    'collaborators',
                    'https://api.github.com/repos/{}/collaborators?per_page=100'.format(repo_path)
            ):
                collaborators = response.json()
                extraction_time = singer.utils.now()
                for collaborator in collaborators:
                    collaborator['_sdc_repository'] = repo_path
                    with singer.Transformer(pre_hook=utf8_hook) as transformer:
                        rec = transformer.transform(collaborator, schema, metadata=metadata.to_map(mdata))
                    singer.write_record('collaborators', rec, time_extracted=extraction_time)
                    singer.write_bookmark(state, repo_path, 'collaborator', {'since': singer.utils.strftime(extraction_time)})
                    counter.increment()
        except NotFoundException:
            logger.info('Received 404 not found while trying to access collaborators. You must' \
                    ' be a repo owner to view maintainers, skipping stream for repo {}.'\
                    .format(repo_path))
        except AuthException:
            logger.info('Received 403 unauthorized while trying to access collaborators. You must' \
                    ' have push access to load collaborators, skipping stream for repo {}.'\
                    .format(repo_path))

    return state


def create_patch_for_files(old_text, new_text):
    # Note: this patch may be slightly different from a patch generated with git since the diffing
    # algorithms aren't the same, but it will at least be correct and in the same format.

    newlineToken = '\\ No newline at end of file'
    # Add this random data too in the rare case where a file literally ends in the newlineToken but
    # does have a new line at the end.
    sentinal = "SDF1G5ALB3YU"
    newlineMarker = newlineToken + sentinal
    newlineMarkerLength = len(newlineMarker)

    # Also remove empty lines at end so that they aren't included in the diff output to get the
    # format to match git's patches.

    oldSplit = old_text.split('\n')
    if oldSplit[-1] != '':
        oldSplit[-1] += newlineMarker
    else:
        oldSplit = oldSplit[:-1]

    newSplit = new_text.split('\n')
    if newSplit[-1] != '':
        newSplit[-1] += newlineMarker
    else:
        newSplit = newSplit[:-1]

    # Patches don't use any when coming from the github API, so don't use nay here
    diff = difflib.unified_diff(oldSplit, newSplit, n=0)

    # Transform this to match the format of git patches coming from the API
    difflist = list(diff)
    newDiffList = []
    firstDiffFound = False
    for diffLine in difflist:
        # Skip lines before the first @@
        if diffLine[0:2] == '@@':
            firstDiffFound = True
        if not firstDiffFound:
            continue

        # Remove extra newlines after each diff line
        if diffLine[-1:] == '\n':
            newDiffList.append(diffLine[:-1])
        # If we found the newline marker at the end, remove it and add the newline token on the next
        # line like the diff format from github.
        elif diffLine[-newlineMarkerLength:] == newlineMarker:
            newDiffList.append(diffLine[:-newlineMarkerLength])
            newDiffList.append(newlineToken)
        else:
            newDiffList.append(diffLine)

    output = '\n'.join(newDiffList)
    return output

repo_cache = {}
def get_repo_metadata(repo_path):
    if not repo_path in repo_cache:
        for response in authed_get_all_pages(
                'branches',
                'https://api.github.com/repos/{}'.format(repo_path)
        ):
            repo_cache[repo_path] = response.json()
            # Will never be multiple pages
            break
    return repo_cache[repo_path]

BRANCH_CACHE = {}
def get_all_branches(schema, repo_path,  state, mdata, start_date):
    '''
    https://docs.github.com/en/rest/reference/repos#list-branches
    '''
    # No bookmark available

    default_branch_name = get_repo_metadata(repo_path)['default_branch']

    cur_cache = {}
    BRANCH_CACHE[repo_path] = cur_cache

    with metrics.record_counter('branches') as counter:
        for response in authed_get_all_pages(
                'branches',
                'https://api.github.com/repos/{}/branches?per_page=100'.format(repo_path)
        ):
            branches = response.json()
            extraction_time = singer.utils.now()
            for branch in branches:
                branch['_sdc_repository'] = repo_path
                branch['repo_name'] = repo_path + ':' + branch['name']
                isdefault = branch['name'] == default_branch_name
                branch['isdefault'] = isdefault

                cur_cache[branch['name']] = { 'sha': branch['commit']['sha'], \
                    'isdefault': isdefault, 'name': branch['name'] }
                with singer.Transformer() as transformer:
                    rec = transformer.transform(branch, schema, metadata=metadata.to_map(mdata))
                singer.write_record('branches', rec, time_extracted=extraction_time)
                counter.increment()
    return state

def get_all_heads_for_commits(repo_path):
    '''
    Gets a list of all SHAs to use as heads for importing lists of commits. Includes all branches
    and PRs (both base and head) as well as the main branch to get all potential starting points.
    '''
    default_branch_name = get_repo_metadata(repo_path)['default_branch']

    # If this data has already been populated with get_all_branches, don't duplicate the work.
    if not repo_path in BRANCH_CACHE:
        cur_cache = {}
        BRANCH_CACHE[repo_path] = cur_cache
        for response in authed_get_all_pages(
            'branches',
            'https://api.github.com/repos/{}/branches?per_page=100'.format(repo_path)
        ):
            branches = response.json()
            for branch in branches:
                isdefault = branch['name'] == default_branch_name
                cur_cache[branch['name']] = {
                    'sha': branch['commit']['sha'],
                    'isdefault': isdefault,
                    'name': branch['name']
                }

    if not repo_path in PR_CACHE:
        cur_cache = {}
        PR_CACHE[repo_path] = cur_cache
        for response in authed_get_all_pages(
            'pull_requests',
            'https://api.github.com/repos/{}/pulls?per_page=100&state=all'.format(repo_path)
        ):
            pull_requests = response.json()
            for pr in pull_requests:
                pr_num = pr.get('number')
                cur_cache[str(pr_num)] = {
                    'pr_num': str(pr_num),
                    'base_sha': pr['base']['sha'],
                    'base_ref': pr['base']['ref'],
                    'head_sha': pr['head']['sha'],
                    'head_ref': pr['head']['ref']
                }

    # Now build a set of all potential heads
    head_set = {}
    for key, val in BRANCH_CACHE[repo_path].items():
        head_set[val['sha']] = 'refs/heads/' + val['name']
    for key, val in PR_CACHE[repo_path].items():
        head_set[val['head_sha']] = 'refs/pull/' + val['pr_num'] + '/head'
        # There could be a PR into a branch that has since been deleted and this is our only record
        # of its head, so include it
        head_set[val['base_sha']] = 'refs/heads/' + val['base_ref']
    return head_set

# Diffs over this many bytes of text are dropped and instead replaced with a flag indicating that
# the file is large.
LARGE_FILE_DIFF_THRESHOLD = 1024 * 1024

def get_commit_detail_api(commit, repo_path):
    '''
    # Augment each commit with overall stats and file-level diff data by hitting the commits
    # endpoint with the individual commit hash.
    # This is copied from the github documentation:
    # Note: If there are more than 300 files in the commit diff, the response will
    # include pagination link headers for the remaining files, up to a limit of 3000
    # files. Each page contains the static commit information, and the only changes are
    # to the file listing.
    # TODO: if the changed file count exceeds 3000, then use the raw checkout to fetch
    # this data.
    '''

    # Hash of file addition that's too large, for testing
    #if commit['sha'] != '836f0c47362e2f92f57ddee977df0f5c0da6d53b':
    #    continue
    # Hash of commit with file change that's too large, for testing
    #if commit['sha'] != '1961e1b44e7c15b1f62f6da99b0e284b71d64048':
    #    continue

    commit['files'] = []
    for commit_detail in authed_get_all_pages(
        'commits',
        'https://api.github.com/repos/{}/commits/{}'.format(repo_path, commit['sha'])
    ):
        # TODO: test fetching multiple pages of changed files if the changed file count
        # exceeds 300.
        detail_json = commit_detail.json()
        commit['stats'] = detail_json['stats']
        commit['files'].extend(detail_json['files'])

    # Iterate through each of the file changes and fetch the raw patch if it is missing
    # because it is too big of a change
    for commitFile in commit['files']:
        commitFile['is_binary'] = False
        commitFile['is_large_patch'] = False

        # Skip if there's already a patch
        if 'patch' in commitFile:
            continue
        # If no changes are showing, this is probably a binary file
        if commitFile['changes'] == 0 and commitFile['additions'] == 0 and \
                commitFile['deletions'] == 0:
            # Indicate that this file is binary if it's "modified" and the change
            # counts are zero. Change counts can be zero for other reasons with renames
            # and additions/deletions of empty files.
            # Note: this will be wrong if there is a pure mode change
            if commitFile['status'] == 'modified':
                commitFile['is_binary'] = True
            continue

        # Patch is missing for large file, get the urls of the current and previous
        # raw change blobs
        currentContentsUrl = commitFile['contents_url']

        try:
            for currentContents in authed_get_all_pages(
                'file_contents',
                currentContentsUrl
            ):
                currentContentsJson = currentContents.json()
                fileContent = currentContentsJson['content']
                    # TODO: do we need to catch base64 decode errors in case file is binary?
                decodedFileContent = base64.b64decode(fileContent).decode("utf-8")
                # Will only be one page
                break

            # Get the previous contents if the file existed before (not added)
            decodedPreviousFileContent = ''
            if commitFile['status'] != 'added':
                contentPath = currentContentsUrl.split('?ref=')[0]
                # First parent is base, second parent is head
                baseSha = commit['parents'][0]['sha']
                if 'previous_filename' in commitFile:
                    contentPath = contentPath.replace(commitFile['filename'],
                        commitFile['previous_filename'])
                previousContentsUrl = contentPath + '?ref=' + baseSha

                for previousContents in authed_get_all_pages(
                    'file_contents',
                    previousContentsUrl
                ):
                    previousContentsJson = previousContents.json()
                    previousFileContent = previousContentsJson['content']
                    # TODO: do we need to catch base64 decode errors in case file is binary?
                    decodedPreviousFileContent = base64.b64decode(previousFileContent).decode("utf-8")
                    # Will only be one page
                    break

            patch = create_patch_for_files(decodedPreviousFileContent, decodedFileContent)
            if len(patch) > LARGE_FILE_DIFF_THRESHOLD:
                commitFile['is_large_patch'] = True
            else:
                commitFile['patch'] = patch
        except NotFoundException as err:
            logger.info('Encountered 404 while fetching blob. Flagging as large file and ' \
                'skipping. Original exception: ' + repr(err))
            commitFile['is_large_patch'] = True
        except AuthException as err:
            # Original error:
            # {'message': 'This API returns blobs up to 1 MB in size. The requested blob is too
            # large to fetch via the API, but you can use the Git Data API to request blobs up to
            # 100 MB in size.', 'errors': [{'resource': 'Blob', 'field': 'data', 'code':
            # 'too_large'}], 'documentation_url':
            # 'https://docs.github.com/rest/reference/repos#get-repository-content'}
            logger.info('Encountered 403 while fetching blob, which likely means it is too '\
                'large. Treating as large file and skipping. Original excpetion: ' + repr(err))
            commitFile['is_large_patch'] = True

def get_commit_detail_local(commit, repo_path, gitLocal):
    try:
        changes = gitLocal.getCommitDiff(repo_path, commit['sha'])
        commit['files'] = changes
    except Exception as e:
        # This generally shouldn't happen since we've already fetched and checked out the head
        # commit successfully, so it probably indicates some sort of system error. Just let it
        # bubbl eup for now.
        raise e

def get_commit_changes(commit, repo_path, useLocal, gitLocal):
    if useLocal:
        get_commit_detail_local(commit, repo_path, gitLocal)
    else:
        get_commit_detail_api(commit, repo_path)

        for commitFile in commit['files']:
            if 'added' == commitFile['status']:
                commitFile['changetype'] = 'add'
            elif 'removed' == commitFile['status']:
                commitFile['changetype'] = 'delete'
            # 'renamed' takes precedence over 'modified' in github, so we need to determine if there
            # was actually a change by looking at other fields.
            elif commitFile['additions'] > 0 or commitFile['deletions'] > 0 or \
                    commitFile['is_binary'] or commitFile['is_large_patch']:
                commitFile['changetype'] = 'edit'
            else:
                commitFile['changetype'] = 'none'
            commitFile['commit_sha'] = commit['sha']
    for f in commit['files']:
        f['_sdc_repository'] = repo_path
        f['id'] = '{}/{}/{}'.format(f['_sdc_repository'], f['commit_sha'], f['filename'])
    return commit['files']

def get_all_commits(schema, repo_path,  state, mdata, start_date):
    '''
    https://developer.github.com/v3/repos/commits/#list-commits-on-a-repository
    '''
    logger.info('A2')
    bookmark = get_bookmark(state, repo_path, "commits", "since", start_date)
    if not bookmark:
        bookmark = '1970-01-01'

    # Get the set of all commits we have fetched previously
    fetchedCommits = get_bookmark(state, repo_path, "commits", "fetchedCommits")
    if not fetchedCommits:
        fetchedCommits = {}
    else:
        # We have run previously, so we don't want to use the time-based bookmark becuase it could
        # skip commits that are pushed after they are committed. So, reset the 'since' bookmark back
        # to the beginning of time and rely solely on the fetchedCommits bookmark.
        bookmark = '1970-01-01'

    # We don't want newly fetched commits to update the state if we fail partway through, because
    # this could lead to commits getting marked as fetched when their parents are never fetched. So,
    # copy the dict.
    fetchedCommits = fetchedCommits.copy()

    # Get all of the branch heads to use for querying commits
    heads = get_all_heads_for_commits(repo_path)

    # Set this here for updating the state when we don't run any queries
    extraction_time = singer.utils.now()

    with metrics.record_counter('commits') as counter:
        for head in heads:
            # If the head commit has already been synced, then skip.
            if head in fetchedCommits:
                continue

            # Maintain a list of parents we are waiting to see
            missingParents = {}

            cururl = 'https://api.github.com/repos/{}/commits?per_page=100&sha={}&since={}' \
                .format(repo_path, head, bookmark)
            while True:
                # Get commits one page at a time
                response = list(authed_get_yield('commits', cururl))[0]
                commits = response.json()
                extraction_time = singer.utils.now()
                for commit in commits:
                    # Skip commits we've already imported
                    if commit['sha'] in fetchedCommits:
                        continue
                    commit['_sdc_repository'] = repo_path
                    with singer.Transformer() as transformer:
                        rec = transformer.transform(commit, schema, metadata=metadata.to_map(mdata))
                    singer.write_record('commits', rec, time_extracted=extraction_time)

                    # Record that we have now fetched this commit
                    fetchedCommits[commit['sha']] = 1
                    # No longer a missing parent
                    missingParents.pop(commit['sha'], None)

                    # Keep track of new missing parents
                    for parent in commit['parents']:
                        if not parent['sha'] in fetchedCommits:
                            missingParents[parent['sha']] = 1
                    counter.increment()

                # If there are no missing parents, then we are done prior to reaching the lst page
                if not missingParents:
                    break
                elif 'next' in response.links:
                    cururl = response.links['next']['url']
                # Else if we have reached the end of our data but not found the parents, then we
                # have a problem
                else:
                    raise GithubException('Some commit parents never found: ' + \
                        ','.join(missingParents.keys()))

    # Don't write until the end so that we don't record fetchedCommits if we fail and never get
    # their parents.
    singer.write_bookmark(state, repo_path, 'commits', {
        'since': singer.utils.strftime(extraction_time),
        'fetchedCommits': fetchedCommits
    })

    return state

async def get_commit_changes_async(commit, repo_path, hasLocal, gitLocal):
    changedFiles = get_commit_changes(commit, repo_path, hasLocal, gitLocal)
    return changedFiles

async def getChangedfilesForCommits(commits, repo_path, hasLocal, gitLocal):
    coros = []
    for commit in commits:
        changesCoro = asyncio.to_thread(get_commit_changes, commit, repo_path, hasLocal, gitLocal)
        #changesCoro = get_commit_changes_async(commit, repo_path, hasLocal, gitLocal)
        coros.append(changesCoro)
    results = await asyncio.gather(*coros)
    return results

async def get_all_commit_files(schema, repo_path,  state, mdata, start_date, gitLocal):
    bookmark = get_bookmark(state, repo_path, "commit_files", "since", start_date)
    if not bookmark:
        bookmark = '1970-01-01'

    # Get the set of all commits we have fetched previously
    fetchedCommits = get_bookmark(state, repo_path, "commit_files", "fetchedCommits")
    if not fetchedCommits:
        fetchedCommits = {}
    else:
        # We have run previously, so we don't want to use the time-based bookmark becuase it could
        # skip commits that are pushed after they are committed. So, reset the 'since' bookmark back
        # to the beginning of time and rely solely on the fetchedCommits bookmark.
        bookmark = '1970-01-01'

    # We don't want newly fetched commits to update the state if we fail partway through, because
    # this could lead to commits getting marked as fetched when their parents are never fetched. So,
    # copy the dict.
    fetchedCommits = fetchedCommits.copy()

    # Get all of the branch heads to use for querying commits
    heads = get_all_heads_for_commits(repo_path)

    # Set this here for updating the state when we don't run any queries
    extraction_time = singer.utils.now()

    with metrics.record_counter('commit_files') as counter:
        for head in heads:
            headRef = heads[head]
            # If the head commit has already been synced, then skip.
            if head in fetchedCommits:
                logger.info('Head already fetched {} {}'.format(headRef, head))
                continue

            logger.info('Getting files for head {} {}'.format(headRef, head))

            # Maintain a list of parents we are waiting to see
            missingParents = {}

            # Attempt to checkout the head ref
            # NOTE: This doesn't use our rate limit at all, which is nice!
            FORCE_API = False
            if FORCE_API:
                hasLocal = False
            else:
                hasLocal = gitLocal.fetchRef(repo_path, headRef, head)
                if not hasLocal:
                    # Will fall back to using github's API
                    logger.info('FAILED to fetch ref {}/{}/{}'.format(repo_path, headRef, head))

            cururl = 'https://api.github.com/repos/{}/commits?per_page=100&sha={}&since={}' \
                .format(repo_path, head, bookmark)
            while True:
                # Get commits one page at a time
                if hasLocal:
                    commits = gitLocal.getCommitsFromHead(repo_path, head)
                else:
                    response = list(authed_get_yield('commits', cururl))[0]
                    commits = response.json()
                extraction_time = singer.utils.now()
                commitQ = []
                for commit in commits:
                    # Skip commits we've already imported
                    if commit['sha'] in fetchedCommits:
                        continue

                    commitQ.append(commit)

                    # Record that we have now fetched this commit
                    fetchedCommits[commit['sha']] = 1
                    # No longer a missing parent
                    missingParents.pop(commit['sha'], None)

                    # Keep track of new missing parents
                    for parent in commit['parents']:
                        if not parent['sha'] in fetchedCommits:
                            missingParents[parent['sha']] = 1
                    counter.increment()

                logger.info('Processing {} commits'.format(len(commitQ)))

                # Run in batches
                i = 0
                BATCH_SIZE = 64
                if not hasLocal:
                    BATCH_SIZE = 1
                while i * BATCH_SIZE < len(commitQ):
                    curQ = commitQ[i * BATCH_SIZE:(i + 1) * BATCH_SIZE]
                    changedFileList = await getChangedfilesForCommits(curQ, repo_path, hasLocal, gitLocal)
                    for changedFiles in changedFileList:
                        for changedFile in changedFiles:
                            with singer.Transformer() as transformer:
                                rec = transformer.transform(changedFile, schema, metadata=metadata.to_map(mdata))
                            singer.write_record('commit_files', rec, time_extracted=extraction_time)
                    i += 1

                # If there are no missing parents, then we are done prior to reaching the lst page
                if not missingParents:
                    break
                elif not hasLocal and 'next' in response.links:
                    cururl = response.links['next']['url']
                # Else if we have reached the end of our data but not found the parents, then we
                # have a problem
                else:
                    raise GithubException('Some commit parents never found: ' + \
                        ','.join(missingParents.keys()))

    # Don't write until the end so that we don't record fetchedCommits if we fail and never get
    # their parents.
    singer.write_bookmark(state, repo_path, 'commit_files', {
        'since': singer.utils.strftime(extraction_time),
        'fetchedCommits': fetchedCommits
    })

    return state

def get_all_issues(schema, repo_path,  state, mdata, start_date):
    '''
    https://developer.github.com/v3/issues/#list-issues-for-a-repository
    '''

    bookmark = get_bookmark(state, repo_path, "issues", "since", start_date)
    if bookmark:
        query_string = '&since={}'.format(bookmark)
    else:
        query_string = ''

    with metrics.record_counter('issues') as counter:
        for response in authed_get_all_pages(
                'issues',
                'https://api.github.com/repos/{}/issues?per_page=100&state=all&sort=updated&direction=asc{}'.format(repo_path, query_string)
        ):
            issues = response.json()
            extraction_time = singer.utils.now()
            for issue in issues:
                issue['_sdc_repository'] = repo_path
                with singer.Transformer(pre_hook=utf8_hook) as transformer:
                    rec = transformer.transform(issue, schema, metadata=metadata.to_map(mdata))
                singer.write_record('issues', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, repo_path, 'issues', {'since': singer.utils.strftime(extraction_time)})
                counter.increment()
    return state

def get_all_comments(schema, repo_path, state, mdata, start_date):
    '''
    https://developer.github.com/v3/issues/comments/#list-comments-in-a-repository

    TODO: There seems to a limit of 40,000 commments for this endpoint. Instead, get the comments
    associated with each individual issues.
    '''

    bookmark = get_bookmark(state, repo_path, "comments", "since", start_date)
    if bookmark:
        query_string = '&since={}'.format(bookmark)
    else:
        query_string = ''

    with metrics.record_counter('comments') as counter:
        for response in authed_get_all_pages(
                'comments',
                'https://api.github.com/repos/{}/issues/comments?per_page=100&sort=updated' \
                '&direction=asc{}'.format(repo_path, query_string)
        ):
            comments = response.json()
            extraction_time = singer.utils.now()
            for comment in comments:
                comment['_sdc_repository'] = repo_path
                with singer.Transformer(pre_hook=utf8_hook) as transformer:
                    rec = transformer.transform(comment, schema, metadata=metadata.to_map(mdata))
                singer.write_record('comments', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, repo_path, 'comments', {'since': singer.utils.strftime(extraction_time)})
                counter.increment()
    return state

def get_all_stargazers(schema, repo_path, state, mdata, _start_date):
    '''
    https://developer.github.com/v3/activity/starring/#list-stargazers

    NOTE: This endpoint is limited to a maximum of 40,000 rows.
    '''

    stargazers_headers = {'Accept': 'application/vnd.github.v3.star+json'}

    with metrics.record_counter('stargazers') as counter:
        for response in authed_get_all_pages(
                'stargazers',
                'https://api.github.com/repos/{}/stargazers?per_page=100'.format(repo_path), stargazers_headers
        ):
            stargazers = response.json()
            extraction_time = singer.utils.now()
            for stargazer in stargazers:
                user_id = stargazer['user']['id']
                stargazer['_sdc_repository'] = repo_path
                with singer.Transformer(pre_hook=utf8_hook) as transformer:
                    rec = transformer.transform(stargazer, schema, metadata=metadata.to_map(mdata))
                rec['user_id'] = user_id
                singer.write_record('stargazers', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, repo_path, 'stargazers', {'since': singer.utils.strftime(extraction_time)})
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
    'branches': get_all_branches,
    'commits': get_all_commits,
    'commit_files': get_all_commit_files,
    'comments': get_all_comments,
    'issues': get_all_issues,
    'assignees': get_all_assignees,
    'collaborators': get_all_collaborators,
    'pull_requests': get_all_pull_requests,
    'releases': get_all_releases,
    'stargazers': get_all_stargazers,
    'events': get_all_events,
    'issue_events': get_all_issue_events,
    'issue_milestones': get_all_issue_milestones,
    'issue_labels': get_all_issue_labels,
    'projects': get_all_projects,
    'commit_comments': get_all_commit_comments,
    'teams': get_all_teams
}

SUB_STREAMS = {
    'pull_requests': ['reviews', 'review_comments'],
    'projects': ['project_cards', 'project_columns'],
    'teams': ['team_members', 'team_memberships']
}

async def do_sync(config, state, catalog):
    access_token = config['access_token']
    session.headers.update({'authorization': 'token ' + access_token})

    gitLocal = GitLocal({
        'access_token': access_token,
        'workingDir': '/tmp'
    })

    start_date = config['start_date'] if 'start_date' in config else None
    # get selected streams, make sure stream dependencies are met
    selected_stream_ids = get_selected_streams(catalog)
    validate_dependencies(selected_stream_ids)

    logger.info(selected_stream_ids)

    repositories = list(filter(None, config['repository'].split(' ')))

    state = translate_state(state, catalog, repositories)
    singer.write_state(state)

    # Put branches and then pull requests before commits, which have a data dependency on them.
    def schemaSortFunc(val):
        if val['tap_stream_id'] == 'branches':
            return 'aa'
        elif val['tap_stream_id'] == 'pull_requests':
            return 'bb'
        else:
            return val['tap_stream_id']
    catalog['streams'].sort(key=schemaSortFunc)

    #pylint: disable=too-many-nested-blocks
    for repo in repositories:
        logger.info("Starting sync of repository: %s", repo)
        for stream in catalog['streams']:
            stream_id = stream['tap_stream_id']
            stream_schema = stream['schema']
            mdata = stream['metadata']

            # if it is a "sub_stream", it will be sync'd by its parent
            if not SYNC_FUNCTIONS.get(stream_id):
                continue

            # if stream is selected, write schema and sync
            if stream_id in selected_stream_ids:
                logger.info("Syncing stream: %s", stream_id)
                singer.write_schema(stream_id, stream_schema, stream['key_properties'])

                # get sync function and any sub streams
                sync_func = SYNC_FUNCTIONS[stream_id]
                sub_stream_ids = SUB_STREAMS.get(stream_id, None)

                # sync stream
                if not sub_stream_ids:
                    if stream_id == 'commit_files':
                        state = await sync_func(stream_schema, repo, state, mdata, start_date, gitLocal)
                    else:
                        state = sync_func(stream_schema, repo, state, mdata, start_date)

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
                    state = sync_func(stream_schemas, repo, state, mdata, start_date)

                singer.write_state(state)

@singer.utils.handle_top_exception(logger)
def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.discover:
        do_discover(args.config)
    else:
        catalog = args.properties if args.properties else get_catalog()
        asyncio.run(do_sync(args.config, args.state, catalog))

async def main2():
    sys.stderr.write('asdf\n')

if __name__ == "__main__":
    main()
