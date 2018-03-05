import argparse
import os
import json
import requests
import singer
import singer.metrics as metrics

session = requests.Session()
logger = singer.get_logger()

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

def get_all_pull_requests(repo_path, state):

    review_response = None
    for response in authed_get_all_pages('files', 'https://api.github.com/repos/{}/pulls?state=all'.format(repo_path)):
        files = response.json()
        for file in files:
            pr_number = file.get('number')
            for review_response in authed_get_all_pages('reviews', 'https://api.github.com/repos/{}/pulls/{}/reviews'.format(repo_path,pr_number)):
                reviews = review_response.json()
                singer.write_records('reviews', reviews)
        singer.write_records('files', files)


    return state

def get_all_assignees(repo_path, state):

    for response in authed_get_all_pages('assignees', 'https://api.github.com/repos/{}/assignees'.format(repo_path)):
        assignees = response.json()

        singer.write_records('assignees', assignees)

    return state

def get_all_collaborators(repo_path, state):

    for response in authed_get_all_pages('collaborators', 'https://api.github.com/repos/{}/collaborators'.format(repo_path)):
        collaborators = response.json()

        singer.write_records('collaborators', collaborators)

    return state


def get_all_commits(repo_path, state):
    if 'commits' in state and state['commits'] is not None:
        query_string = '?since={}'.format(state['commits'])
    else:
        query_string = ''

    latest_commit_time = None

    with metrics.record_counter('commits') as counter:
        for response in authed_get_all_pages('commits', 'https://api.github.com/repos/{}/commits{}'.format(repo_path, query_string)):
            commits = response.json()

            for commit in commits:
                counter.increment()
                commit.pop('author', None)
                commit.pop('committer', None)

            singer.write_records('commits', commits)
            if not latest_commit_time:
                latest_commit_time = commits[0]['commit']['committer']['date']

    state['commits'] = latest_commit_time
    return state

def get_all_issues(repo_path, state):
    if 'issues' in state and state['issues'] is not None:
        query_string = '&since={}'.format(state['issues'])
    else:
        query_string = ''

    last_issue_time = None
    with metrics.record_counter('issues') as counter:
        for response in authed_get_all_pages('issues', 'https://api.github.com/repos/{}/issues?sort=updated&direction=asc{}'.format(repo_path, query_string)):
            issues = response.json()
            counter.increment(len(issues))
            if len(issues) > 0:
                last_issue_time = issues[-1]['updated_at']
            singer.write_records('issues', issues)

    state['issues'] = last_issue_time
    return state

def get_all_stargazers(repo_path, state):
    if 'stargazers' in state and state['stargazers'] is not None:
        query_string = '&since={}'.format(state['stargazers'])
    else:
        query_string = ''

    stargazers_headers = {'Accept': 'application/vnd.github.v3.star+json'}
    last_stargazer_time = None
    with metrics.record_counter('stargazers') as counter:
        for response in authed_get_all_pages('stargazers', 'https://api.github.com/repos/{}/stargazers?sort=updated&direction=asc{}'.format(repo_path, query_string), stargazers_headers):
            stargazers = response.json()
            counter.increment(len(stargazers))
            if len(stargazers) > 0:
                last_stargazer_time = stargazers[-1]['starred_at']

            for stargazer in stargazers:
                stargazer['starred_repo'] = repo_path
                stargazer['user_id'] = stargazer['user']['id']

            singer.write_records('stargazers', stargazers)

    state['stargazers'] = last_stargazer_time
    return state

def do_sync(config, state):
    access_token = config['access_token']
    repo_path = config['repository']
    schemas = load_schemas()

    session.headers.update({'authorization': 'token ' + access_token})

    if state:
        logger.info('Replicating commits since %s from %s', state, repo_path)
    else:
        logger.info('Replicating all commits from %s', repo_path)

    singer.write_schema('commits', schemas['commits'], 'sha')
    singer.write_schema('issues', schemas['issues'], 'id')
    singer.write_schema('assignees', schemas['assignees'], 'id')
    singer.write_schema('collaborators', schemas['collaborators'], 'id')
    singer.write_schema('files', schemas['files'], 'sha')
    singer.write_schema('reviews', schemas['reviews'], 'id')
    singer.write_schema('stargazers', schemas['stargazers'], ['user_id', 'starred_repo'])
    state = get_all_commits(repo_path, state)
    state = get_all_issues(repo_path, state)
    state = get_all_assignees(repo_path, state)
    state = get_all_pull_requests(repo_path, state)
    state = get_all_collaborators(repo_path, state)
    state = get_all_stargazers(repo_path, state)
    singer.write_state(state)


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config', help='Config file', required=True)
    parser.add_argument(
        '-s', '--state', help='State file')

    args = parser.parse_args()

    with open(args.config) as config_file:
        config = json.load(config_file)

    missing_keys = []
    for key in ['access_token', 'repository']:
        if key not in config:
            missing_keys += [key]

    if len(missing_keys) > 0:
        logger.fatal("Missing required configuration keys: {}".format(missing_keys))
        exit(1)

    state = {}
    if args.state:
        with open(args.state, 'r') as file:
            for line in file:
                state = json.loads(line.strip())

    do_sync(config, state)

if __name__ == '__main__':
    main()
