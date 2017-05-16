import argparse
import requests
import singer
import json
import os
import singer.stats

session = requests.Session()
logger = singer.get_logger()


def authed_get(source, url):
    with singer.stats.Timer(source=source) as stats:
        session.headers.update({'Accept': 'application/vnd.github.v3.star+json'})
        resp = session.request(method='get', url=url)
        stats.http_status_code = resp.status_code
        return resp

def authed_get_all_pages(source, url):
    while True:
        r = authed_get(source, url)
        yield r
        if 'next' in r.links:
            url = r.links['next']['url']
        else:
            break

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def load_schemas():
    schemas = {}

    with open(get_abs_path('tap_github/commits.json')) as file:
        schemas['commits'] = json.load(file)

    with open(get_abs_path('tap_github/issues.json')) as file:
        schemas['issues'] = json.load(file)

    with open(get_abs_path('tap_github/stargazers.json')) as file:
        schemas['stargazers'] = json.load(file)

    return schemas

def get_all_commits(repo_path, state):
    if 'commits' in state and state['commits'] is not None:
        query_string = '?since={}'.format(state['commits'])
    else:
        query_string = ''

    latest_commit_time = None

    with singer.stats.Counter(source='commits') as stats:
        for response in authed_get_all_pages('commits', 'https://api.github.com/repos/{}/commits{}'.format(repo_path, query_string)):
            commits = response.json()

            for commit in commits:
                stats.add(record_count=1)
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
    with singer.stats.Counter(source='issues') as stats:
        for response in authed_get_all_pages('issues', 'https://api.github.com/repos/{}/issues?sort=updated&direction=asc{}'.format(repo_path, query_string)):
            issues = response.json()
            stats.add(record_count=len(issues))
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

    last_stargazer_time = None
    with singer.stats.Counter(source='stargazers') as stats:
        for response in authed_get_all_pages('stargazers', 'https://api.github.com/repos/{}/stargazers?sort=updated&direction=asc{}'.format(repo_path, query_string)):
            stargazers = response.json()
            stats.add(record_count=len(stargazers))
            if len(stargazers) > 0:
                last_stargazer_time = stargazers[-1]['starred_at']
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
    singer.write_schema('stargazers', schemas['stargazers'], 'id')
    state = get_all_commits(repo_path, state)
    state = get_all_issues(repo_path, state)
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
