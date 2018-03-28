# tap-github

This is a [Singer](https://singer.io) tap that produces JSON-formatted
data from the GitHub API following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:
- Pulls raw data from the [GitHub REST API](https://developer.github.com/v3/)
- Extracts the following resources from GitHub for a single repository:
  - [Assignees](https://developer.github.com/v3/issues/assignees/#list-assignees)
  - [Collaborators](https://developer.github.com/v3/repos/collaborators/#list-collaborators)
  - [Commits](https://developer.github.com/v3/repos/commits/#list-commits-on-a-repository)
  - [Issues](https://developer.github.com/v3/issues/#list-issues-for-a-repository)
  - [Pull Request Files](https://developer.github.com/v3/pulls/#list-pull-requests-files)
  - [Pull Request Reviews](https://developer.github.com/v3/pulls/reviews/#list-reviews-on-a-pull-request)
  - [Stargazers](https://developer.github.com/v3/activity/starring/#list-stargazers)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Quick start

1. Install

   We recommend using a virtualenv:

    ```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > pip install tap-github
    ```

2. Create a GitHub access token

    Login to your GitHub account, go to the
    [Personal Access Tokens](https://github.com/settings/tokens) settings
    page, and generate a new token with at least the `repo` scope. Save this
    access token, you'll need it for the next step.

3. Create the config file

    Create a JSON file containing the access token you just created
    and the path to the repository. The repo path is relative to
    `https://github.com/`. For example the path for this repository is
    `singer-io/tap-github`.

    ```json
    {"access_token": "your-access-token",
     "repository": "singer-io/tap-github"}
    ```

4. [Optional] Create the initial state file

    You can provide JSON file that contains a date for the "commit" and
    "issues" endpoints to force the application to only fetch commits and
    issues newer than those dates. If you omit the file it will fetch all
    commits and issues.

    ```json
    {"commits": "2017-01-17T20:32:05Z",
     "issues":  "2017-01-17T20:32:05Z"}
    ```

5. Run the application

    `tap-github` can be run with:

    ```bash
    tap-github --config config.json [--state state.json]
    ```

---

Copyright &copy; 2018  Stitch

