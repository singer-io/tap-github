# tap-rest-api

This is a [Singer](https://singer.io) tap that produces JSON-formatted
data from the GitHub API following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from any standard REST API
- Schema is detected automatically if WSDL (planned feature, still WIP)

## Quick start

1. Install

   We recommend using a virtualenv:

   ```bash
   > virtualenv -p python3 venv
   > source venv/bin/activate
   > pip install tap-rest-api
   ```

2. Create a GitHub access token

   Login to your GitHub account, go to the
   [Personal Access Tokens](https://github.com/settings/tokens) settings
   page, and generate a new token with at least the `repo` scope. Save this
   access token, you'll need it for the next step.

3. Create the config file

   Make a copy of the `github.config.json` called `github.secret-config.json` and add the access token you just created
   and the path to one or multiple repositories that you want to extract data from. Each repo path should be space delimited. The repo path is relative to
   `https://github.com/`. For example the path for this repository is
   `singer-io/tap-rest-api`.

   ```json
   {"access_token": "your-access-token",
    "repository": "aaronsteers/tap-rest-api singer-io/getting-started"
    ...
   }
   ```

   **Note:** If you are working in a copy of this repo, the secret-config.json file will automatically be ignored from git. If not, please add the following line to your `.gitignore` file: `**/*secret-config*`

4. Run the tap in discovery mode to get properties.json file

   ```bash
   tap-rest-api --config github.config.json --discover > properties.json
   ```

5. Run the application

   `tap-rest-api` can be run with:

   ```bash
   tap-rest-api --config config.json --properties properties.json
   ```

---

Copyright &copy; 2018 Stitch
