# Changelog

# 3.0.1
  * Re-add `files` and `stats` properties to `PrCommits` stream

# 3.0.0
  * Allow all python versions to grab the correct key_properties/PK value [#199](https://github.com/singer-io/tap-github/pull/199)
  * Dependabot update [#193](https://github.com/singer-io/tap-github/pull/193)

# 2.0.6
  * Remove `files` and `stats` fields from `commits` endpoint as they are not returned without fetching individual commmits [#198](https://github.com/singer-io/tap-github/pull/198)
  * Remove `files` and `stats` fields from `pr-commits` endpoint as they are not documented and not returned
  * Update tests accordingly

# 2.0.5
  * Remove date-time format from the field discussion_url in releases schema [#196](https://github.com/singer-io/tap-github/pull/196)

# 2.0.4
  * Recursively call the function if `Retry-After` has the value greater than 0 [#192](https://github.com/singer-io/tap-github/pull/192)

# 2.0.3
  * Handles the secondary rate limit - `Retry-After` [#191](https://github.com/singer-io/tap-github/pull/191)

# 2.0.2
  * Make the tap sleep for `X-RateLimit-Reset` + `2` seconds, whenever the API rate limit is hit [#190](https://github.com/singer-io/tap-github/pull/190)

# 2.0.1
  * Allow `commits` stream sync to continue when we hit an empty repo [#187](https://github.com/singer-io/tap-github/pull/187)

# 2.0.0
  * Schema updates [#170](https://github.com/singer-io/tap-github/pull/170) [#169](https://github.com/singer-io/tap-github/pull/169)
    * Update data types of fields in `events` and `issue_events` stream
    * Add missing fields to the schemas
  * Update dict based implementation to class based [#168](https://github.com/singer-io/tap-github/pull/168)
  * Implement currently syncing for repos and streams [#171](https://github.com/singer-io/tap-github/pull/171) [#174](https://github.com/singer-io/tap-github/pull/174)
  * Implement custom exception handling and backoff for 5xx error [#166](https://github.com/singer-io/tap-github/pull/166)
  * Support of custom domain [#172](https://github.com/singer-io/tap-github/pull/172)
  * Sync teams at organization level [#173](https://github.com/singer-io/tap-github/pull/173) 
  * Update integration test suite [#167](https://github.com/singer-io/tap-github/pull/167)

# 1.10.4
  * Fix team_members stream primary Key [#157] (https://github.com/singer-io/tap-github/pull/157)

# 1.10.3
  * Implemented wildcard implementation [#145] (https://github.com/singer-io/tap-github/pull/145)
  * Added additional test coverage [#145] (https://github.com/singer-io/tap-github/pull/145)

# 1.10.2
  * Added Request Timeout

# 1.10.1
  * Add support of object in "parent" field for teams [149](https://github.com/singer-io/tap-github/pull/149)

# 1.10.0
  * Handle rate limiting [#113](https://github.com/singer-io/tap-github/pull/113)
  * Handle `None` date times in the Issue Milestones Stream [#114](https://github.com/singer-io/tap-github/pull/114)
  * Change the Stargazers Stream to be Full Table replication [#118](https://github.com/singer-io/tap-github/pull/118)
  * Log an error message if an organization cannot be found [#121](https://github.com/singer-io/tap-github/pull/121)
  * Bump `singer-python` dependency to `v5.12.1` [#117](https://github.com/singer-io/tap-github/pull/117)
  * Remove stream `pull_request_reviews` [#117](https://github.com/singer-io/tap-github/pull/117)
  * Add API access check to discovery mode [#123](https://github.com/singer-io/tap-github/pull/123)

# 1.9.2
  * Adds `base` to `pull_requests` schema [#109](https://github.com/singer-io/tap-github/pull/109)

# 1.9.1
  * Fix some field data types for the `issue_events` stream [#102](https://github.com/singer-io/tap-github/pull/102)
    * `issue_events.issue.milestone`
    * `issue_events.issue.active_lock_reason`
    * `issue_events.issue.performed_via_github_app`
    * `issue_events.issue.labels.description`
    * `issue_events.performed_via_github_app`

# 1.9.0
  * Adds `issue_events` stream [#92](https://github.com/singer-io/tap-github/pull/92)
  * Makes `project_cards` stream a child of `project_columns`[#89](https://github.com/singer-io/tap-github/pull/89)

# 1.8.2
  * Fix [issue 83](https://github.com/singer-io/tap-github/issues/83) by removing `format: date-time` from `html_url` field in `projects.json`.

# 1.8.1
  * Add stream `pr_commits` back in [#81](https://github.com/singer-io/tap-github/pull/81)

# 1.8.0
  * Added team_memberships stream [#75](https://github.com/singer-io/tap-github/pull/75)

# 1.7.1
  * Remove `"format": "date-time"` from JSON schema's for keys that are not dates [#72](https://github.com/singer-io/tap-github/pull/72)

# 1.7.0
  * Adds many new streams [#70](https://github.com/singer-io/tap-github/pull/70)

# 1.6.0
  * Adds a new stream for PR Commits [#67](https://github.com/singer-io/tap-github/pull/67)

# 1.4.0
  * Adds a new stream for Github Reviews [#53](https://github.com/singer-io/tap-github/pull/53)

# 1.3.6
  * Add a call to `raise_for_status()` to catch more errors than before [#55](https://github.com/singer-io/tap-github/pull/55)
  * Update schemas to make some fields `date-time`s [#60](https://github.com/singer-io/tap-github/pull/60)

## 1.3.5
  * Add the `submitted_at` field to the `review` stream [#52](https://github.com/singer-io/tap-github/pull/52)

## 1.3.4
  * Filter pull_requests stream using its updated at [#49](https://github.com/singer-io/tap-github/pull/49)
  * Fix a bug in the setup of sub streams [#47](https://github.com/singer-io/tap-github/pull/47)

## 1.3.3
  * Include closed issues when we retrieve issues [#45](https://github.com/singer-io/tap-github/pull/45)

## 1.3.2
  * Checks responses for an auth error (403) and throws exception [#39](https://github.com/singer-io/tap-github/pull/39)
  * Remove `milestone` and add type to `closed_at` in `Issues` schema [#40](https://github.com/singer-io/tap-github/pull/40)

## 1.3.1
  * Adds empty schema for issue.assignee [#37](https://github.com/singer-io/tap-github/pull/37)

## 1.3.0
  * Add functionality to accept multiple repos [#33](https://github.com/singer-io/tap-github/pull/33)

## 1.2.1
  * Update version of `requests` to `2.20.0` in response to CVE 2018-18074

## 1.2.0
  * Adds issue comments stream  [#32](https://github.com/singer-io/tap-github/pull/32)

## 1.1.1
  * Rename the comments stream to review_comments [#31](https://github.com/singer-io/tap-github/pull/31)

## 1.1.0
  * Added the comments stream [#30](https://github.com/singer-io/tap-github/pull/30)

## 1.0.5
  * More updates to the issues schema

## 1.0.4
  * Updates the issues schema [#29](https://github.com/singer-io/tap-github/pull/29)

## 1.0.3
  * Adds additional schema to the issues JSON schema [#28](https://github.com/singer-io/tap-github/pull/28)

## 1.0.2
  * Checks responses for an not found error (404) and throws exception

## 1.0.1
  * Checks responses for an auth error (401) and throws exception [#26](https://github.com/singer-io/tap-github/pull/26)

## 1.0.0
  * Writes appropriate metadata to allow for selection of fields
  * Adds bookmarking and incremental replication to Commits and Issues streams [#22](https://github.com/singer-io/tap-github/pull/22)

## 0.5.4
  * Bumps version of singer-python to 5.0.14 to fix datetime strftime issues documented in [#69](https://github.com/singer-io/singer-python/pull/69)

## 0.5.3
  * Adds more fields to reviews stream
  * [#18](https://github.com/singer-io/tap-github/pull/18)

## 0.5.2
  * Fix record counting for reviews stream
  * [#17](https://github.com/singer-io/tap-github/pull/17)

## 0.5.1
  * Reverts change to transformer to avoid excess logging
  * [#16](https://github.com/singer-io/tap-github/pull/16)

## 0.5.0
  * Adds support for "sub-streams" (streams dependent on other streams)
  * Adds reviews stream
  * [#14](https://github.com/singer-io/tap-github/pull/14)

## 0.4.1
  * Check schema's 'selected' property as well as metadata during table selection

## 0.4.0
  * Removes 'reviews' and 'files' streams
  * Adds 'pull_requests' stream
  * Adds discovery mode
  * Adds table selection -- see README for updated instructions on running this tap
  * Fixes bug where records did not match schema
  * Removed incremental replication from a few streams due to bugs, should be added later
  * [#9](https://github.com/singer-io/tap-github/pull/9)

## 0.3.0
  * Adds support for retrieving pull requests, assignees and collaborars [#8](https://github.com/singer-io/tap-github/pull/8)