# Changelog

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
