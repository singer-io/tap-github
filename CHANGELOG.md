# Changelog

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
