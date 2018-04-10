# Changelog

## 0.3.0
  * Adds support for retrieving pull requests, assignees and collaborars [#8](https://github.com/singer-io/tap-github/pull/8)

## 0.4.0
  * Removes 'reviews' and 'files' streams
  * Adds 'pull_requests' stream
  * Adds discovery mode
  * Adds table selection -- see README for updated instructions on running this tap
  * Fixes bug where records did not match schema
  * Removed incremental replication from a few streams due to bugs, should be added later
  * [#9](https://github.com/singer-io/tap-github/pull/9)

## 0.4.1
  * Check schema's 'selected' property as well as metadata during table selection

## 0.5.0
  * Adds support for "sub-streams" (streams dependent on other streams)
  * Adds reviews stream
  * [#14](https://github.com/singer-io/tap-github/pull/14)

## 0.5.1
  * Reverts change to transformer to avoid excess logging
  * [#16](https://github.com/singer-io/tap-github/pull/16)

## 0.5.2
  * Fix record counting for reviews stream
  * [#17](https://github.com/singer-io/tap-github/pull/17)

## 0.5.3
  * Adds more fields to reviews stream
  * [#18](https://github.com/singer-io/tap-github/pull/18)
