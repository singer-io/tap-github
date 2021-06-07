
# Table of Contents

1.  [Tap-Tester Redesign](#orgb8dedd9)
    1.  [Goal](#org4f28fed)
    2.  [Problems To Solve](#org371ad46)
    3.  [Proposal](#org4ce8f53)
        1.  [No shelling out](#orgc792f3a)
        2.  [Code Coverage](#org1689643)
        3.  [Use target-stitch](#org19b7b3f)
        4.  [Should we rewrite?](#orgc430dc0)
    4.  [Further Research](#orgb069ae4)
    5.  [Testing](#orgc9b669c)
2.  [Notes on Design](#org85ebc3d)
    1.  [Introduction](#org7be2af2)
        1.  [Goal 1: tap agnostic tests](#orgf706685)
        2.  [Goal 2: Tests in python, for python](#org841e732)
        3.  [Using StringIO](#orgf2c0742)


<a id="orgb8dedd9"></a>

# Tap-Tester Redesign


<a id="org4f28fed"></a>

## Goal

1.  To provide tap-tester as a library that tests can import
2.  To make something we can open source
3.  To eliminate the run-test and run-a-test scripts


<a id="org371ad46"></a>

## Problems To Solve

1.  I can run the tap without shelling out
2.  I can use a code coverage tool on the tap's codebase
3.  I can run the target with the tap
    1.  The target can use the ValidatingHandler to do schema checks
    2.  The target can use the LoggingHandler to save batches to a file
        1.  We noticed this is a requirement of some tests
4.  Decide whether to rewrite tap-tester or modify the existing code


<a id="org4ce8f53"></a>

## Proposal


<a id="orgc792f3a"></a>

### No shelling out

At its core, tap-tester is just a 


<a id="org1689643"></a>

### Code Coverage

Fix me


<a id="org19b7b3f"></a>

### Use target-stitch

Use the StringIO class to provide an in memory buffer of all tap output.


<a id="orgc430dc0"></a>

### Should we rewrite?

Fix me


<a id="orgb069ae4"></a>

## Further Research

TBD


<a id="orgc9b669c"></a>

## Testing

One benefit of having `tap-tester` as a library is that we can write the test suite once and
allow taps to import the tests. This would greatly accelerate the time it takes to get a new tap
under test. It also allows us to have confidence in the quality of the tests.

Here is the guideline from the QA Team on what a `DiscoveryTest` should look like:

-   fkjdaslkj

So can we design tap-tester so that we can be tap agnostic while running these asserts?

Some problems you will notice:

-   `pytest` will discover and collect multiple versions of the same test
    
    Given the following code structure (extra files removed for clarity)
    
        ~/git/alu-tester
        $ tree .
        .
        └── alu_tester
            ├── __init__.py
            └── sample_tests.py
    
    and
    
        /opt/code/tap-github
        $ tree tests/alu/
        tests/alu/
        ├── test_automatic_fields.py
        └── base.py
    
    Here are what the files look like
    
        # sample_tests.py
        import unittest
        
        class BaseTest(unittest.TestCase):
        
            def test_run(self):
                print(f'Hi from {self.__class__}')
                self.assertEqual(1, 1)
        
        class AutomaticFieldsTest(BaseTest):
            pass
    
    &#x2014;
    
        # base.py
        from alu_tester.sample_tests import BaseTest
        class GithubBaseTest(EnableTest, BaseTest):
            def test_run(self):
                print(f"Hi from {self.__class__} and not sample_tests.BaseTest")
    
    &#x2014;
    
        # test_automatic_fields.py
        import tap_github
        fibrom alu_tester.sample_tests import AutomaticFieldsTest
        from .base import GithubBaseTest
        
        class GithubAutomaticFieldsTest(AutomaticFieldsTest, GithubBaseTest):
            pass
    
    Here's the "error"
    
        # bad pytest collect
        (tap-github) vagrant@vagrant:/opt/code/tap-github$ pytest --collect-only tests/alu/base.py
        ========================== test session starts ==========================
        platform linux -- Python 3.8.10, pytest-6.2.4, py-1.10.0, pluggy-0.13.1
        rootdir: /home/vagrant/git/tap-github
        collected 3 items
        
        <Package alu>
          <Module test_automatic_fields.py>
            <UnitTestCase AutomaticFieldsTest>
              <TestCaseFunction test_run>
            <UnitTestCase GithubBaseTest>
              <TestCaseFunction test_run>
            <UnitTestCase GithubAutomaticFieldsTest>
              <TestCaseFunction test_run>
        ====================== 3 tests collected in 0.17s ======================
    
    Notice the duplication, i.e. `AutomaticFieldsTest` and `GithubAutomaticFieldsTest`.
    
    Assuming we use `pytest` as our runner, we can fix this by marking the `sample_tests.*`
    classes as not a test.
    
        # sample_tests.py
        import unittest
        
        class BaseTest(unittest.TestCase):
            __test__ = False  # Marks this as not a test
            def test_run(self):
                print(f'Hi from {self.__class__}')
                self.assertEqual(1, 1)
        
        class AutomaticFieldsTest(BaseTest):
            pass
    
    Running out collect again we see much better results

    # good pytest collect
    (tap-github) vagrant@vagrant:/opt/code/tap-github$ pytest --collect-only tests/alu/base.py
    ========================== test session starts ==========================
    platform linux -- Python 3.8.10, pytest-6.2.4, py-1.10.0, pluggy-0.13.1
    rootdir: /home/vagrant/git/tap-github
    collected 2 items
    
    <Package alu>
      <Module test_automatic_fields.py>
        <UnitTestCase GithubBaseTest>
          <TestCaseFunction test_run>
        <UnitTestCase GithubAutomaticFieldsTest>
          <TestCaseFunction test_run>
    ====================== 2 tests collected in 0.17s ======================


<a id="org85ebc3d"></a>

# Notes on Design

We should spend some time designing some new usage patterns for running tap-tester. Spend some
time hacking around and thinking about the problem and then go over it in a meeting.

Here are some goals which we should shoot for:

-   Run tests directly from Python without the need for a run-it
-   Run tests and allow breakpoints within the tap to be hit
-   Capture output and be able to pass them to the ValidatingHandler in the target (or tap-tester
    can just use that as well?)
-   Our new approach should offer enough helper functions to get all our Message Types
-   Provide code-coverage and be able to eventually do something with it

DoD:

-   Do some up-front research and spiking around how we could run tap-tester in python without
    shelling out to the tap's main
-   Take some notes on whether we'd prefer to rewrite tap-tester to fit this new mode, or modify the
    existing code.
-   Write up a document
-   Have a meeting with members of the team to go over our preferred design


<a id="org7be2af2"></a>

## Introduction

`tap-tester` has a few problems:

-   It's closed source
-   Running it has turned into 3 separate bash scripts
-   Writing new tests is a lot of copying and pasting

The closed source aspect of this hurts us because it makes it impossible for contractors to run
tap-tester without intervention from us. We wrote the in-memory backend to help address this, but
maybe that's more of a bandaid than a real fix.

The `run-test`, `run-a-test`, and `run-a-test-simple` scripts are complicated. There are test
runners out there, so can we just leverage them?

Copying and pasting tests around makes it hard to update old tests. There was a period of time
where the QA Team was still working on the assertions that should be made in the tests. However,
during that time, we were still actively writing test. So there was always this ambiguity around
which tap had the most up to date set of assertions to copy from.

Leslie and I explored all of these problems and hope that some concepts we talk about here can
influence the future of tap testing.

First, we need to talk about the requirements of the new `tap-tester`. From the card, we gathered

-   We should do away with the `run-test` scripts
-   The developer experience for someone working on tests should be better
-   There are some abstractions in the current framework that are first class citizens of the tests
    and we should make it easy to work with them
-   We want to dabble with code coverage

The proposal of this document is that we rewrite `tap-tester` to use be a library that offers

-   helper functions to make test writing easy
-   the ability to capture tap and target output to run our assertions against

We have found that instead of shelling out in order to run a tap and target, we can directly call
the high level functions of a tap (i.e. `do_discovery` and `do_sync` in most cases) and capture
the output produced by those functions.

Since we have a python dictionary catalog in the test now, we can directly manipulate it in order
to remove the dependency on any Stitch service. This also deemphasizes the tap and Stitch
relationship so that we can focus on the tap and data relationship.

Finally, we have found that we can write one suite of tests and make it easy for any tap to be
covered by test. And any improvements to the tests will be "broadcast" to every tap if all of the
taps can inherit from the base test classes.

The rest of this document will go into how we

-   Created a repo outside of any tap repo
-   Created base test classes that taps can inherit from
-   Imported and ran tap code from the test code
-   Collected code coverage metrics around all of this
-   Got the target working in this new test framework
-   Improved the developer experience when it comes to debugging tests


<a id="orgf706685"></a>

### Goal 1: tap agnostic tests


<a id="org841e732"></a>

### Goal 2: Tests in python, for python

If you ask yourself what `tap-tester` is, you'll hopefully realize that it's a chunk of code
that leverages the `python unittests` module to test discovery and sync mode.

What do we expect out of discovery mode? Well, we should be returned a correctly shaped Catalog.
We'll talk specific assertions later, but generally, does the catalog have the right number of
streams? Does the metadata look correct?

What do we expect out of sync mode? Key features that need to work are

-   The tap respects the start date
-   The tap can read and write bookmarks
-   The tap can paginate data
-   The tap respects field selection


<a id="orgf2c0742"></a>

### Using StringIO

The entire rewrite of `tap-tester` is centered on this new idea: we can capture STDOUT and make
assertions against the messages emitted from the tap (or the catalog in the case of the
discovery test).

The concept is pretty simple. We create this buffer object and replace STDOUT with it. Since the
API is the same for it as it is for STDOUT, the tap is none the wiser. We 

One aspect of this approach we did not look into is memory limits on single buffers.

