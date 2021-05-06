import tap_github

from alu_tester import BaseTest
from alu_tester import capture_output
from alu_tester import partition
from alu_tester import user

import json
import logging

LOG = logging.getLogger('GithubBaseTest')

def debug_output(thing1, thing2):
    """Pass in singer_messages, then messages_by_type"""

    with open('singer_messages', 'w') as outfile1, \
         open('messages_by_type', 'w') as outfile2:

        outfile1.write(thing1)
        json.dump(thing2, outfile2)


class GithubBaseTest(BaseTest):

    def test_run(self):

        # Run Discovery
        raw_catalog = capture_output(tap_github.do_discover)

        catalog = json.loads(raw_catalog)

        self.assertTrue(isinstance(catalog, dict))
        self.assertTrue('streams' in catalog)
        self.assertTrue(isinstance(catalog['streams'], list))


        selected_catalog = user.select_all_streams(catalog)

        config = {'user-agent': 'alu@talend.com',
                  'access_token': 'REDACTED',
                  'repository': 'singer-io/tap-github',}

        singer_messages = capture_output(tap_github.do_sync,
                                         config,
                                         {},
                                         selected_catalog)

        messages_by_type = partition.by_type(singer_messages)

        debug_output(singer_messages, messages_by_type)

        records_by_stream = partition.by_stream(messages_by_type['RECORD'])

        for stream, records in records_by_stream.items():
            LOG.info(f"{stream}: record count {len(records)}")

        for message_type in ['RECORD', 'STATE', 'SCHEMA']:
            with self.subTest(message_type=message_type):
                self.assertGreater(
                    len(messages_by_type[message_type]),
                    0
                )
