import tap_github
from argparse import Namespace
from alu_tester import BaseTest
from alu_tester import capture_output
from alu_tester import partition
from alu_tester import user
from alu_tester import cli
import target_stitch
import json
import logging
import os
import sys
from io import TextIOWrapper
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
        #raw_catalog = capture_output(tap_github.do_discover)
        config = {'user-agent': 'alu@talend.com',
                  'access_token': os.getenv("TAP_GITHUB_TOKEN"),
                  'repository': 'singer-io/tap-github',}
        raw_catalog = cli.run_discovery(config)

        catalog = json.loads(raw_catalog)

        self.assertTrue(isinstance(catalog, dict))
        self.assertTrue('streams' in catalog)
        self.assertTrue(isinstance(catalog['streams'], list))


        selected_catalog = user.select_all_streams(catalog)

        # singer_messages = capture_output(tap_github.do_sync,
        #                                  config,
        #                                  {},
        #                                  selected_catalog)

        with open('singer_messages') as outfile:
            singer_messages = outfile.read()

        with open('target_output', 'wb') as outfile:
            target = target_stitch.TargetStitch([target_stitch.ValidatingHandler(),
                                                 target_stitch.LoggingHandler(TextIOWrapper(buffer=outfile, encoding=None, errors=None, newline=None, line_buffering=False, write_through=False),
                                                 target_stitch.DEFAULT_MAX_BATCH_BYTES, target_stitch.DEFAULT_MAX_BATCH_RECORDS)], sys.stdout, target_stitch.DEFAULT_MAX_BATCH_BYTES, target_stitch.DEFAULT_MAX_BATCH_RECORDS, 300)
            target.consume(singer_messages.split('\n')[:-1])


        messages_by_type = partition.by_type(singer_messages)

        debug_output(singer_messages, messages_by_type)

        records_by_stream = partition.by_stream(messages_by_type['RECORD'])

        # for stream, records in records_by_stream.items():
        #     LOG.info(f"{stream}: record count {len(records)}")

        for message_type in ['RECORD', 'STATE', 'SCHEMA']:
            with self.subTest(message_type=message_type):
                self.assertGreater(
                    len(messages_by_type[message_type]),
                    0
                )

        with open('target_output') as afile:
            string = afile.read().split('\n')
            batches = [json.loads(s) for s in string]

        records_by_stream = user.examine_target_output_file(batches)
        for stream, count in records_by_stream.items():
            with self.subTest(stream=stream):
                self.assertGreater(count, 0)

        records_by_stream = user.get_records_from_target_output(batches)
        fields_by_stream = user.examine_target_output_for_fields(batches)
        for stream, value in records_by_stream.items():
            with self.subTest(stream=stream):
                self.assertGreater(len(value['messages']), 0)
                self.assertGreaterEqual(len(fields_by_stream[stream]), len(set(value['schema']['properties'].keys())))
