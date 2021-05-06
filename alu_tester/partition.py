from collections import defaultdict
import json
import logging


LOG = logging.getLogger('alu_tester.partition')


def by_stream(tap_output):
    """Given a dictionary of enumerated records, remove the indexes

    Given {"stream1": [(1, {"id": 1}), (2, {"id": 2})]}

    Return {"stream1": [{"id":1}, {"id":2}]}
    """
    streams = defaultdict(list)

    for stream, records in tap_output.items():
        for i, record in records:
            stream = record['stream']
            streams[stream].append(record)

    return streams


def by_type(messages):

    tap_output = {
        'RECORD': defaultdict(list),
        'STATE': [],
        'SCHEMA': [],
    }

    for i, message in enumerate(messages.split('\n')):
        if len(message) == 0:
            continue

        message = json.loads(message)

        message_type = message['type']

        LOG.debug(f'Found {message_type}')

        if message_type == 'RECORD':
            stream = message['stream']
            tap_output[message_type][stream].append((i, message))
        else:
            tap_output[message_type].append((i, message))

    return tap_output


