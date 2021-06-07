from copy import deepcopy
from collections import defaultdict
import logging


LOG = logging.getLogger('alu_tester.user')


def select_stream(metadata_entry):
    """Appends `selected` metadata to the stream"""

    modified_entry = deepcopy(metadata_entry)

    if modified_entry['breadcrumb'] == []:
        modified_entry['metadata']['selected'] = 'true'

    return modified_entry


def select_all_streams(catalog):
    """Loop over a catalog and select the streams"""

    modified_catalog = deepcopy(catalog)

    for catalog_entry in modified_catalog['streams']:
        tap_stream_id = catalog_entry['tap_stream_id']
        LOG.info(f'Selecting stream {tap_stream_id}')
        catalog_entry['metadata'] = [select_stream(metadata_entry)
                                     for metadata_entry in catalog_entry['metadata']]
    return modified_catalog


def select_certain_streams(catalog, certain_streams):
    """Loop over a catalog and select `certain_streams`"""

    return select_all_streams(
        {"streams": [catalog_entry
                     for catalog_entry in catalog['streams']
                     if catalog_entry['tap_stream_id'] in certain_streams]}
    )

def select_a_field(metadata_entry):
    metadata_entry['metadata']['selected'] = 'true'
    return metadata_entry

def select_all_fields(catalog):
    for catalog_entry in catalog['streams']:
        for metadata_entry in catalog_entry['metadata']:
            if metadata_entry['breadcrumb'] is not []:
                metadata_entry['metadata']['selected'] = 'true'
    return catalog

def select_all_streams_and_fields(catalog):
    modified_catalog = select_all_streams(catalog)
    return select_all_fields(modified_catalog)

def select_some_fields(catalog, fields):
    for catalog_entry in catalog['streams']:
        for metadata_entry in catalog_entry['metadata']:
            if metadata_entry['breadcrumb'] is not [] and metadata_entry['breadcrumb'][1] in fields:
                metadata_entry['metadata']['selected'] = 'true'
    return catalog

def get_records_from_target_output(target_output_file):
    records_by_stream = {}
    for batch in target_output_file:
        stream = batch.get('table_name')
        if stream not in records_by_stream:
            records_by_stream[stream] = {'messages': [],
                                         'schema': batch['schema'],
                                         'key_names' : batch.get('key_names'),
                                         'table_version': batch.get('table_version')}
        records_by_stream[stream]['messages'] += batch['messages']
    return records_by_stream

def examine_target_output_for_fields(target_output_file):
    fields_by_stream = defaultdict(set)
    for batch in target_output_file:
        stream = batch.get('table_name')
        for message in batch.get('messages'):
            if message['action'] == 'upsert':
                fields_by_stream[stream].update(set(message.get("data", {}).keys()))
    return fields_by_stream

def examine_target_output_file(target_output_file):
    messages_per_stream = {}
    fields_by_stream = get_records_from_target_output(target_output_file)

    return {stream: len(value['messages']) for stream, value in fields_by_stream.items() }
