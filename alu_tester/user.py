from copy import deepcopy
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
