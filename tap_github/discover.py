import singer
from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_github.schema import get_schemas

LOGGER = singer.get_logger()

def discover(client):
    """
    Run the discovery mode, prepare the catalog file and return catalog.
    """
    # Check credential in the discover mode.
    client.verify_access_for_repo()

    schemas, field_metadata = get_schemas()
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        try:
            schema = Schema.from_dict(schema_dict)
            mdata = field_metadata[stream_name]
        except Exception as err:
            LOGGER.error(err)
            LOGGER.error('stream_name: %s', stream_name)
            LOGGER.error('type schema_dict: %s', type(schema_dict))
            raise err

        key_properties = metadata.to_map(mdata).get((), {}).get('table-key-properties')

        catalog.streams.append(CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=key_properties,
            schema=schema,
            metadata=mdata
        ))

    return catalog.to_dict()
