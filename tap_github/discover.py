import singer
from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_github.schema import get_schemas
from tap_github.streams import STREAMS

LOGGER = singer.get_logger()


def _build_access_check_url(base_url, stream_obj, repo_path, org):
    """
    Build a minimal URL to probe whether a stream endpoint is accessible.
    Uses per_page=1 to keep the response small.
    """
    # Strip any existing query parameters so we control the query string.
    base_path = stream_obj.path.split('?')[0]
    if stream_obj.use_organization:
        url = '{}/{}'.format(base_url, base_path).format(org)
    else:
        url = '{}/repos/{}/{}'.format(base_url, repo_path, base_path)
    return url + '?per_page=1'


def _is_stream_accessible(stream_name, inaccessible_streams):
    """
    Recursively check whether a stream or any of its ancestors is inaccessible.
    Returns False if the stream itself or any ancestor appears in inaccessible_streams.
    """
    if stream_name in inaccessible_streams:
        return False
    parent = STREAMS[stream_name]().parent
    if parent:
        return _is_stream_accessible(parent, inaccessible_streams)
    return True


def discover(client):
    """
    Run the discovery mode, prepare the catalog file and return catalog.
    Streams whose API endpoints are not accessible (403/404) are excluded.
    """
    # Check credential in the discover mode.
    client.verify_access_for_repo()

    repositories, organizations = client.extract_repos_from_config()
    # Use the first repo and org to probe each stream's endpoint.
    repo_path = repositories[0] if repositories else None
    org = next(iter(organizations)) if organizations else None

    # Identify top-level streams (no parent) that are not accessible.
    inaccessible_streams = set()
    if repo_path:
        for stream_name, stream_class in STREAMS.items():
            stream_obj = stream_class()
            if stream_obj.parent is None:
                test_url = _build_access_check_url(client.base_url, stream_obj, repo_path, org)
                if not client.check_stream_accessible(stream_name, test_url):
                    inaccessible_streams.add(stream_name)
                    LOGGER.warning(
                        "Stream '%s' will be excluded from the catalog: "
                        "insufficient permissions or resource not found.",
                        stream_name
                    )

    schemas, field_metadata = get_schemas()
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        # Exclude streams that are inaccessible or whose ancestor is inaccessible.
        if not _is_stream_accessible(stream_name, inaccessible_streams):
            if stream_name not in inaccessible_streams:
                LOGGER.warning(
                    "Stream '%s' will be excluded from the catalog: "
                    "parent stream is not accessible.",
                    stream_name
                )
            continue

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
