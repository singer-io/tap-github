import singer
from singer import metadata
import collections
import requests
import singer.metrics as metrics
import singer.bookmarks as bookmarks
from singer.utils import strptime_to_utc

class DependencyException(Exception):
    pass

session = requests.Session()
logger = singer.get_logger()

DOCS_URL = "https://github.com/aaronsteers/tap-rest-api"
DEFAULT_TIME_KEY = "since"


def get_selected_streams(catalog):
    """
    Gets selected streams.  Checks schema's 'selected'
    first -- and then checks metadata, looking for an empty
    breadcrumb and mdata with a 'selected' entry
    """
    selected_streams = []
    for stream in catalog["streams"]:
        stream_metadata = stream["metadata"]
        if stream["schema"].get("selected", False):
            selected_streams.append(stream["tap_stream_id"])
        else:
            for entry in stream_metadata:
                # stream metadata will have empty breadcrumb
                if not entry["breadcrumb"] and entry["metadata"].get("selected", None):
                    selected_streams.append(stream["tap_stream_id"])
    return selected_streams



def _parse_endpoint_config(config_dict, config_key, stream_id, default=None):
    if "endpoints" not in config_dict:
        raise RuntimeError(
            f"Missing 'endpoints' config in JSON file. For more info: {DOCS_URL}"
        )
    if stream_id not in config_dict["endpoints"]:
        raise RuntimeError(
            f"Missing endpoint config for '{stream_id}' in JSON file. For more info: {DOCS_URL}"
        )
    return config_dict["endpoints"][stream_id].get(config_key, default)


def _get_endpoint_url(stream_id, config, query_string="", parent_keys=None):
    """
    Arguments:
    ...
      parent_keys: a list of parent key values to inject into the url
                   if parent_keys is provided, url should contain {} placeholders
                   e.g. 'pulls/{}/reviews'
    """
    url = _parse_endpoint_config(
        config_dict=config, config_key="url", stream_id=stream_id
    )
    if not url:
        raise RuntimeError(
            f"Missing 'url' config for '{stream_id}' in JSON file. For more info: {DOCS_URL}"
        )
    url = config["endpoints"][stream_id]["url"]
    if "url_prefix" in config:
        url = f"{config['url_prefix']}{url}"
    if query_string:
        if "?" in url:
            url = f"{url}&{query_string}"
        else:
            url = f"{url}?{query_string}"
    if parent_keys:
        url = url.format(*parent_keys)
    return url


def _get_endpoint_time_key(stream_id, config):
    return _parse_endpoint_config(
        config_key="time_key", config_dict=config, stream_id=stream_id
    )


def _get_endpoint_key(stream_id, config):
    return _parse_endpoint_config(
        config_key="key", config_dict=config, stream_id=stream_id
    )


def _get_endpoint_parents(stream_id, config):
    parents = _parse_endpoint_config(
        config_key="parents", config_dict=config, stream_id=stream_id
    )
    return parents or []


def _get_endpoint_children(stream_id, config):
    result = []
    for possible_child in get_streams(config):
        if possible_child == stream_id:
            continue # ignore self
        if stream_id in _get_endpoint_parents(possible_child, config):
            result.append(possible_child)
    return result


def _get_endpoint_parent_key_alias(stream_id, config):
    return _parse_endpoint_config(
        config_key="parent_key_alias", config_dict=config, stream_id=stream_id
    )


def _get_endpoint_time_filter_query(stream_id, config):
    return _parse_endpoint_config(
        config_key="time_filter_query", config_dict=config, stream_id=stream_id
    )


def sync_rest_data(stream_id, catalog, state, sub_streams=None):
    """
    Generic function to get (sync) data from a REST API endpoint
    Arguments:
    - sub_streams should be None or a list of child stream names to be synced
    """
    config = _get_config()

    schema = _get_schema(stream_id, catalog)
    mdata = _get_metadata(stream_id, catalog)
    stream_key = _get_endpoint_key(stream_id)
    time_key_sort = _parse_endpoint_config(config, "time_key_sort", stream_name)
    time_key = _get_endpoint_time_key(stream_id, config)
    time_filter_query = _get_endpoint_time_filter_query(stream_id, config)
    query_string = None
    bookmark = bookmarks.get_bookmark(
        state=state, tap_stream_id=stream_id, key=time_key or DEFAULT_TIME_KEY
    )
    if bookmark and time_filter_query:
        query_string = time_filter_query.format(bookmark)
    max_timestamp = None
    sub_streams = [
        x for x in _get_endpoint_children(stream_id, config)
        if x in sub_streams or sub_streams == True
    ]
    counters = {
        stream_id: metrics.record_counter(stream_id)
        for stream_id in [stream_id] + sub_streams
    }
    for response in authed_get_all_pages(
        stream_id, _get_endpoint_url(stream_id, config, query_string)
    ):
        elements = response.json()
        extraction_time = singer.utils.now()
        for element in elements:
            timestamp = (
                strptime_to_utc(element[time_key]) if time_key else extraction_time
            )
            max_timestamp = (
                timestamp if not max_timestamp else max(max_timestamp, timestamp)
            )
            if bookmark_value:
                bookmark_time = singer.utils.strptime_to_utc(bookmark_value)
            else:
                bookmark_time = 0
            if time_key_sort == "desc" and bookmark_time:
                if singer.utils.strptime_to_utc(element.get(time_key)) < bookmark_time:
                    logging.info(
                        f"Data from '{stream_name}' is sorted descending on "
                        f"time key '{time_key_name}' and the current record's timestamp "
                        f"('{time_key_value}') is older than the available bookmark "
                        f"('{bookmark_time}'). Skipping remaining records in stream."
                    )
                    return state
            with singer.Transformer() as transformer:
                rec = transformer.transform(
                    element, schema, metadata=metadata.to_map(mdata)
                )
            singer.write_record(stream_id, rec, time_extracted=extraction_time)
            if sub_streams:
                parent_key_value = element[stream_key]
                for child_stream in sub_streams:
                    for element in get_child_elements(
                        stream_id=child_stream,
                        parent_key_value=parent_key_value,
                        catalog=catalog,
                        state=state,
                    ):
                        time_key = _get_endpoint_time_key(child_stream, config)
                        singer.write_record(
                            child_stream,
                            element,
                            time_extracted=extraction_time,
                        )
                        singer.write_bookmark(
                            state,
                            child_stream,
                            child_time_key or DEFAULT_TIME_KEY,
                            singer.utils.strftime(extraction_time),
                        )
                        counters[child_stream].increment()
            singer.write_bookmark(
                state,
                stream_id,
                time_key or DEFAULT_TIME_KEY,
                singer.utils.strftime(max_timestamp),
            )
            counters[stream_id].increment()
    return state


def get_rest_data_child_elements(stream_id, parent_key_value, catalog, state):
    schema = _get_schema(stream_id, catalog)
    mdata = _get_metadata(stream_id, catalog)
    for response in authed_get_all_pages(
        stream_id, _get_endpoint_url(stream_id, config, parent_key=parent_key_value)
    ):
        elements = response.json()
        extraction_time = singer.utils.now()
        for element in elements:
            with singer.Transformer() as transformer:
                rec = transformer.transform(
                    element, schema, metadata=metadata.to_map(mdata)
                )
            yield rec
        return state


def get_stream_from_catalog(stream_id, catalog):
    for stream in catalog["streams"]:
        if stream["tap_stream_id"] == stream_id:
            return stream
    return None


def _get_schema(stream_id, catalog):
    stream = get_stream_from_catalog(stream_id, catalog)
    if not stream:
        raise RuntimeError(f"Could not find '{stream_id}' schema in catalog data.")
    return stream["schema"]


def _get_metadata(stream_id, catalog):
    for stream in catalog["streams"]:
        if stream["tap_stream_id"] == stream_id:
            return stream["metadata"]
    raise RuntimeError(f"Could not find '{stream_id}' metadata in catalog data.")


def do_sync(config, state, catalog):
    access_token = config["access_token"]
    session.headers.update({"authorization": "token " + access_token})
    # get selected streams, make sure stream dependencies are met
    selected_stream_ids = get_selected_streams(catalog)
    _validate_dependencies(selected_stream_ids, config)
    singer.write_state(state)
    logger.info("Starting sync from API")
    for stream in catalog["streams"]:
        stream_id = stream["tap_stream_id"]
        if stream_id not in selected_stream_ids:
            logger.info(f"Skipping stream '{stream_id}' because it is not selected.")
        stream_schema = _get_schema(stream_id, catalog)
        mdata = _get_metadata(stream_id, catalog)
        if _get_endpoint_parents(stream_id, config):
            # if it is a "sub_stream", it will be sync'd by its parent
            continue
        if stream_id in selected_stream_ids:
            singer.write_schema(stream_id, stream_schema, stream["key_properties"])
            sub_stream_ids = [
                x for x in _get_endpoint_children(stream_id, config)
                if x in selected_stream_ids
            ]
            for sub_stream_id in sub_stream_ids:
                sub_stream = get_stream_from_catalog(sub_stream_id, catalog)
                singer.write_schema(
                    sub_stream_id,
                    sub_stream["schema"],
                    sub_stream["key_properties"],
                )
            state = sync_rest_data(stream_id, catalog, state, sub_streams)
            singer.write_state(state)


def _validate_dependencies(selected_stream_ids, config):
    errs = []
    for stream_id in selected_stream_ids:
        parent = _get_endpoint_parents(stream_id, config)
        if parent and parent not in selected_stream_ids:
            errs.append(
                f"Unable to extract '{stream_id}' data. "
                f"To receive '{stream_id}' data, you also need to select '{parent}'."
            )
    if errs:
        raise DependencyException(" ".join(errs))
