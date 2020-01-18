"""
The CLI entrypoint for the tap
"""

import singer

from . import discover
from . import sync

REQUIRED_CONFIG_KEYS = ["access_token", "endpoints"]

logger = singer.get_logger()

@singer.utils.handle_top_exception(logger)
def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.discover:
        discover.do_discover()
    else:
        catalog = args.properties if args.properties else discover.get_catalog()
        sync.do_sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
