#/bin/bash

s-tap install tap-rest-api --install_source=./setup.py --plugin_alias=tap-rest-api1
s-tap plan rest-api --select_file=data.select --config_file=tests/github.secret-config.json --catalog_dir=tests
