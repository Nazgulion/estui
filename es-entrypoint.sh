#!/bin/bash
echo 'reindex.remote.whitelist: "es-source:9200,es-target:9200"' >> /usr/share/elasticsearch/config/elasticsearch.yml
exec /usr/local/bin/docker-entrypoint.sh "$@"
