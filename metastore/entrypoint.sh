#!/bin/bash
set -e
envsubst < /opt/hive/conf/hive-site.xml.template > /opt/hive/conf/hive-site.xml
envsubst < /opt/hive/conf/core-site.xml.template > /opt/hive/conf/core-site.xml
cp /opt/hive/conf/core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
exec /entrypoint.sh "$@"
