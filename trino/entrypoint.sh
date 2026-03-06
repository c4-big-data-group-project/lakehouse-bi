#!/bin/bash
set -e
envsubst < /etc/trino/config.properties.template > /etc/trino/config.properties

# Copy files from the catalog directory, applying
# envsubst when the files end with ".template".
for template in /etc/trino/catalog/*.properties.template; do
    [ -f "$template" ] || continue
    output="${template%.template}"
    echo "INFO: Generating file (substituting environment variables): $(basename "$output")"
    envsubst < "$template" > "$output"
done

exec /usr/lib/trino/bin/launcher run --etc-dir=/etc/trino "$@"