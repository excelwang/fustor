#!/bin/bash
set -e

CONFIG_SRC="/config/fusion-config"
CONFIG_DST="/root/.fustor/fusion-config"

if [ -d "$CONFIG_SRC" ]; then
    echo "Processing Fusion configuration templates from $CONFIG_SRC to $CONFIG_DST..."
    mkdir -p "$CONFIG_DST"
    
    # Check if there are any YAML files
    shopt -s nullglob
    templates=("$CONFIG_SRC"/*.yaml)
    
    if [ ${#templates[@]} -gt 0 ]; then
        for template in "${templates[@]}"; do
            if [ -f "$template" ]; then
                filename=$(basename "$template")
                echo "Processing $filename..."
                envsubst < "$template" > "$CONFIG_DST/$filename"
            fi
        done
    else
        echo "No YAML configuration templates found in $CONFIG_SRC"
    fi
fi

exec "$@"
