#!/bin/bash
# Script to completely clean up the Fustor Integration Test environment.
# Performs a "Cold Start" by stopping all containers and removing volumes.

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "Tearing down Fustor Integration environment at $SCRIPT_DIR..."

cd "$SCRIPT_DIR"
docker compose down -v

echo "Cleanup complete. Next test run will perform a fresh initialization."
