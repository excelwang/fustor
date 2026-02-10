#!/bin/bash
# scripts/publish_to_pypi.sh
# Robust PyPI publishing script for Fustor Monorepo

set -e

# --- Configuration & Safety Checks ---

DRY_RUN=false
if [[ "$1" == "--dry-run" ]]; then
    DRY_RUN=true
    echo "!!! DRY RUN MODE ENABLED - No packages will be uploaded !!!"
fi

echo "Starting PyPI publishing process for Fustor monorepo..."

# 1. Ensure Git state is clean (No uncommitted changes)
if [[ $(git status --porcelain) ]]; then
    echo "Error: Git working directory is not clean. Please commit or stash changes before publishing."
    echo "This is critical to prevent 'dirty' version tags."
    exit 1
fi

# 2. Determine Unified Version
# We use the latest git tag as the source of truth for all packages in this run.
# If not on a tag, we fail (Production safety).
CURRENT_TAG=$(git describe --tags --exact-match 2>/dev/null || echo "")

if [ -z "$CURRENT_TAG" ]; then
    echo "Error: Current commit is not tagged. Production releases must be initiated from a Git Tag."
    echo "Please tag your commit: git tag -a v0.8.4 -m 'Release v0.8.4' && git push --tags"
    # For development/testing, you can skip this check by commenting it out, 
    # but the pretended version might be less predictable.
    exit 1
fi

# Strip 'v' prefix if present for Python versioning standards
PUBLISH_VERSION=$(echo "$CURRENT_TAG" | sed 's/^v//')
export SETUPTOOLS_SCM_PRETEND_VERSION="$PUBLISH_VERSION"

echo "----------------------------------------------------"
echo "Unified Release Version: $PUBLISH_VERSION"
echo "Git Tag: $CURRENT_TAG"
echo "----------------------------------------------------"

# Function to build and publish a package
publish_package() {
    local package_dir="$1"
    local package_name="$2"
    echo ">>> Processing package: $package_name in $package_dir"

    if [ ! -d "$package_dir" ]; then
        echo "Error: Directory $package_dir not found. Skipping."
        return 1
    fi

    pushd "$package_dir" > /dev/null

    # Cleanup old artifacts
    rm -rf dist/ build/ *.egg-info

    # Build using uv (respecting the PRETEND_VERSION)
    echo "Building $package_name@$PUBLISH_VERSION..."
    uv build --out-dir dist

    if [ $? -ne 0 ]; then
        echo "Error: Build failed for $package_name."
        popd > /dev/null
        exit 1
    fi

    # Publish
    if [ "$DRY_RUN" = true ]; then
        echo "[DRY-RUN] Would publish $package_name to PyPI."
    else
        echo "Uploading $package_name to PyPI..."
        # We use --skip-existing to allow the script to be re-run if it fails midway
        # without failing on already uploaded packages.
        uv publish --skip-existing dist/*
    fi

    if [ $? -ne 0 ]; then
        echo "Error: Publish failed for $package_name."
        popd > /dev/null
        exit 1
    fi

    echo "Successfully processed $package_name."
    popd > /dev/null
    echo ""
}

# --- Execution Phases ---

# Phase 1: Foundation
echo "--- Phase 1: Foundation ---"
publish_package "core" "fustor-core"
publish_package "extensions/schema-fs" "fustor-schema-fs"

# Phase 2: Connectors & Utilities
echo "--- Phase 2: Connectors & Utilities ---"
publish_package "extensions/sender-echo" "fustor-sender-echo"
publish_package "extensions/sender-openapi" "fustor-sender-openapi"
publish_package "extensions/source-elasticsearch" "fustor-source-elasticsearch"
publish_package "extensions/source-mysql" "fustor-source-mysql"
publish_package "extensions/source-oss" "fustor-source-oss"
publish_package "extensions/view-fs" "fustor-view-fs"
publish_package "extensions/receiver-http" "fustor-receiver-http"

# Phase 3: SDKs
echo "--- Phase 3: SDKs & Core Connectors ---"
publish_package "extensions/source-fs" "fustor-source-fs"
publish_package "fusion-sdk" "fustor-fusion-sdk"
publish_package "agent-sdk" "fustor-agent-sdk"
publish_package "demo" "fustor-demo"

# Phase 4: Main Services
echo "--- Phase 4: Final Services ---"
publish_package "extensions/sender-http" "fustor-sender-http"
publish_package "fusion" "fustor-fusion"
publish_package "agent" "fustor-agent"
publish_package "benchmark" "fustor-benchmark"

echo "----------------------------------------------------"
echo "Successfully published all packages version $PUBLISH_VERSION"
echo "----------------------------------------------------"