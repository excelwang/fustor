#!/bin/bash
set -e

echo "Starting PyPI publishing process for Fustor monorepo..."

# Function to build and publish a package
publish_package() {
    local package_dir="$1"
    local package_name="$2"
    echo "----------------------------------------------------"
    echo "Processing package: $package_name in $package_dir"
    echo "----------------------------------------------------"

    if [ ! -d "$package_dir" ]; then
        echo "Error: Directory $package_dir not found. Skipping."
        return 1
    fi

    pushd "$package_dir" > /dev/null

    echo "Cleaning up old build artifacts..."
    rm -rf dist/ build/ *.egg-info

    echo "Building package: $package_name..."
    # Force output to local dist/ directory to ensure the publish step finds it
    uv build --out-dir dist

    if [ $? -ne 0 ]; then
        echo "Error: Build failed for $package_name. Aborting."
        popd > /dev/null
        exit 1
    fi

    # Check if dist directory has files
    if [ -z "$(ls -A dist)" ]; then
       echo "Error: Build succeeded but 'dist' directory is empty. Cannot publish."
       popd > /dev/null
       exit 1
    fi

    echo "Publishing package: $package_name to PyPI..."
    # Ensure you are logged in to PyPI or have credentials configured (e.g., via ~/.pypirc)
    uv publish dist/*

    if [ $? -ne 0 ]; then
        echo "Error: Publish failed for $package_name. Aborting."
        popd > /dev/null
        exit 1
    fi

    echo "Successfully processed $package_name."
    popd > /dev/null
    echo ""
}

# --- Phase 1: Foundation (No Internal Dependencies) ---
echo "--- Phase 1: Publishing Foundation Packages ---"
publish_package "packages/core" "fustor-core"
publish_package "packages/common" "fustor-common"
publish_package "packages/event-model" "fustor-event-model"

# --- Phase 2: Clients & Basic Connectors ---
echo "--- Phase 2: Publishing Clients & Basic Connectors ---"

publish_package "packages/pusher-echo" "fustor-pusher-echo"
publish_package "packages/pusher-openapi" "fustor-pusher-openapi"
publish_package "packages/source-elasticsearch" "fustor-source-elasticsearch"
publish_package "packages/source-mysql" "fustor-source-mysql"
publish_package "packages/source-oss" "fustor-source-oss"
publish_package "packages/view-fs" "fustor-view-fs"

# --- Phase 3: SDKs & Advanced Connectors ---
echo "--- Phase 3: Publishing SDKs & Advanced Connectors ---"
publish_package "packages/source-fs" "fustor-source-fs"
publish_package "packages/fusion-sdk" "fustor-fusion-sdk"
publish_package "packages/agent-sdk" "fustor-agent-sdk"
publish_package "packages/demo" "fustor-demo"

# --- Phase 4: Services & Complex Integrations ---
echo "--- Phase 4: Publishing Services & Complex Integrations ---"
publish_package "packages/pusher-fusion" "fustor-pusher-fusion"
publish_package "fusion" "fustor-fusion"
publish_package "agent" "fustor-agent"
publish_package "benchmark" "fustor-benchmark"


echo "----------------------------------------------------"
echo "PyPI publishing script finished."
echo "----------------------------------------------------"
