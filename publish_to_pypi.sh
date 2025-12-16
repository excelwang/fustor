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
    # Use 'uv publish' or 'twine upload dist/*'
    # uv publish dist/*

    # For demonstration, I will echo the twine command. Uncomment the line above for actual publishing.
    echo "Command to publish (uncomment 'uv publish' in script to enable): twine upload dist/*"
    # twine upload dist/*


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
publish_package "packages/fustor_common" "fustor-common"
publish_package "packages/fustor_event_model" "fustor-event-model"
publish_package "registry" "fustor-registry"

# --- Phase 2: Clients & Basic Connectors ---
echo "--- Phase 2: Publishing Clients & Basic Connectors ---"
publish_package "packages/fustor_registry_client" "fustor-registry-client"
publish_package "packages/web_ui" "fustor-web-ui"
publish_package "packages/pusher_echo" "fustor-pusher-echo"
publish_package "packages/pusher_openapi" "fustor-pusher-openapi"
publish_package "packages/source_elasticsearch" "fustor-source-elasticsearch"
publish_package "packages/source_mysql" "fustor-source-mysql"
publish_package "packages/source_oss" "fustor-source-oss"

# --- Phase 3: SDKs & Advanced Connectors ---
echo "--- Phase 3: Publishing SDKs & Advanced Connectors ---"
publish_package "packages/source_fs" "fustor-source-fs"
publish_package "packages/fustor_fusion_sdk" "fustor-fusion-sdk"
publish_package "packages/fustor_agent_sdk" "fustor-agent-sdk"
publish_package "packages/demo_bio_fusion" "fustor-demo"

# --- Phase 4: Services & Complex Integrations ---
echo "--- Phase 4: Publishing Services & Complex Integrations ---"
publish_package "packages/pusher_fusion" "fustor-pusher-fusion"
publish_package "fusion" "fustor-fusion"
publish_package "agent" "fustor-agent"

# --- Phase 5: CLI ---
echo "--- Phase 5: Publishing CLI ---"
publish_package "packages/fustor_cli" "fustor-cli"

echo "----------------------------------------------------"
echo "PyPI publishing script finished."
echo "Remember to uncomment 'uv publish' or 'twine upload dist/*' in the script for actual publishing."
echo "----------------------------------------------------"
