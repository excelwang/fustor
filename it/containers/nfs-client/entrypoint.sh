#!/bin/bash
set -e

# Wait for NFS server to be ready
echo "Waiting for NFS server at ${NFS_SERVER}:${NFS_PATH}..."
until showmount -e "${NFS_SERVER}" 2>/dev/null | grep -q "${NFS_PATH}"; do
    sleep 1
done
echo "NFS server is ready."

# Mount NFS share
echo "Mounting NFS share..."
mount -t nfs -o "${MOUNT_OPTIONS}" "${NFS_SERVER}:${NFS_PATH}" "${MOUNT_POINT}"
echo "NFS mounted at ${MOUNT_POINT}"

# Start Agent if enabled
if [ "${AGENT_ENABLED}" = "true" ]; then
    echo "Starting Fustor Agent (${AGENT_ID})..."
    
    # Create agent config directory
    mkdir -p /root/.fustor/agent-config
    
    # Copy and process config template with environment variable substitution
    # The config file is mounted from it/config/agent-config/default.yaml
    if [ -f "/config/agent-config/default.yaml" ]; then
        # Substitute environment variables in the config
        # Uses gettext-base (envsubst) installed in Dockerfile
        envsubst < /config/agent-config/default.yaml > /root/.fustor/agent-config/default.yaml
        echo "Agent config loaded and processed from mounted volume"
    else
        echo "ERROR: Agent config file not found at /config/agent-config/default.yaml"
        exit 1
    fi
    
    # Start agent in foreground
    echo "Starting Fustor Agent (${AGENT_ID}) in foreground..."
    exec fustor-agent start
fi

# Keep container running if Agent was not started
echo "Container ready. Entering idle loop..."
exec tail -f /dev/null
