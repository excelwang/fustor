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
        envsubst < /config/agent-config/default.yaml > /root/.fustor/agent-config/default.yaml
        echo "Agent config loaded from mounted volume"
    else
        # Fallback: generate minimal config inline
        cat > /root/.fustor/agent-config/default.yaml <<EOF
# Minimal Agent Config for ${AGENT_ID}
sources:
  shared-fs:
    driver: fs
    uri: "${MOUNT_POINT}"
    credential:
      type: passwd
      user: root
      password: ""
    disabled: false

senders:
  fusion-main:
    driver: http
    uri: "${FUSION_ENDPOINT}"
    credential:
      type: api_key
      key: "${API_KEY:-test-api-key-123}"
    disabled: false

pipes:
  "${PIPE_ID:-integration-test-ds}":
    source: shared-fs
    sender: fusion-main
    disabled: false
    audit_interval_sec: 300.0
    sentinel_interval_sec: 120.0
    heartbeat_interval_sec: 10.0
EOF
        echo "Agent config generated from fallback template"
    fi
    
    # Start agent in foreground
    echo "Starting Fustor Agent (${AGENT_ID}) in foreground..."
    exec fustor-agent start
fi

# Keep container running if Agent was not started
echo "Container ready. Entering idle loop..."
exec tail -f /dev/null
