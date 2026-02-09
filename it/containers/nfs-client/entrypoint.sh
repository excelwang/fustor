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
    
    # Create agent config directory (AgentConfigLoader expects this location)
    mkdir -p /root/.fustor/agent-config
    cat > /root/.fustor/agent-config/default.yaml << EOF
# Unified Agent Config for ${AGENT_ID}

sources:
  shared-fs:
    driver: fs
    uri: "${MOUNT_POINT}"
    credential:
      key: ""
    disabled: false
    driver_params:
      scan_interval: 60
      audit_interval: 300

senders:
  fusion-main:
    driver: fusion
    uri: "${FUSION_ENDPOINT}"
    credential:
      key: "${API_KEY:-integration-test-key}"
    disabled: false
    driver_params:
      view_id: "${VIEW_ID:-integration-test-ds}"

pipes:
  main-sync:
    source: shared-fs
    sender: fusion-main
    disabled: false
    audit_interval_sec: 300
    sentinel_interval_sec: 120
    heartbeat_interval_sec: 10
EOF

    # Start agent in foreground (will load from default.yaml automatically)
    echo "Starting Fustor Agent (${AGENT_ID}) in foreground..."
    exec fustor-agent start
fi

# Keep container running if Agent was not started
echo "Container ready. Entering idle loop..."
exec tail -f /dev/null
