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
    
    # Create agent configuration
    mkdir -p /data/agent
    cat > /data/agent/config.yaml << EOF
agent:
  id: "${AGENT_ID}"
  host: "0.0.0.0"
  port: ${AGENT_PORT}
  data_dir: "/data/agent"

sources:
  - name: "shared-fs"
    driver: "source-fs"
    config:
      watch_paths:
        - "${MOUNT_POINT}"
      scan_interval: 60
      audit_interval: 300

pushers:
  - name: "fusion"
    driver: "fusion"
    config:
      endpoint: "${FUSION_ENDPOINT}"
      api_key: "${API_KEY}"
      view_id: "${VIEW_ID}"
EOF

    # Start agent in foreground
    echo "Starting Fustor Agent (${AGENT_ID}) in foreground..."
    exec fustor-agent start --config /data/agent/config.yaml
fi

# Keep container running if Agent was not started
echo "Container ready. Entering idle loop..."
exec tail -f /dev/null
