commit cee83167236889931affa6d02ecb1c012490c48e
Author: Your Name <you@example.com>
Date:   Sun May 3 08:09:12 2026 +0800

    fs-meta: merge demo assets with env-driven setup

diff --git a/fs-meta/docs/examples/demo-env.example.sh b/fs-meta/docs/examples/demo-env.example.sh
new file mode 100755
index 0000000..1484bbf
--- /dev/null
+++ b/fs-meta/docs/examples/demo-env.example.sh
@@ -0,0 +1,35 @@
+#!/usr/bin/env bash
+
+# Copy this file to a local, untracked env file and fill values for the target demo.
+# Do not commit the filled env file.
+
+export FSMETA_DEMO_USER="<ssh-user>"
+export FSMETA_DEMO_HOST_1="<node-1-host-or-ip>"
+export FSMETA_DEMO_HOST_2="<node-2-host-or-ip>"
+export FSMETA_DEMO_HOST_3="<node-3-host-or-ip>"
+export FSMETA_DEMO_HOST_4="<node-4-host-or-ip>"
+export FSMETA_DEMO_HOST_5="<node-5-host-or-ip>"
+
+export FSMETA_DEMO_ENTRY_HOST="$FSMETA_DEMO_HOST_2"
+export FSMETA_DEMO_ENTRY_SSH="${FSMETA_DEMO_USER}@${FSMETA_DEMO_ENTRY_HOST}"
+export FSMETA_DEMO_ENTRY_NODE="<entry-node-name>"
+export FSMETA_DEMO_PEER_HOST="$FSMETA_DEMO_HOST_3"
+export FSMETA_DEMO_PEER_SSH="${FSMETA_DEMO_USER}@${FSMETA_DEMO_PEER_HOST}"
+export FSMETA_DEMO_PEER_NODE="<peer-node-name>"
+export FSMETA_DEMO_API_BASE="http://${FSMETA_DEMO_ENTRY_HOST}:18080"
+
+export FSMETA_DEMO_BUILD_ROOT="<fs-meta-source-root-on-entry-node>"
+export FSMETA_DEMO_RUN_ROOT="<capanix-run-root-on-entry-node>"
+export FSMETA_DEMO_CONTROL_SOCKET="${FSMETA_DEMO_RUN_ROOT}/nodes/${FSMETA_DEMO_ENTRY_NODE}/home/core.sock"
+export FSMETA_DEMO_DEPLOY_CONFIG="${FSMETA_DEMO_RUN_ROOT}/fs-meta-embedded.yaml"
+export FSMETA_DEMO_ENTRY_NODE_ENV="${FSMETA_DEMO_RUN_ROOT}/nodes/${FSMETA_DEMO_ENTRY_NODE}/node.env"
+export FSMETA_DEMO_PEER_RUN_ROOT="<capanix-run-root-on-peer-node>"
+export FSMETA_DEMO_PEER_NODE_ENV="${FSMETA_DEMO_PEER_RUN_ROOT}/nodes/${FSMETA_DEMO_PEER_NODE}/node.env"
+export FSMETA_DEMO_PEER_REGISTRY="${FSMETA_DEMO_PEER_RUN_ROOT}/nodes/${FSMETA_DEMO_PEER_NODE}/home/registry.json"
+
+export FSMETA_DEMO_MOUNT_1="<node-1-mount-point>"
+export FSMETA_DEMO_MOUNT_2="<node-2-mount-point>"
+export FSMETA_DEMO_MOUNT_3="<node-3-mount-point>"
+export FSMETA_DEMO_MOUNT_4="<node-4-mount-point>"
+export FSMETA_DEMO_MOUNT_5="<node-5-mount-point>"
+export FSMETA_DEMO_ROOTS_FILE="<local-rendered-monitoring-roots-json>"
