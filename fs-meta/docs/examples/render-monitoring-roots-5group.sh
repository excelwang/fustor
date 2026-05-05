commit cee83167236889931affa6d02ecb1c012490c48e
Author: Your Name <you@example.com>
Date:   Sun May 3 08:09:12 2026 +0800

    fs-meta: merge demo assets with env-driven setup

diff --git a/fs-meta/docs/examples/render-monitoring-roots-5group.sh b/fs-meta/docs/examples/render-monitoring-roots-5group.sh
new file mode 100755
index 0000000..e6ab313
--- /dev/null
+++ b/fs-meta/docs/examples/render-monitoring-roots-5group.sh
@@ -0,0 +1,49 @@
+#!/usr/bin/env bash
+set -euo pipefail
+
+if [[ $# -ne 2 ]]; then
+  echo "usage: $0 <template.json> <output.json>" >&2
+  exit 1
+fi
+
+template=$1
+output=$2
+
+required=(
+  FSMETA_DEMO_HOST_1
+  FSMETA_DEMO_HOST_2
+  FSMETA_DEMO_HOST_3
+  FSMETA_DEMO_HOST_4
+  FSMETA_DEMO_HOST_5
+  FSMETA_DEMO_MOUNT_1
+  FSMETA_DEMO_MOUNT_2
+  FSMETA_DEMO_MOUNT_3
+  FSMETA_DEMO_MOUNT_4
+  FSMETA_DEMO_MOUNT_5
+)
+
+for name in "${required[@]}"; do
+  if [[ -z "${!name:-}" ]]; then
+    echo "missing required env: $name" >&2
+    exit 1
+  fi
+done
+
+python3 - "$template" "$output" <<'PY'
+import json
+import os
+import string
+import sys
+
+template_path = sys.argv[1]
+output_path = sys.argv[2]
+
+with open(template_path, "r", encoding="utf-8") as f:
+    rendered = string.Template(f.read()).substitute(os.environ)
+
+data = json.loads(rendered)
+
+with open(output_path, "w", encoding="utf-8") as f:
+    json.dump(data, f, ensure_ascii=False, indent=2)
+    f.write("\n")
+PY
