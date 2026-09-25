from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest


ROOT = Path(__file__).resolve().parents[2]
CLI = ROOT / "tools/fustor_boundary.py"


class FustorBoundaryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.store = self.root / "dataview"
        self.config = self.root / "config.json"
        self.config.write_text(
            json.dumps(
                {
                    "schemaVersion": 1,
                    "dataView": {"dataViewId": "test-view", "storePath": str(self.store)},
                    "declaredAdmissions": [
                        {
                            "profileId": "external-result-v1",
                            "producerId": "test-producer",
                            "schemaRef": "schema://example/result",
                            "schemaVersion": "1",
                            "externalRefPrefixes": ["file:///candidate/"],
                            "maxManifestBytes": 8192,
                            "maxExternalSizeBytes": 1048576,
                            "maxContextTtlMs": 60000,
                            "policyContextDigest": "policy:test",
                            "grantEpoch": 1,
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def invoke(self, command: str, request: dict) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                sys.executable,
                str(CLI),
                command,
                "--config",
                str(self.config),
                "--request-json",
                "-",
            ],
            input=json.dumps(request),
            capture_output=True,
            text=True,
            check=False,
        )

    def issue(self) -> dict:
        result = self.invoke(
            "admission-issue",
            {
                "requestId": "request-1",
                "profileId": "external-result-v1",
                "idempotencyKey": "stable-key",
                "policyContextDigest": "policy:test",
                "grantEpoch": 1,
                "ttlMs": 30000,
            },
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)

    def manifest(self) -> dict:
        return {
            "externalRef": "file:///candidate/result.json",
            "externalRevision": "result:sha256:" + "a" * 64,
            "digestAlgorithm": "sha256",
            "digest": "b" * 64,
            "sizeBytes": 123,
            "schemaRef": "schema://example/result",
            "schemaVersion": "1",
            "mediaType": "application/json",
            "producerId": "test-producer",
            "producerVersion": "1.0.0",
            "lineageRefs": ["input:sha256:" + "c" * 64],
            "evidenceRefs": ["trace:sha256:" + "d" * 64],
            "validationRefs": ["query:test"],
            "observedOrProducedAt": "2026-09-05T00:00:00Z",
            "policyLabels": ["candidate-only"],
            "freshness": "fresh",
            "retention": {"mode": "candidate-cleanup"},
        }

    def test_declared_result_is_atomic_idempotent_and_manifest_only(self) -> None:
        context = self.issue()
        request = {
            "submissionId": "submission-1",
            "admission": context,
            "manifest": self.manifest(),
            "derivation": {"kind": "external-workflow", "dependencyRefs": ["coox:run:test"]},
        }
        first = self.invoke("declared-submit", request)
        second = self.invoke("declared-submit", request)
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertEqual(json.loads(first.stdout), json.loads(second.stdout))
        self.assertEqual(json.loads(first.stdout)["status"], "accepted")

        commits = list((self.store / "commits").glob("*.json"))
        self.assertEqual(len(commits), 1)
        commit = json.loads(commits[0].read_text(encoding="utf-8"))
        self.assertEqual(commit["completeManifestAttachment"], request["manifest"])
        self.assertFalse((self.store / "result.json").exists())

    def test_rejection_leaves_head_unchanged(self) -> None:
        context = self.issue()
        head_before = (self.store / "head.json").read_bytes()
        manifest = self.manifest()
        manifest["graphAssertions"] = [{"from": "caller", "to": "owned"}]
        result = self.invoke(
            "declared-submit",
            {
                "submissionId": "submission-bad",
                "admission": context,
                "manifest": manifest,
                "derivation": {"kind": "external-workflow", "dependencyRefs": ["coox:run:test"]},
            },
        )
        self.assertEqual(result.returncode, 2)
        self.assertEqual(json.loads(result.stderr)["errorCode"], "declared-manifest-forbidden-field")
        self.assertEqual((self.store / "head.json").read_bytes(), head_before)
        self.assertFalse((self.store / "commits").exists())

    def test_graph_request_rejects_raw_query_before_driver_access(self) -> None:
        value = json.loads(self.config.read_text(encoding="utf-8"))
        value["graphSources"] = [
            {
                "objectRef": "graph-1",
                "endpointUri": "bolt://127.0.0.1:9",
                "statusUrl": "http://127.0.0.1:9/status",
                "grantEpoch": 1,
                "credential": {"usernameEnv": "NO_USER", "passwordEnv": "NO_PASSWORD"},
                "queries": [],
            }
        ]
        self.config.write_text(json.dumps(value), encoding="utf-8")
        result = self.invoke(
            "graph-observe",
            {
                "requestId": "graph-request",
                "objectRef": "graph-1",
                "configuredQueryId": "anything",
                "query": "MATCH (n) RETURN n",
                "expectedResultSchemaRef": "schema://example",
                "parameters": {},
                "grantEpoch": 1,
                "deadlineMs": int(time.time() * 1000) + 1000,
                "limits": {
                    "maxRows": 1,
                    "maxBytes": 100,
                    "maxCalls": 1,
                    "maxFanout": 1,
                    "maxLatencyMs": 100,
                    "maxCostUnits": 1,
                },
            },
        )
        self.assertEqual(result.returncode, 2)
        self.assertEqual(json.loads(result.stderr)["errorCode"], "raw-graph-query-forbidden")


if __name__ == "__main__":
    unittest.main()
