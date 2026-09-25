"""ASSERTION INTENT (Black-box tests — public traits and APIs only). Do not introduce white-box testing logic or internal workarounds."""
import base64
import copy
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]


class NativeSourceAdmission(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.reader = Path(os.environ.get("FUSTOR_INSPECT_BIN", ROOT / "target/debug/fustor-source-inspect"))
        cls.view = Path(os.environ.get("FUSTOR_DATA_VIEW_BIN", ROOT / "target/debug/fustor-data-view-inspect"))
        if "FUSTOR_INSPECT_BIN" not in os.environ or "FUSTOR_DATA_VIEW_BIN" not in os.environ:
            subprocess.run(["scripts/resource-guard.sh", "scripts/process-tree-watchdog.sh", "cargo", "build",
                            "-p", "fustor-source-inspect", "-p", "fustor-data-view-inspect"], cwd=ROOT, check=True)

    def call(self, binary, args, request):
        return subprocess.run([str(binary), *args], input=json.dumps(request).encode(),
                              capture_output=True, timeout=30)

    def view_call(self, mode, request=None, expected=True):
        reply = self.call(self.view, [mode, "--config", str(self.config_path)], request or {})
        self.assertEqual(reply.returncode == 0, expected, reply.stderr.decode())
        return json.loads(reply.stdout if expected else reply.stderr)

    def save_config(self):
        self.config_path.write_text(json.dumps(self.config))

    def scoped(self):
        return self.view_call("scoped")["response"]["payload"]["projected"]

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="fustor-native-admission-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "sample.jsonl").write_text('{"id":1,"name":"A"}\n{"id":2,"name":"B"}\n')
        self.source_path = self.root / "source.json"
        self.source_path.write_text(json.dumps({"default_page_size":10, "max_page_size":10, "field_sample_limit":2,
            "endpoints":[{"object_ref":"sample", "root_path":str(self.root), "path_scopes":["sample.jsonl"], "active":True}],
            "__cnx_runtime":{"resource_grants":[{"resource_kind":"jsonl", "object_ref":"sample", "interfaces":["read"],
                "path_scopes":["sample.jsonl"], "grant_epoch":1, "grant_state":"active"}]}}))
        self.limits = dict(max_rows=10, max_bytes=262144, max_calls=3, max_fanout=1, max_latency_ms=5000, max_cost_units=3)
        now = time.time_ns() // 1_000_000
        self.request = {"wire_version":2, "context":{"request_id":"discovery-observation", "grant_epoch":1,
            "issued_at_ms":now, "deadline_ms":now+10000, "limits":self.limits},
            "request":{"object_ref":"sample", "operation":"snapshot-page", "path":"sample.jsonl", "limit":10}}
        # Discovery seed comes from an actual native read, not a fabricated receipt.
        probe = self.call(self.reader, ["read", "--kind", "jsonl", "--config", str(self.source_path)], self.request)
        self.assertEqual(probe.returncode, 0, probe.stderr)
        receipt = json.loads(probe.stdout)["response"]["receipt"]
        self.authority = "authority:jsonl:sample:sample.jsonl"
        self.config_path = self.root / "dataview.json"
        self.config = {"data_view_id":"native-source-test", "state_path":str(self.root/"state.json"), "allowed_state_root":str(self.root),
            "source_policies":[{"profile_id":"sample-fields", "source_kind":"jsonl", "source_authority_ref":self.authority, "authority_object_ref":"sample",
                "producer_id":"native-jsonl", "policy_context_digest":"test-policy-v1", "policy_labels":["test"], "max_ttl_ms":30000}],
            "scoped_request":{"revision_selector":{"kind":"latest"}, "now_ms":0,
                "access":{"subject_scope_id":"test:operator", "tenant_id":"test", "principal_id":"operator", "purpose":"inspection",
                    "permitted_source_authority_refs":[self.authority], "visible_policy_labels":["test"], "current_grant_epoch":1,
                    "allow_stale_but_plannable":False, "coverage_evidence":"complete-for-scope",
                    "discovery_budget":{"time_budget_ms":30000, "remaining_time_ms":30000, "remaining_calls":10,
                        "remaining_rows":100, "remaining_bytes":1048576, "remaining_fanout":10, "remaining_cost_units":10},
                    "disclosure_budget":{"max_results":20, "max_payload_bytes":262144, "max_relationships":20,
                        "include_payloads":True, "include_evidence_refs":True}},
                "discovery_request":{"discovery_intent":{"task":"inspect sample structure", "priorities":[]},
                    "policy_context":{"user_id":"operator", "tenant_id":"test", "task_purpose":"inspection",
                        "allowed_source_instances":[{"source_kind":"jsonl", "source_instance":"sample"}], "grant_epoch":1, "request_context":{}},
                    "discovery_scope":{"source_instances":[], "max_fragments":5}, "time_budget_ms":30000,
                    "disclosure_budget":{"max_fragments":5, "max_names":5, "max_relation_hints":5},
                    "candidate_fragments":[{"source_kind":"jsonl", "source_instance":"sample", "local_id":"sample.jsonl",
                        "node_kind":"collection", "native_ref":str(self.root/"sample.jsonl"), "display_name":"sample.jsonl",
                        "native_pointer":str(self.root/"sample.jsonl"), "fingerprint":receipt["source_revision"],
                        "observed_at_ms":receipt["observed_at_ms"], "grant_epoch":receipt["grant_epoch"]}],
                    "source_capabilities":[{"source_kind":"jsonl", "source_instance":"sample", "operations":["field-discovery"],
                        **{k:v for k,v in self.limits.items() if k != "max_cost_units"}}], "scoped_data_view":None}}}
        self.save_config()

    def admitted_carrier(self):
        before = self.scoped()
        actions = before["discovery_frontier"]["ranked_actions"]
        self.assertEqual(len(actions), 1, before)
        action = actions[0]["action"]
        issued = self.view_call("action-issue", {"profile_id":"sample-fields", "action_id":action["action_id"], "ttl_ms":30000})
        self.assertEqual(issued["response"]["kind"], "action-execution-context", issued)
        context = issued["response"]["payload"]
        request = copy.deepcopy(self.request)
        request["context"].update(request_id=context["execution_id"], issued_at_ms=context["issued_at_ms"], deadline_ms=context["expires_at_ms"])
        request["request"]["operation"] = "discover-fields"
        wire = self.call(self.reader, ["read-wire", "--kind", "jsonl", "--config", str(self.source_path)], request)
        self.assertEqual(wire.returncode, 0, wire.stderr)
        return {"profile_id":"sample-fields", "submission_id":"native-fields", "execution":context,
                "response_base64":base64.b64encode(wire.stdout).decode()}

    # @verify_spec("CONTRACTS.DATA_VIEW.DEMAND_MATERIALIZATION", mode="system")
    # @verify_spec("CONTRACTS.DATA_VIEW.RESULT_NORMALIZATION_OWNERSHIP", mode="system")
    def test_original_native_result_forms_relationships_and_replays_without_new_revision(self):
        carrier = self.admitted_carrier()
        accepted = self.view_call("source-submit", carrier)
        self.assertEqual(accepted["response"]["payload"]["status"], "accepted", accepted)
        view = self.scoped()
        self.assertEqual(len(view["visible_results"]), 3, view)
        self.assertEqual(len(view["visible_relationships"]), 2, view)
        identities = {node["result_ref"] for node in view["visible_results"]}
        for edge in view["visible_relationships"]:
            self.assertEqual(edge["relationship_kind"], "has-field")
            self.assertIn(edge["source_result_ref"], identities)
            self.assertIn(edge["target_result_ref"], identities)
        self.assertEqual(self.view_call("source-submit", carrier), accepted)
        reopened = self.scoped()  # separate native process/reopen each time
        for field in ("data_view", "visible_results", "visible_relationships"):
            self.assertEqual(reopened[field], view[field])

    # @verify_spec("CONTRACTS.DATA_VIEW.EVIDENCE_GATED_ATOMIC_COMMIT", mode="system")
    def test_corrupted_context_carrier_and_revoked_scope_cannot_advance_head(self):
        carrier = self.admitted_carrier()
        before = self.scoped()
        state = (self.root / "state.json").read_bytes()
        invalid = copy.deepcopy(carrier)
        invalid["execution"]["execution_id"] += "-wrong"
        self.view_call("source-submit", invalid, expected=False)
        invalid = copy.deepcopy(carrier)
        raw = bytearray(base64.b64decode(invalid["response_base64"]))
        raw[-1] ^= 1
        invalid["response_base64"] = base64.b64encode(raw).decode()
        self.view_call("source-submit", invalid, expected=False)
        self.assertEqual((self.root/"state.json").read_bytes(), state)
        self.assertEqual(self.scoped()["data_view"], before["data_view"])
        accepted = self.view_call("source-submit", carrier)
        self.assertEqual(accepted["response"]["payload"]["status"], "accepted", accepted)
        self.config["scoped_request"]["access"]["permitted_source_authority_refs"] = []
        self.save_config()
        denied = self.scoped()
        self.assertEqual(denied["visible_results"], [])
        self.assertEqual(denied["visible_relationships"], [])
        self.view_call("source-submit", carrier, expected=False)


if __name__ == "__main__":
    unittest.main()
