#!/usr/bin/env python3
"""Small configuration-driven executable for Fustor boundary conformance.

This executable deliberately owns no workflow topology and no domain mapping.
It exposes two generic boundaries needed by real-source acceptance:

* bounded, configured, read-only graph observation; and
* evidence-gated admission of an external-result manifest into DataView.

The graph query text and every product-specific schema live in deployment
configuration.  Requests can select only a configured query id.
"""

from __future__ import annotations

import argparse
import base64
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import time
from typing import Any, Callable
import urllib.error
import urllib.request
import uuid


VERSION = "0.1.0"
WIRE_VERSION = 1
SOURCE_WIRE_VERSION = 2
FORBIDDEN_QUERY_WORDS = re.compile(
    r"\b(create|merge|delete|detach|set|remove|drop|alter|load|foreach)\b",
    re.IGNORECASE,
)
FORBIDDEN_REQUEST_KEYS = {
    "query",
    "queryText",
    "statement",
    "cypher",
    "gremlin",
    "mutation",
    "schemaUpload",
    "refresh",
    "load",
    "admin",
}


class BoundaryError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def required_string(value: dict[str, Any], key: str, code: str = "invalid-request") -> str:
    candidate = value.get(key)
    if not isinstance(candidate, str) or not candidate.strip():
        raise BoundaryError(code, f"required string is missing: {key}")
    return candidate.strip()


def required_positive_int(
    value: dict[str, Any], key: str, code: str = "invalid-request"
) -> int:
    candidate = value.get(key)
    if isinstance(candidate, bool) or not isinstance(candidate, int) or candidate <= 0:
        raise BoundaryError(code, f"required positive integer is invalid: {key}")
    return candidate


def load_object(path: Path, code: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BoundaryError(code, f"cannot load JSON object from {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise BoundaryError(code, f"JSON document must be an object: {path}")
    return value


def read_request(path: str) -> dict[str, Any]:
    try:
        raw = sys.stdin.read() if path == "-" else Path(path).read_text(encoding="utf-8")
        value = json.loads(raw)
    except (OSError, json.JSONDecodeError) as exc:
        raise BoundaryError("invalid-request", f"cannot decode request: {exc}") from exc
    if not isinstance(value, dict):
        raise BoundaryError("invalid-request", "request must be a JSON object")
    return value


def atomic_write(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    encoded = canonical_bytes(value) + b"\n"
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(encoded)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def select_by(items: Any, key: str, expected: str, code: str) -> dict[str, Any]:
    if not isinstance(items, list):
        raise BoundaryError(code, f"configuration list is missing for {key}")
    selected = [item for item in items if isinstance(item, dict) and item.get(key) == expected]
    if len(selected) != 1:
        raise BoundaryError(code, f"exactly one configured {key} is required: {expected}")
    return selected[0]


def require_secret(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise BoundaryError("credential-unavailable", f"credential environment is missing: {name}")
    return value


def ensure_read_only(statement: str) -> None:
    compact = " ".join(statement.split())
    if not compact or FORBIDDEN_QUERY_WORDS.search(compact):
        raise BoundaryError("graph-query-not-read-only", "configured graph query is not read-only")
    first = compact.split(" ", 1)[0].lower()
    if first not in {"match", "return", "with", "unwind"}:
        raise BoundaryError("graph-query-not-read-only", "configured graph query has an unsupported entry clause")


def check_graph_limits(request: dict[str, Any], query: dict[str, Any]) -> dict[str, int]:
    limits = request.get("limits")
    if not isinstance(limits, dict):
        raise BoundaryError("invalid-request", "graph request limits must be an object")
    names = (
        "maxRows",
        "maxBytes",
        "maxCalls",
        "maxFanout",
        "maxLatencyMs",
        "maxCostUnits",
    )
    parsed = {name: required_positive_int(limits, name) for name in names}
    configured = {
        "maxRows": required_positive_int(query, "maxRows", "invalid-configuration"),
        "maxBytes": required_positive_int(query, "maxBytes", "invalid-configuration"),
        "maxCalls": 2,
        "maxFanout": required_positive_int(query, "maxFanout", "invalid-configuration"),
        "maxLatencyMs": required_positive_int(query, "maxTimeMs", "invalid-configuration"),
        "maxCostUnits": 2,
    }
    for name in names:
        if parsed[name] > configured[name]:
            raise BoundaryError("bounded-limit-exceeded", f"requested {name} exceeds configuration")
    return parsed


def fetch_status(url: str, user: str, password: str, timeout_seconds: float) -> dict[str, Any]:
    token = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
    request = urllib.request.Request(url, headers={"Authorization": f"Basic {token}"})
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            raw = response.read(1024 * 1024 + 1)
            if len(raw) > 1024 * 1024:
                raise BoundaryError("graph-status-too-large", "graph status exceeds 1 MiB")
            value = json.loads(raw.decode("utf-8"))
    except (OSError, urllib.error.URLError, json.JSONDecodeError) as exc:
        raise BoundaryError("graph-source-unavailable", f"graph status observation failed: {exc}") from exc
    if not isinstance(value, dict):
        raise BoundaryError("graph-source-unavailable", "graph status is not an object")
    return value


def graph_revision(status: dict[str, Any]) -> tuple[str, str]:
    schema = status.get("Schema") or {}
    gremlin = status.get("GremlinServer") or {}
    node = status.get("NodeStatus") or {}
    if not (
        node.get("Healthy") is True
        and gremlin.get("Healthy") is True
        and schema.get("Exists") is True
        and schema.get("Healthy") is True
    ):
        raise BoundaryError("graph-source-not-ready", "graph provider is not in a readable state")
    bounded = {
        "schemaModTime": schema.get("ModTime"),
        "schemaHealthy": schema.get("Healthy"),
        "localCacheStatus": gremlin.get("LocalCacheStatus"),
        "cacheItems": sorted(
            (
                {
                    "name": item.get("Name") or item.get("Id"),
                    "type": item.get("Type"),
                    "state": item.get("State"),
                }
                for item in (gremlin.get("LocalCacheStatusDetail") or [])
                if isinstance(item, dict)
            ),
            key=lambda item: (str(item["type"]), str(item["name"])),
        ),
    }
    evidence_digest = sha256_bytes(canonical_bytes(bounded))
    native = str(schema.get("ModTime") or "").strip()
    revision = f"puppygraph-schema:{native}" if native else f"graph-observation:sha256:{evidence_digest}"
    return revision, evidence_digest


def graph_observe(config: dict[str, Any], request: dict[str, Any]) -> dict[str, Any]:
    if FORBIDDEN_REQUEST_KEYS.intersection(request):
        raise BoundaryError("raw-graph-query-forbidden", "request may select only a configured query id")
    request_id = required_string(request, "requestId")
    object_ref = required_string(request, "objectRef")
    query_id = required_string(request, "configuredQueryId")
    endpoint = select_by(config.get("graphSources"), "objectRef", object_ref, "graph-source-not-configured")
    query = select_by(endpoint.get("queries"), "queryId", query_id, "graph-query-not-configured")
    limits = check_graph_limits(request, query)
    grant_epoch = required_positive_int(request, "grantEpoch")
    if grant_epoch != required_positive_int(endpoint, "grantEpoch", "invalid-configuration"):
        raise BoundaryError("grant-epoch-mismatch", "graph source grant epoch is stale")
    deadline_ms = required_positive_int(request, "deadlineMs")
    started_ms = now_ms()
    if deadline_ms <= started_ms:
        raise BoundaryError("deadline-exceeded", "graph source deadline has expired")
    expected_schema = required_string(request, "expectedResultSchemaRef")
    result_schema = required_string(query, "resultSchemaRef", "invalid-configuration")
    if expected_schema != result_schema:
        raise BoundaryError("result-schema-mismatch", "configured graph result schema does not match request")
    statement = required_string(query, "statement", "invalid-configuration")
    ensure_read_only(statement)
    parameters = request.get("parameters", {})
    if not isinstance(parameters, dict):
        raise BoundaryError("invalid-request", "graph query parameters must be an object")
    allowed_parameters = query.get("allowedParameters", [])
    if not isinstance(allowed_parameters, list) or set(parameters) - set(allowed_parameters):
        raise BoundaryError("graph-parameter-not-configured", "graph query parameter is not configured")

    credential = endpoint.get("credential")
    if not isinstance(credential, dict):
        raise BoundaryError("invalid-configuration", "graph credential reference is missing")
    user = require_secret(required_string(credential, "usernameEnv", "invalid-configuration"))
    password = require_secret(required_string(credential, "passwordEnv", "invalid-configuration"))
    remaining_ms = min(limits["maxLatencyMs"], deadline_ms - started_ms)
    status = fetch_status(
        required_string(endpoint, "statusUrl", "invalid-configuration"),
        user,
        password,
        max(0.1, remaining_ms / 1000),
    )
    source_revision, status_digest = graph_revision(status)

    try:
        from neo4j import GraphDatabase, Query
    except ImportError as exc:
        raise BoundaryError("graph-driver-unavailable", "Neo4j-compatible graph driver is unavailable") from exc
    elapsed_ms = now_ms() - started_ms
    remaining_ms = min(limits["maxLatencyMs"] - elapsed_ms, deadline_ms - now_ms())
    if remaining_ms <= 0:
        raise BoundaryError("deadline-exceeded", "graph source deadline expired before query")
    driver = GraphDatabase.driver(
        required_string(endpoint, "endpointUri", "invalid-configuration"),
        auth=(user, password),
        connection_timeout=max(1, min(10, remaining_ms // 1000)),
        connection_acquisition_timeout=max(1, min(10, remaining_ms // 1000)),
        max_transaction_retry_time=0,
    )
    rows: list[dict[str, Any]] = []
    try:
        with driver.session(database=endpoint.get("database") or "puppygraph") as session:
            result = session.run(Query(statement, timeout=remaining_ms / 1000), **parameters)
            for record in result:
                rows.append(dict(record))
                if len(rows) > limits["maxRows"]:
                    raise BoundaryError("rows-exhausted", "graph result exceeds maxRows")
                if len(canonical_bytes(rows)) > limits["maxBytes"]:
                    raise BoundaryError("bytes-exhausted", "graph result exceeds maxBytes")
    except BoundaryError:
        raise
    except Exception as exc:
        raise BoundaryError("graph-query-failed", f"configured graph query failed: {exc}") from exc
    finally:
        driver.close()

    payload = {
        "schemaRef": result_schema,
        "configuredQueryId": query_id,
        "rows": rows,
    }
    payload_encoded = canonical_bytes(payload)
    if len(payload_encoded) > limits["maxBytes"]:
        raise BoundaryError("bytes-exhausted", "graph result exceeds maxBytes")
    latency_ms = now_ms() - started_ms
    if latency_ms > limits["maxLatencyMs"] or now_ms() > deadline_ms:
        raise BoundaryError("deadline-exceeded", "graph source exceeded maxLatencyMs")
    receipt = {
        "source_kind": "graph",
        "authority_object_ref": object_ref,
        "operation": f"configured-query:{query_id}",
        "source_revision": source_revision,
        "native_anchors": [
            {
                "anchor_kind": "configured-query",
                "native_ref": f"{object_ref}/{query_id}",
            }
        ],
        "evidence_refs": [
            f"graph-status:sha256:{status_digest}",
            f"graph-config:sha256:{sha256_bytes(canonical_bytes(endpoint))}",
        ],
        "observed_at_ms": now_ms(),
        "grant_epoch": grant_epoch,
        "continuation_state": "terminal",
        "actual": {
            "rows": len(rows),
            "bytes": len(payload_encoded),
            "calls": 2,
            "fanout": 1,
            "latency_ms": latency_ms,
            "cost_units": 2,
        },
        "payload_digest": sha256_bytes(payload_encoded),
    }
    receipt["receipt_digest"] = sha256_bytes(canonical_bytes(receipt))
    return {
        "wire_version": SOURCE_WIRE_VERSION,
        "request_id": request_id,
        "payload": payload,
        "receipt": receipt,
    }


def data_view_paths(config: dict[str, Any]) -> tuple[str, Path]:
    data_view = config.get("dataView")
    if not isinstance(data_view, dict):
        raise BoundaryError("invalid-configuration", "dataView configuration is missing")
    data_view_id = required_string(data_view, "dataViewId", "invalid-configuration")
    store = Path(required_string(data_view, "storePath", "invalid-configuration")).resolve()
    if store == Path("/"):
        raise BoundaryError("invalid-configuration", "DataView store path is unsafe")
    return data_view_id, store


def ensure_genesis(data_view_id: str, store: Path) -> dict[str, Any]:
    head_path = store / "head.json"
    if head_path.exists():
        return load_object(head_path, "dataview-store-invalid")
    genesis = {
        "dataViewId": data_view_id,
        "revisionId": f"dv:sha256:{sha256_bytes(canonical_bytes({'dataViewId': data_view_id, 'genesis': True}))}",
    }
    atomic_write(head_path, genesis)
    return genesis


def issue_admission(config: dict[str, Any], request: dict[str, Any]) -> dict[str, Any]:
    profile_id = required_string(request, "profileId")
    profile = select_by(
        config.get("declaredAdmissions"),
        "profileId",
        profile_id,
        "admission-profile-not-configured",
    )
    request_id = required_string(request, "requestId")
    idempotency_key = required_string(request, "idempotencyKey")
    policy_digest = required_string(request, "policyContextDigest")
    if policy_digest != required_string(profile, "policyContextDigest", "invalid-configuration"):
        raise BoundaryError("policy-context-mismatch", "admission policy context does not match")
    grant_epoch = required_positive_int(request, "grantEpoch")
    if grant_epoch != required_positive_int(profile, "grantEpoch", "invalid-configuration"):
        raise BoundaryError("grant-epoch-mismatch", "admission grant epoch is stale")
    ttl_ms = required_positive_int(request, "ttlMs")
    max_ttl_ms = required_positive_int(profile, "maxContextTtlMs", "invalid-configuration")
    if ttl_ms > max_ttl_ms:
        raise BoundaryError("bounded-limit-exceeded", "admission ttl exceeds configuration")

    data_view_id, store = data_view_paths(config)
    head = ensure_genesis(data_view_id, store)
    issued_at = now_ms()
    contract = {
        "wireVersion": WIRE_VERSION,
        "requestId": request_id,
        "profileId": profile_id,
        "producerId": required_string(profile, "producerId", "invalid-configuration"),
        "allowedSchemaRef": required_string(profile, "schemaRef", "invalid-configuration"),
        "allowedSchemaVersion": required_string(profile, "schemaVersion", "invalid-configuration"),
        "allowedExternalRefPrefixes": profile.get("externalRefPrefixes"),
        "maxManifestBytes": required_positive_int(profile, "maxManifestBytes", "invalid-configuration"),
        "maxExternalSizeBytes": required_positive_int(profile, "maxExternalSizeBytes", "invalid-configuration"),
        "baseRevisionId": required_string(head, "revisionId", "dataview-store-invalid"),
        "policyContextDigest": policy_digest,
        "grantEpoch": grant_epoch,
        "issuedAtMs": issued_at,
        "expiresAtMs": issued_at + ttl_ms,
        "idempotencyKey": idempotency_key,
    }
    prefixes = contract["allowedExternalRefPrefixes"]
    if not isinstance(prefixes, list) or not prefixes or not all(isinstance(item, str) and item for item in prefixes):
        raise BoundaryError("invalid-configuration", "externalRefPrefixes must be nonempty strings")
    contract_digest = sha256_bytes(canonical_bytes(contract))
    context = dict(contract)
    context["admissionContractDigest"] = contract_digest
    context["admissionId"] = f"admission:sha256:{contract_digest}"
    idempotency_ref = sha256_bytes(idempotency_key.encode("utf-8"))
    idempotency_path = store / "admission-idempotency" / f"{idempotency_ref}.json"
    if idempotency_path.exists():
        existing = load_object(idempotency_path, "dataview-store-invalid")
        if existing != context:
            raise BoundaryError("idempotency-conflict", "admission idempotency key was reused")
        return existing
    atomic_write(store / "admissions" / f"{contract_digest}.json", context)
    atomic_write(idempotency_path, context)
    return context


def validate_manifest(context: dict[str, Any], manifest: dict[str, Any]) -> bytes:
    forbidden = {
        "sourceReceipt",
        "sourceAnchors",
        "nativeAnchors",
        "sourceRevision",
        "canonicalIdentities",
        "graphAssertions",
        "relationshipAssertions",
    }
    if forbidden.intersection(manifest):
        raise BoundaryError("declared-manifest-forbidden-field", "declared result contains source or DataView-owned fields")
    required = (
        "externalRef",
        "externalRevision",
        "digestAlgorithm",
        "digest",
        "sizeBytes",
        "schemaRef",
        "schemaVersion",
        "mediaType",
        "producerId",
        "producerVersion",
        "lineageRefs",
        "evidenceRefs",
        "validationRefs",
        "observedOrProducedAt",
        "policyLabels",
        "freshness",
        "retention",
    )
    for key in required:
        if key not in manifest:
            raise BoundaryError("declared-manifest-incomplete", f"manifest field is missing: {key}")
    if required_string(manifest, "producerId") != context["producerId"]:
        raise BoundaryError("producer-mismatch", "manifest producer is not admitted")
    if (
        required_string(manifest, "schemaRef") != context["allowedSchemaRef"]
        or required_string(manifest, "schemaVersion") != context["allowedSchemaVersion"]
    ):
        raise BoundaryError("unsupported-schema", "manifest schema is not admitted")
    external_ref = required_string(manifest, "externalRef")
    if not any(external_ref.startswith(prefix) for prefix in context["allowedExternalRefPrefixes"]):
        raise BoundaryError("external-ref-not-admitted", "manifest external reference is outside policy")
    if required_string(manifest, "digestAlgorithm").lower() != "sha256":
        raise BoundaryError("digest-not-supported", "only sha256 manifests are admitted")
    digest_value = required_string(manifest, "digest")
    if re.fullmatch(r"[0-9a-f]{64}", digest_value) is None:
        raise BoundaryError("digest-invalid", "manifest sha256 digest is invalid")
    size_bytes = required_positive_int(manifest, "sizeBytes")
    if size_bytes > context["maxExternalSizeBytes"]:
        raise BoundaryError("bounded-limit-exceeded", "external result size exceeds admission")
    for key in ("lineageRefs", "evidenceRefs", "validationRefs", "policyLabels"):
        value = manifest.get(key)
        if not isinstance(value, list) or not value or not all(isinstance(item, str) and item for item in value):
            raise BoundaryError("declared-manifest-incomplete", f"manifest {key} must be nonempty strings")
    if manifest.get("freshness") != "fresh" or not isinstance(manifest.get("retention"), dict):
        raise BoundaryError("freshness-or-retention-invalid", "manifest freshness or retention is invalid")
    encoded = canonical_bytes(manifest)
    if len(encoded) > context["maxManifestBytes"]:
        raise BoundaryError("bounded-limit-exceeded", "manifest carrier exceeds admission")
    return encoded


def submit_declared(config: dict[str, Any], request: dict[str, Any]) -> dict[str, Any]:
    submission_id = required_string(request, "submissionId")
    context = request.get("admission")
    manifest = request.get("manifest")
    derivation = request.get("derivation")
    if not isinstance(context, dict) or not isinstance(manifest, dict) or not isinstance(derivation, dict):
        raise BoundaryError("invalid-request", "admission, manifest, and derivation objects are required")
    data_view_id, store = data_view_paths(config)
    head = ensure_genesis(data_view_id, store)
    submission_digest = sha256_bytes(canonical_bytes(request))
    submission_ref = sha256_bytes(submission_id.encode("utf-8"))
    submission_path = store / "submissions" / f"{submission_ref}.json"
    if submission_path.exists():
        existing = load_object(submission_path, "dataview-store-invalid")
        if existing.get("submissionDigest") != submission_digest:
            raise BoundaryError("idempotency-conflict", "submission id was reused with different content")
        outcome = existing.get("outcome")
        if not isinstance(outcome, dict):
            raise BoundaryError("dataview-store-invalid", "stored outcome is invalid")
        return outcome

    admission_id = required_string(context, "admissionId")
    admission_digest = required_string(context, "admissionContractDigest")
    if admission_id != f"admission:sha256:{admission_digest}":
        raise BoundaryError("admission-context-mismatch", "admission identity is invalid")
    stored_context = load_object(
        store / "admissions" / f"{admission_digest}.json",
        "admission-context-not-found",
    )
    if stored_context != context:
        raise BoundaryError("admission-context-mismatch", "admission context does not match issued state")
    if required_positive_int(context, "expiresAtMs") <= now_ms():
        raise BoundaryError("admission-context-expired", "admission context expired")
    if required_string(context, "baseRevisionId") != required_string(head, "revisionId", "dataview-store-invalid"):
        raise BoundaryError("base-revision-mismatch", "DataView head changed after admission issuance")
    manifest_encoded = validate_manifest(context, manifest)
    derivation_kind = required_string(derivation, "kind")
    dependency_refs = derivation.get("dependencyRefs")
    if not isinstance(dependency_refs, list) or not dependency_refs:
        raise BoundaryError("declared-manifest-incomplete", "derivation dependencyRefs are required")

    result_material = {
        "schemaRef": manifest["schemaRef"],
        "schemaVersion": manifest["schemaVersion"],
        "externalRef": manifest["externalRef"],
        "externalRevision": manifest["externalRevision"],
        "digest": manifest["digest"],
    }
    result_ref = f"result:sha256:{sha256_bytes(canonical_bytes(result_material))}"
    commit_material = {
        "baseRevisionId": context["baseRevisionId"],
        "submissionDigest": submission_digest,
        "manifestDigest": sha256_bytes(manifest_encoded),
    }
    commit_digest = sha256_bytes(canonical_bytes(commit_material))
    revision = {
        "dataViewId": data_view_id,
        "revisionId": f"dv:sha256:{commit_digest}",
    }
    outcome = {
        "submissionId": submission_id,
        "status": "accepted",
        "dataView": revision,
        "deltaRef": f"delta:sha256:{commit_digest}",
        "manifestDigest": sha256_bytes(manifest_encoded),
    }
    commit = {
        "wireVersion": WIRE_VERSION,
        "admissionId": admission_id,
        "submissionId": submission_id,
        "baseRevision": head,
        "revision": revision,
        "completeManifestAttachment": manifest,
        "normalized": {
            "resultRef": result_ref,
            "entityKind": f"external-result:{manifest['schemaRef']}",
            "dependencyRefs": dependency_refs,
            "normalizationEvidence": [
                f"declared-manifest-normalizer:{manifest['schemaRef']}:{manifest['schemaVersion']}",
                f"derivation:{derivation_kind}",
            ],
        },
        "committedAt": utc_now(),
    }
    atomic_write(store / "commits" / f"{commit_digest}.json", commit)
    atomic_write(store / "head.json", revision)
    atomic_write(
        submission_path,
        {
            "submissionDigest": submission_digest,
            "outcome": outcome,
        },
    )
    return outcome


def graph_probe(config: dict[str, Any], request: dict[str, Any]) -> dict[str, Any]:
    """Passive source accessibility only; does not execute a graph query."""
    object_ref = required_string(request, "objectRef")
    endpoint = select_by(config.get("graphSources"), "objectRef", object_ref, "graph-source-not-configured")
    if required_positive_int(request, "grantEpoch") != required_positive_int(endpoint, "grantEpoch"):
        raise BoundaryError("grant-epoch-mismatch", "graph source grant epoch is stale")
    timeout_ms = required_positive_int(request, "maxLatencyMs")
    if timeout_ms > 10000:
        raise BoundaryError("bounded-limit-exceeded", "probe timeout exceeds 10000ms")
    credential = endpoint.get("credential") or {}
    status = fetch_status(
        required_string(endpoint, "statusUrl"),
        require_secret(required_string(credential, "usernameEnv")),
        require_secret(required_string(credential, "passwordEnv")),
        timeout_ms / 1000,
    )
    revision, evidence_digest = graph_revision(status)
    return {"reachable": True, "sourceRevision": revision, "observedAtMs": now_ms(),
            "evidenceRef": f"graph-status:sha256:{evidence_digest}", "adapter": "fustor.graph-conformance"}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fustor generic boundary conformance executable")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("version")
    for name in ("graph-observe", "graph-probe", "admission-issue", "declared-submit"):
        command = subparsers.add_parser(name)
        command.add_argument("--config", required=True)
        command.add_argument("--request-json", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "version":
        print(VERSION)
        return 0
    try:
        config = load_object(Path(args.config).resolve(), "invalid-configuration")
        if config.get("schemaVersion") != WIRE_VERSION:
            raise BoundaryError("invalid-configuration", "unsupported boundary configuration version")
        request = read_request(args.request_json)
        handlers: dict[str, Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]]] = {
            "graph-observe": graph_observe,
            "graph-probe": graph_probe,
            "admission-issue": issue_admission,
            "declared-submit": submit_declared,
        }
        result = handlers[args.command](config, request)
        print(canonical_bytes(result).decode("utf-8"))
        return 0
    except BoundaryError as exc:
        print(
            canonical_bytes({"ok": False, "errorCode": exc.code, "message": str(exc)}).decode("utf-8"),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
