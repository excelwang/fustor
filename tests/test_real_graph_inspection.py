#!/usr/bin/env python3
"""ASSERTION INTENT (Black-box tests — public traits and APIs only).
Do not introduce white-box testing logic or internal workarounds.

Opt-in real-provider certification through the public native CLI. No provider
fixtures, synthetic records, private driver imports or permissive fallbacks.
The independently compiled expectation file supplies boundedQueries entries
with queryId, parameters and expected (one scalar/table row).
"""
import argparse
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time


def sha(file):
    return hashlib.sha256(file.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--expectations', type=Path, required=True)
    parser.add_argument('--credential-env-file', type=Path, required=True)
    parser.add_argument('--evidence', type=Path, required=True)
    args = parser.parse_args()
    for file in (args.binary, args.config, args.expectations, args.credential_env_file):
        assert file.is_absolute() and file.resolve(strict=True) == file and file.is_file()
    assert args.credential_env_file.stat().st_mode & 0o077 == 0
    config = json.loads(args.config.read_bytes())
    assert len(config['endpoints']) == 1
    endpoint = config['endpoints'][0]
    grant = next(item for item in config['__cnx_runtime']['resource_grants'] if item['object_ref'] == endpoint['object_ref'])
    credential = next(item for item in config['credentials'] if item['credential_ref'] == endpoint['credential_ref'])
    assert credential['auth_type'] == 'basic_env'
    values = dict(line.split('=', 1) for line in args.credential_env_file.read_text().splitlines() if line and not line.startswith('#'))
    environment = dict(PATH='/usr/bin:/bin')
    for key in ('username_env', 'password_env'):
        environment[credential[key]] = values[credential[key]]
    expectations = json.loads(args.expectations.read_bytes())['boundedQueries']
    assert len(expectations) >= 4
    queries = {query['query_id']: query for query in endpoint['queries']}

    def invoke(request):
        native = subprocess.run([str(args.binary), 'read', '--kind', 'graph', '--config', str(args.config)],
            input=json.dumps(request).encode(), env=environment, capture_output=True, timeout=35)
        assert len(native.stdout) + len(native.stderr) <= 8388608
        return native.returncode, json.loads(native.stdout or native.stderr)

    def request_for(expected):
        query = queries[expected['queryId']]
        limits = query['limits']
        now = int(time.time() * 1000)
        return dict(wire_version=2, context=dict(request_id='real-graph-' + query['query_id'], grant_epoch=grant['grant_epoch'],
            issued_at_ms=now, deadline_ms=now + limits['max_latency_ms'], limits=limits),
            request=dict(object_ref=endpoint['object_ref'], operation='configured-query', configured_query_id=query['query_id'],
                expected_result_schema_ref=query['result_schema_ref'], parameters=expected.get('parameters', {}), limit=limits['max_rows']))

    reads, rejections = [], []
    # @verify_spec("CONTRACTS.INSPECTION.NATIVE_INSPECTION_PARITY", mode="system")
    for expected in expectations:
        code, result = invoke(request_for(expected))
        assert code == 0, result
        assert result['adapter'] == 'fustor.native.oneshot.v1' and result['wireEncoding'] == 'msgpack'
        response = result['response']
        assert response['payload']['kind'] != 'error', response['payload']
        payload, receipt = response['payload']['payload'], response['receipt']
        assert payload['rows'] == [expected['expected']], (expected['queryId'], payload['rows'], expected['expected'])
        assert payload['complete'] is True
        assert payload['fustor_read_only_enforcement'] == 'native-object-query-epoch-parameter-budget-gates'
        assert payload['backend_account_permissions'] == 'unverified'
        assert receipt['source_kind'] == 'graph' and receipt['authority_object_ref'] == endpoint['object_ref']
        assert receipt['grant_epoch'] == grant['grant_epoch'] and receipt['receipt_digest'] and receipt['source_revision']
        assert receipt['actual']['rows'] == 1 and receipt['actual']['calls'] == (3 if endpoint.get('status_url') else 1)
        reads.append(dict(queryId=expected['queryId'], native=result))

    # @verify_spec("CONTRACTS.SOURCE.NATIVE_READ_AUTHORIZATION", mode="system")
    for case in ('unknown-object', 'unknown-query', 'stale-epoch', 'unexpected-parameter', 'hard-cap', 'write-operation', 'raw-statement'):
        request = deepcopy(request_for(expectations[0]))
        if case == 'unknown-object': request['request']['object_ref'] = 'not-granted'
        if case == 'unknown-query': request['request']['configured_query_id'] = 'not-approved'
        if case == 'stale-epoch': request['context']['grant_epoch'] += 1
        if case == 'unexpected-parameter': request['request']['parameters'] = {'not_declared': 1}
        if case == 'hard-cap': request['context']['limits']['max_rows'] += 1
        if case == 'write-operation': request['request']['operation'] = 'schema-upload'
        if case == 'raw-statement': request['request']['statement'] = 'CREATE (:MustNeverExecute)'
        code, response = invoke(request)
        if case in ('write-operation', 'raw-statement'):
            assert code != 0 and not response.get('response', {}).get('receipt'), (case, response)
        else:
            assert code == 0 and response['response']['payload']['kind'] == 'error', (case, response)
            assert response['response']['payload']['payload']['code'] in ('access-denied', 'scope-denied', 'invalid-request', 'budget-exceeded'), (case, response)
            assert not response['response'].get('receipt')
        rejections.append(dict(case=case, exitCode=code, native=response))
    # Reject an attempted configured mutation using public config validation;
    # neither the real configuration nor the real graph is modified.
    proposed = deepcopy(config)
    proposed['endpoints'][0]['queries'][0]['statement'] = 'CREATE (n) RETURN n'
    validation = subprocess.run([str(args.binary), 'validate', '--kind', 'graph', '--config', '-'],
        input=json.dumps(proposed).encode(), env=environment, capture_output=True, timeout=10)
    assert validation.returncode != 0
    rejections.append(dict(case='configured-mutation', exitCode=validation.returncode, native=json.loads(validation.stderr)))
    evidence = dict(evidenceLevel='real-provider-public-native-cli', binarySha256=sha(args.binary),
        configurationSha256=sha(args.config), independentExpectationsSha256=sha(args.expectations), reads=reads,
        rejections=rejections, backendAccountPermissions='unverified', workflowExecutionCertified=False)
    with os.fdopen(os.open(args.evidence, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600), 'w') as stream:
        json.dump(evidence, stream)
    print(json.dumps(dict(verified=True, queries=len(reads), nativeRejections=len(rejections), evidence=str(args.evidence),
        binarySha256=evidence['binarySha256'], workflowExecutionCertified=False)))


if __name__ == '__main__':
    main()
