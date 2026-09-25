"""ASSERTION INTENT (Black-box tests — public traits and APIs only). Do not introduce white-box testing logic or internal workarounds.

Native CLI tests against a loopback HTTP provider fixture. This is not a real
PuppyGraph/Bolt acceptance result and never contacts a remote candidate.
"""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import base64
import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]
BIN = os.environ.get('FUSTOR_INSPECT_BIN', str(ROOT / 'target/debug/fustor-source-inspect'))


class NativeGraphInspectionTests(unittest.TestCase):
    # @verify_spec("CONTRACTS.INSPECTION.NATIVE_INSPECTION_PARITY", mode="system")
    def test_native_graph_grants_receipt_and_no_caller_query(self):
        calls = []
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                calls.append((self.path, self.headers.get('Authorization')))
                expected = 'Basic ' + base64.b64encode(b'status-user:status-password').decode()
                if self.headers.get('Authorization') != expected:
                    self.send_response(401)
                    self.end_headers()
                    return
                body = json.dumps({'NodeStatus': {'Healthy': True}, 'GremlinServer': {'Healthy': True},
                                   'Schema': {'Exists': True, 'Healthy': True, 'ModTime': 'fixture-generation'},
                                   'schemaDetail': 'x' * 70000}).encode()
                self.send_response(200)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            def log_message(self, *args):
                pass
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        limits = dict(max_rows=1, max_bytes=262144, max_calls=3, max_fanout=1, max_latency_ms=5000, max_cost_units=3)
        config = dict(endpoints=[dict(object_ref='graph', endpoint_uri='bolt://127.0.0.1:1',
            status_url=f'http://127.0.0.1:{server.server_port}/status', credential_ref='reader',
            status_credential_ref='status-reader', read_only_identity=True,
            active=True, queries=[dict(query_id='count', statement='MATCH (n) RETURN count(n) AS count', result_schema_ref='schema:count', limits=limits)])],
            credentials=[dict(credential_ref='reader', auth_type='basic_env', username_env='FIXTURE_GRAPH_USER', password_env='FIXTURE_GRAPH_PASS'),
                         dict(credential_ref='status-reader', auth_type='basic_env', username_env='FIXTURE_STATUS_USER', password_env='FIXTURE_STATUS_PASS')],
            __cnx_runtime=dict(resource_grants=[dict(resource_kind='graph', object_ref='graph', grant_state='active', grant_epoch=2, interfaces=['read'], query_scopes=['count'])]))
        with tempfile.TemporaryDirectory() as directory:
            file = Path(directory) / 'native.json'
            file.write_text(json.dumps(config))
            def call(mode, request=None):
                process = subprocess.run([BIN, mode, '--kind', 'graph', '--config', str(file)], input=json.dumps(request or {}),
                    capture_output=True, text=True, timeout=10, env={**os.environ,
                        'FIXTURE_GRAPH_USER': 'query-user', 'FIXTURE_GRAPH_PASS': 'query-password',
                        'FIXTURE_STATUS_USER': 'status-user', 'FIXTURE_STATUS_PASS': 'status-password'})
                return process.returncode, json.loads(process.stdout if process.returncode == 0 else process.stderr)
            self.assertEqual(call('validate'), (0, {'valid': True}))
            self.assertEqual(call('describe')[1]['status']['payload']['source_kind'], 'graph')
            self.assertFalse(calls)
            now = time.time_ns() // 1000000
            request = dict(wire_version=2, context=dict(request_id='probe', grant_epoch=2, issued_at_ms=now, deadline_ms=now+5000, limits=limits),
                           request=dict(object_ref='graph', operation='probe-connection', limit=1))
            code, output = call('read', request)
            self.assertEqual(code, 0, output)
            receipt = output['response']['receipt']
            self.assertEqual(receipt['source_kind'], 'graph')
            self.assertEqual(receipt['source_revision'], 'puppygraph-schema:fixture-generation')
            self.assertEqual(receipt['grant_epoch'], 2)
            self.assertEqual(receipt['actual']['calls'], 1)
            self.assertGreater(receipt['actual']['bytes'], 0)
            self.assertEqual(output['response']['payload']['payload']['generation_alignment'], 'unknown')
            request['context']['limits'] = {**limits, 'max_bytes': 65536}
            exhausted = call('read', request)[1]['response']['payload']
            self.assertEqual(exhausted['kind'], 'error')
            self.assertEqual(exhausted['payload']['budget_exhaustion'], 'max_bytes')
            request['context']['limits'] = {**limits, 'max_bytes': 4194305}
            self.assertEqual(call('read', request)[1]['response']['payload']['kind'], 'error')
            request['context']['limits'] = limits
            request['context']['grant_epoch'] = 3
            self.assertEqual(call('read', request)[1]['response']['payload']['kind'], 'error')
            request['context']['grant_epoch'] = 2
            request['request'].update(operation='configured-query', configured_query_id='unregistered', expected_result_schema_ref='schema:count')
            self.assertEqual(call('read', request)[1]['response']['payload']['kind'], 'error')
            request['request']['statement'] = 'CREATE (n)'
            self.assertNotEqual(call('read', request)[0], 0)
            self.assertEqual([item[0] for item in calls], ['/status', '/status'],
                             'all authorization failures must precede provider I/O')
            invalid = json.loads(json.dumps(config))
            invalid['endpoints'][0]['status_credential_ref'] = 'missing-status-reader'
            file.write_text(json.dumps(invalid))
            self.assertNotEqual(call('validate')[0], 0)
            for statement in ['CREATE (n) RETURN n', 'MATCH (n) DELETE n RETURN n', 'CALL db.labels()', 'RETURN 1; RETURN 2', 'RETURN 1 /* comment */']:
                config['endpoints'][0]['queries'][0]['statement'] = statement
                file.write_text(json.dumps(config))
                self.assertNotEqual(call('validate')[0], 0, statement)


if __name__ == '__main__':
    unittest.main()
