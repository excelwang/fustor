"""ASSERTION INTENT: public CLI tests; no remote data, service, or test-only reader."""
import json
import os
import copy
import socket
import select
from pathlib import Path
import subprocess
import unittest

ROOT = Path(__file__).resolve().parents[1]

class PassiveInspectionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.binary = Path(os.environ.get('FUSTOR_INSPECT_BIN', ROOT / 'target/debug/fustor-source-inspect'))
        if 'FUSTOR_INSPECT_BIN' not in os.environ:
            subprocess.run(['cargo', 'build', '-p', 'fustor-source-inspect'], cwd=ROOT, check=True, capture_output=True)

    def call(self, args, request):
        return subprocess.run([str(self.binary), *args], input=json.dumps(request), capture_output=True, text=True, timeout=10)

    # @verify_spec("CONTRACTS.OPERATIONS.ONE_SHOT_INSPECTION", mode="system")
    def test_stdin_configuration_validation_does_not_open_a_source(self):
        config = {'default_page_size': 1, 'max_page_size': 2, 'field_sample_limit': 1,
            'endpoints': [{'object_ref': 'fixture', 'root_path': '/nonexistent-passive-inspection-fixture', 'path_scopes': ['*.jsonl'], 'active': True}],
            '__cnx_runtime': {'resource_grants': [{'resource_kind': 'jsonl', 'object_ref': 'fixture', 'grant_epoch': 1,
                'grant_state': 'active', 'interfaces': ['read'], 'path_scopes': ['*.jsonl']}]}}
        result = self.call(['validate', '--kind', 'jsonl', '--config', '-'], config)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout), {'valid': True})
        config['__cnx_runtime']['resource_grants'][0]['object_ref'] = 'undeclared'
        denied = self.call(['validate', '--kind', 'jsonl', '--config', '-'], config)
        self.assertNotEqual(denied.returncode, 0)
        self.assertEqual(denied.stdout, '')

    # @verify_spec("CONTRACTS.OPERATIONS.PASSIVE_BUDGET_ESTIMATE", mode="system")
    def test_budget_cli_keeps_cold_defaults_and_rejects_authority_expansion(self):
        limits = dict(max_rows=3, max_bytes=100000, max_calls=1, max_fanout=1, max_latency_ms=2500, max_cost_units=1)
        request = dict(now_ms=100000000, context_key='synthetic-scope-v1', defaults=limits, hard_caps=[dict(limits)], history=[])
        result = self.call(['budget'], request)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)['limits'], limits)
        request['overrides'] = {'max_bytes': 100001}
        self.assertNotEqual(self.call(['budget'], request).returncode, 0)
        request['overrides'] = {'max_latency_ms': 1000}
        manual = json.loads(self.call(['budget'], request).stdout)
        self.assertEqual(manual['limits']['max_rows'], 3)
        self.assertEqual(manual['max_bytes']['mode'], 'automatic')
        self.assertEqual(manual['max_latency_ms']['mode'], 'manual')

    # @verify_spec("CONTRACTS.INSPECTION.NATIVE_INSPECTION_PARITY", mode="system")
    def test_scope_preflight_uses_native_authority_for_all_families_without_provider_io(self):
        # A listening socket detects forbidden connections, but serves no data or
        # protocol response. No provider health/read acceptance is claimed here.
        listener = socket.socket()
        listener.bind(('127.0.0.1', 0))
        listener.listen()
        self.addCleanup(listener.close)
        port = listener.getsockname()[1]
        limits = dict(max_rows=1, max_bytes=65536, max_calls=3, max_fanout=1, max_latency_ms=3000, max_cost_units=3)
        for kind in ['jsonl', 'csv', 'local-fs', 'pg', 'mysql', 'es', 's3', 'graph']:
            with self.subTest(kind=kind):
                endpoint = dict(object_ref='allowed', active=True)
                grant = dict(resource_kind='elasticsearch' if kind == 'es' else kind, object_ref='allowed', grant_epoch=7, grant_state='active', interfaces=['read'])
                scope = dict(object_ref='allowed', grant_epoch=7)
                if kind in ['jsonl', 'csv', 'local-fs']:
                    endpoint.update(root_path='/nonexistent-passive-scope-root', path_scopes=['permitted/*'])
                    grant['path_scopes'] = ['permitted/*']
                    scope['path'] = 'permitted/sample'
                    bad_field, bad_value = 'path', 'private/sample'
                else:
                    protocol = 'postgresql' if kind == 'pg' else 'mysql' if kind == 'mysql' else 'bolt' if kind == 'graph' else 'http'
                    endpoint['endpoint_uri'] = f'{protocol}://127.0.0.1:{port}'
                    if kind in ['pg', 'mysql']:
                        endpoint.update(schema_scopes=['public'], table_scopes=['permitted_*'])
                        grant.update(schema_scopes=['public'], table_scopes=['permitted_*'])
                        scope.update(schema='public', table='permitted_records')
                        bad_field, bad_value = 'table', 'private_records'
                    elif kind == 'es':
                        endpoint['index_scopes'] = grant['index_scopes'] = ['permitted-*']
                        scope['index'] = 'permitted-records'
                        bad_field, bad_value = 'index', 'private-records'
                    elif kind == 's3':
                        endpoint.update(bucket='sample', prefix='permitted/', bucket_scopes=['sample'], prefix_scopes=['permitted/*'])
                        grant.update(bucket_scopes=['sample'], prefix_scopes=['permitted/*'])
                        scope.update(bucket='sample', prefix='permitted/data/')
                        bad_field, bad_value = 'bucket', 'private'
                    else:
                        endpoint.update(credential_ref='reader', queries=[dict(query_id='count', statement='RETURN 1 AS count', result_schema_ref='schema:count', limits=limits)])
                        grant['query_scopes'] = ['count']
                        scope['configured_query_id'] = 'count'
                        bad_field, bad_value = 'configured_query_id', 'unregistered'
                config = dict(endpoints=[endpoint], __cnx_runtime=dict(resource_grants=[grant]))
                if kind == 'graph':
                    config['credentials'] = [dict(credential_ref='reader', auth_type='basic_env', username_env='ABSENT_SCOPE_USER', password_env='ABSENT_SCOPE_PASS')]
                args = ['validate-scope', '--kind', kind, '--config', '-']
                request = dict(configuration=config, selection=scope)
                checked = self.call(args, request)
                self.assertEqual(checked.returncode, 0, checked.stderr)
                self.assertEqual(json.loads(checked.stdout), dict(valid=True, scopeAuthorized=True, readiness='not-observed'))
                invalid = [{bad_field: bad_value}, {'grant_epoch': 8}, {'object_ref': 'missing'}, {'caller_query': 'RETURN 1'}]
                for change in invalid:
                    denied = self.call(args, dict(configuration=config, selection={**scope, **change}))
                    self.assertNotEqual(denied.returncode, 0, (kind, change))
                    self.assertEqual(denied.stdout, '')
                    self.assertEqual(json.loads(denied.stderr)['errorCode'], 'source-scope-invalid')
                inactive = copy.deepcopy(config)
                inactive['__cnx_runtime']['resource_grants'][0]['grant_state'] = 'inactive'
                self.assertNotEqual(self.call(args, dict(configuration=inactive, selection=scope)).returncode, 0)
        self.assertEqual(select.select([listener], [], [], 0)[0], [], 'scope validation must not connect to any provider')

if __name__ == '__main__':
    unittest.main()
