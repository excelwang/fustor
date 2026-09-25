// ASSERTION INTENT (Black-box tests — public traits and APIs only). Do not introduce white-box testing logic or internal workarounds.
// Migrated source-owner assertions from Dashboard source-registry.test.js.
// The TCP listener detects forbidden provider I/O; it is not a provider mock.
import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import net from 'node:net';
import { fileURLToPath } from 'node:url';
import { SourceConfigurationManager } from '../index.mjs';

async function typedOwner(t, mutate = () => {}) {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'typed-source-'));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  let connections = 0;
  const listener = net.createServer(socket => { connections++; socket.destroy(); });
  await new Promise(resolve => listener.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise(resolve => listener.close(resolve)));
  const port = listener.address().port;
  const dataRoot = path.join(root, 'files'); await fs.mkdir(dataRoot);
  const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
  for (const name of ['README.md', 'package.json']) await fs.copyFile(path.join(repo, name), path.join(dataRoot, name));
  const limits = { max_rows: 2, max_bytes: 65536, max_calls: 3, max_fanout: 1, max_latency_ms: 3000, max_cost_units: 3 };
  const templates = {}, declarations = {}, policies = [];
  for (const kind of ['mysql', 'pg', 'es', 's3', 'local-fs', 'jsonl', 'csv', 'graph']) {
    const endpoint = { object_ref: 'readable', active: true }, grant = { resource_kind: kind === 'es' ? 'elasticsearch' : kind,
      object_ref: 'readable', grant_state: 'active', grant_epoch: 1, interfaces: ['read'] };
    let connection, objectScope;
    if (['local-fs', 'jsonl', 'csv'].includes(kind)) {
      endpoint.root_path = '/nonexistent-unbound-source-template'; endpoint.path_scopes = ['*']; grant.path_scopes = ['*'];
      connection = { rootRef: 'files' }; objectScope = { path: kind === 'local-fs' ? '.' : 'not-read.' + kind };
    } else {
      endpoint.endpoint_uri = kind === 'pg' ? 'postgresql://127.0.0.1:1/unbound' : kind === 'mysql' ? 'mysql://127.0.0.1:1/unbound' : kind === 'graph' ? 'bolt://127.0.0.1:1' : 'http://127.0.0.1:1';
      connection = { host: '127.0.0.1', port, ...(['pg', 'mysql'].includes(kind) ? { database: 'new_database' } : { tls: false }) };
      if (['pg', 'mysql'].includes(kind)) {
        endpoint.schema_scopes = ['public']; endpoint.table_scopes = ['permitted_*'];
        grant.schema_scopes = ['public']; grant.table_scopes = ['permitted_*']; objectScope = { schema: 'public', table: 'permitted_records' };
      } else if (kind === 'es') { endpoint.index_scopes = grant.index_scopes = ['permitted-*']; objectScope = { index: 'permitted-records' }; }
      else if (kind === 's3') {
        endpoint.bucket = 'samples'; endpoint.prefix = 'permitted/'; endpoint.bucket_scopes = grant.bucket_scopes = ['samples'];
        endpoint.prefix_scopes = grant.prefix_scopes = ['permitted/*']; objectScope = { bucket: 'samples', prefix: 'permitted/new/' };
      } else {
        endpoint.credential_ref = 'reader'; endpoint.queries = [{ query_id: 'count', statement: 'RETURN 1 AS count', result_schema_ref: 'schema:count', limits }];
        grant.query_scopes = ['count']; objectScope = { queryId: 'count' };
      }
    }
    const native = { endpoints: [endpoint], __cnx_runtime: { resource_grants: [grant] }, ...(kind === 'graph' ? {
      credentials: [{ credential_ref: 'reader', auth_type: 'basic_env', username_env: 'ABSENT_TYPED_USER', password_env: 'ABSENT_TYPED_PASS' }],
    } : { default_page_size: 2, max_page_size: 3, retained_native_extension: { preserve: true } }) };
    templates[kind] = native;
    declarations[kind] = { id: kind, label: kind, kind, connection, objectScope, active: true, ...(kind === 'graph' ? { credentialRef: 'reader' } : {}) };
    policies.push({ kind, nativeTemplate: path.join(root, kind + '-template.json'), objectRef: 'readable', grantEpoch: 1,
      permissionContext: 'deployed-read-authority', defaults: limits, hardCaps: [limits] });
  }
  const policy = { schemaVersion: 1, tool: { command: process.env.FUSTOR_SOURCE_INSPECT },
    allowedTargets: [{ host: '127.0.0.1', ports: [port] }], roots: { files: dataRoot }, policies };
  await mutate({ root, policy, templates, declarations });
  for (const item of policies) await fs.writeFile(item.nativeTemplate, JSON.stringify(templates[item.kind]), { mode: 0o600 });
  const policyFile = path.join(root, 'source-policy.json'); await fs.writeFile(policyFile, JSON.stringify(policy), { mode: 0o600 });
  const configuration = { id: 'new', directory: path.join(root, 'owner'), policyFile };
  const manager = await SourceConfigurationManager.open(configuration);
  return { root, dataRoot, policy, policyFile, templates, declarations, configuration, manager,
    reopen: () => SourceConfigurationManager.open(configuration), connections: () => connections };
}

const native = { skip: !process.env.FUSTOR_SOURCE_INSPECT };

// @verify_spec("CONTRACTS.NATIVE_CONFIGURATION.NATIVE_SOURCE_CONFIGURATION", mode="system")
test('catalog-only PG registration preserves native scopes and performs no provider I/O', async t => {
  assert.ok(process.env.FUSTOR_SOURCE_INSPECT, 'current native validator required');
  const s = await typedOwner(t);
  await save(s, { ...s.declarations.pg, objectScope: { catalogMetadata: true } });
  const resolved = await s.manager.resolve();
  assert.deepEqual(resolved.sources[0].operations.map(op => op.id), ['catalog-metadata']);
  assert.deepEqual(resolved.sources[0].operations[0].request, { object_ref: 'readable', operation: 'catalog-metadata', limit: 2 });
  const entry = (await s.manager.describe()).entries[0];
  const file = path.join(s.configuration.directory, entry.id + '-' + entry.nativeDigest + '.json');
  const doc = JSON.parse(await fs.readFile(file));
  assert.deepEqual(doc.endpoints[0].schema_scopes, ['public']);
  assert.deepEqual(doc.endpoints[0].table_scopes, ['permitted_*']);
  await assert.rejects(save(s, { ...s.declarations.pg, objectScope: { catalogMetadata: true, table: 'forbidden' } }), { code: 'SOURCE_DECLARATION_INVALID' });
  await assert.rejects(save(s, { ...s.declarations.pg, objectScope: { catalogMetadata: false } }), { code: 'SOURCE_OBJECT_INVALID' });
  assert.equal(s.connections(), 0);
});

// @verify_spec("CONTRACTS.NATIVE_CONFIGURATION.NATIVE_SOURCE_CONFIGURATION", mode="system")
test('named scopes share one native connection and every scope is authorized before save', async t => {
  assert.ok(process.env.FUSTOR_SOURCE_INSPECT, 'current native validator required');
  const s = await typedOwner(t), { objectScope, ...declaration } = s.declarations.pg;
  const first = { id: 'first', label: 'First dataset', objectScope }, second = { id: 'second', objectScope: { ...objectScope, table: 'permitted_other' } };
  const saved = await save(s, { ...declaration, objectScopes: [first, second] });
  const resolved = await s.manager.resolve();
  assert.equal(resolved.sources.length, 1);
  assert.deepEqual(resolved.sources[0].operations.map(op => op.id), ['first:snapshot-open', 'first:discover-fields', 'second:snapshot-open', 'second:discover-fields']);
  assert.equal(resolved.sources[0].operations[2].request.table, 'permitted_other');
  assert.equal(resolved.sources[0].operations[0].scopeId, 'first');
  await assert.rejects(save(s, { ...declaration, objectScopes: [first, { ...second, objectScope: { ...objectScope, table: 'forbidden' } }] }), { code: 'NATIVE_SOURCE_SCOPE_DENIED' });
  await assert.rejects(save(s, { ...declaration, objectScope, objectScopes: [first] }), { code: 'SOURCE_DECLARATION_INVALID' });
  await assert.rejects(save(s, { ...declaration, objectScopes: [first, first] }), { code: 'SOURCE_SCOPES_INVALID' });
  await assert.rejects(save(s, { ...declaration, objectScopes: [first, { ...second, objectScope: { ...second.objectScope, primaryKey: 'id' } }] }), { code: 'SOURCE_SCOPE_CONFIGURATION_CONFLICT' });
  assert.equal((await s.manager.describe()).revision, saved.revision);
  assert.equal(s.connections(), 0);
  const reopened = await s.reopen();
  assert.deepEqual((await reopened.resolve()).sources[0].operations, resolved.sources[0].operations);
});

// @verify_spec("CONTRACTS.NATIVE_CONFIGURATION.NATIVE_SOURCE_CONFIGURATION", mode="system")
test('graph scopes retain only the explicitly owner-pinned status association', async t => {
  assert.ok(process.env.FUSTOR_SOURCE_INSPECT, 'current native validator required');
  const s = await typedOwner(t, ({ policy, templates, declarations }) => {
    const endpoint = templates.graph.endpoints[0], entry = policy.policies.find(item => item.kind === 'graph');
    endpoint.status_url = 'http://127.0.0.1:1/old'; endpoint.status_credential_ref = 'reader';
    endpoint.queries.push({ ...endpoint.queries[0], query_id: 'other', statement: 'RETURN 2 AS count' });
    templates.graph.__cnx_runtime.resource_grants[0].query_scopes.push('other');
    const port = declarations.graph.connection.port;
    policy.allowedTargets[0].ports.push(1);
    entry.statusBindings = [{ endpointUri: 'bolt://127.0.0.1:' + port, statusUrl: 'http://127.0.0.1:' + port + '/status', statusCredentialRef: 'reader' }];
  });
  const { objectScope, ...declaration } = s.declarations.graph;
  const scopes = [{ id: 'first', objectScope }, { id: 'second', objectScope: { queryId: 'other' } }];
  const first = await save(s, { ...declaration, objectScopes: scopes });
  const entry = (await s.manager.describe()).entries[0];
  const file = path.join(s.configuration.directory, entry.id + '-' + entry.nativeDigest + '.json');
  const original = await fs.readFile(file, 'utf8'), endpoint = JSON.parse(original).endpoints[0];
  assert.equal(endpoint.status_url, 'http://127.0.0.1:' + declaration.connection.port + '/status');
  assert.equal(endpoint.status_credential_ref, 'reader');
  assert.deepEqual((await s.manager.resolve()).sources[0].operations.map(op => op.request.configured_query_id), ['count', 'other']);
  await assert.rejects(save(s, { ...declaration, objectScopes: scopes, statusUrl: 'http://127.0.0.1:1/arbitrary' }), { code: 'SOURCE_DECLARATION_INVALID' });
  assert.equal((await s.manager.describe()).revision, first.revision);
  await save(s, { ...declaration, connection: { ...declaration.connection, port: 1 }, objectScopes: scopes });
  const moved = (await s.manager.describe()).entries[0];
  const changed = JSON.parse(await fs.readFile(path.join(s.configuration.directory, moved.id + '-' + moved.nativeDigest + '.json'))).endpoints[0];
  assert.equal(changed.status_url, undefined); assert.equal(changed.status_credential_ref, undefined);
  assert.equal(await fs.readFile(file, 'utf8'), original);
  assert.equal(s.connections(), 0);
});

async function save(s, declaration, manager = s.manager) {
  return manager.apply({ declaration, expectedRevision: (await manager.describe()).revision });
}

// @verify_spec("CONTRACTS.NATIVE_CONFIGURATION.NATIVE_SOURCE_CONFIGURATION", mode="system")
// @verify_spec("CONTRACTS.SOURCE.SOURCE_CREDENTIAL_AND_SCOPE_ISOLATION", mode="system")
test('MySQL registration and rebinding preserve installed TLS without reading the CA or provider', async t => {
  assert.ok(process.env.FUSTOR_SOURCE_INSPECT, 'current native validator required');
  const s = await typedOwner(t, ({ templates }) => {
    templates.mysql.endpoints[0].endpoint_uri += '?ssl=true';
    templates.mysql.endpoints[0].tls = { ca_file: '/nonexistent-installed-ca.pem' };
  });
  const first = await save(s, s.declarations.mysql);
  const inventory = await s.manager.describe();
  const file = path.join(s.configuration.directory, 'mysql-' + inventory.entries[0].nativeDigest + '.json');
  const original = await fs.readFile(file, 'utf8'), endpoint = JSON.parse(original).endpoints[0];
  assert.equal(new URL(endpoint.endpoint_uri).searchParams.get('ssl'), 'true');
  assert.deepEqual(endpoint.tls, { ca_file: '/nonexistent-installed-ca.pem' });
  const reopened = await s.reopen();
  await save(s, { ...s.declarations.mysql, connection: { ...s.declarations.mysql.connection, database: 'other_database' } }, reopened);
  const changed = await reopened.describe();
  assert.notEqual(changed.revision, first.revision);
  const active = JSON.parse(await fs.readFile(path.join(s.configuration.directory, 'mysql-' + changed.entries[0].nativeDigest + '.json'))).endpoints[0];
  assert.equal(new URL(active.endpoint_uri).searchParams.get('ssl'), 'true');
  assert.deepEqual(active.tls, endpoint.tls);
  assert.equal(await fs.readFile(file, 'utf8'), original);
  assert.equal(s.connections(), 0);
});

// Remaining source-policy fault objectives moved from the old Dashboard
// positive-write routes. This tests native configuration, not provider liveness.
test('policy drift remains visible and deactivation does not adopt newer authority', native, async t => {
  const s = await typedOwner(t), declaration = s.declarations.pg;
  const first = await save(s, declaration);
  const file = s.policy.policies.find(policy => policy.kind === 'pg').nativeTemplate;
  s.templates.pg.__cnx_runtime.resource_grants[0].grant_epoch = 2;
  await fs.writeFile(file, JSON.stringify(s.templates.pg), { mode: 0o600 });
  const reopened = await s.reopen(), resolved = await reopened.resolve();
  assert.equal(resolved.sources.length, 1);
  assert.equal(resolved.sources[0].registrationError, 'REGISTERED_POLICY_DRIFT');
  await assert.rejects(save(s, { ...declaration, label: 'Changed declaration' }, reopened), { code: 'REGISTERED_POLICY_DRIFT' });
  assert.equal((await reopened.describe()).revision, first.revision);
  await save(s, { ...declaration, active: false }, reopened);
  const inventory = await (await s.reopen()).describe();
  assert.equal(inventory.entries[0].active, false);
  const raw = await fs.readFile(path.join(s.configuration.directory, 'pg-' + inventory.entries[0].nativeDigest + '.json'), 'utf8');
  assert.equal(JSON.parse(raw).__cnx_runtime.resource_grants[0].grant_epoch, 1);
  assert.equal(s.connections(), 0);
});

test('endpoint overrides and source root symlink escape are rejected before provider access', native, async t => {
  const s = await typedOwner(t, ({ templates }) => {
    templates.pg.__cnx_runtime.resource_grants[0].endpoint_uri = 'postgresql://127.0.0.1:1/pinned';
  });
  await assert.rejects(save(s, s.declarations.pg), { code: 'NATIVE_ENDPOINT_OVERRIDE_CONFLICT' });
  await fs.symlink(s.root, path.join(s.dataRoot, 'escape'));
  await assert.rejects(save(s, { ...s.declarations['local-fs'], connection: { rootRef: 'files', directory: 'escape' } }), { code: 'SOURCE_ROOT_FORBIDDEN' });
  assert.deepEqual((await s.manager.describe()).entries, []);
  assert.equal(s.connections(), 0);
});

test('one damaged immutable source remains explicit without hiding a valid peer', native, async t => {
  const s = await typedOwner(t);
  for (const id of ['first-files', 'second-files']) await save(s, { ...s.declarations['local-fs'], id });
  const inventory = await s.manager.describe();
  const entry = inventory.entries.find(entry => entry.id === 'first-files');
  await fs.appendFile(path.join(s.configuration.directory, entry.id + '-' + entry.nativeDigest + '.json'), '\n');
  const sources = (await (await s.reopen()).resolve()).sources;
  assert.equal(sources.length, 2);
  assert.equal(sources.find(source => source.id === 'first-files').registrationError, 'REGISTERED_NATIVE_CONFIG_DRIFT');
  assert.equal(sources.find(source => source.id === 'second-files').registrationError, undefined);
  assert.deepEqual(await (await s.reopen()).describe(), inventory);
  assert.equal(s.connections(), 0);
});

test('missing native validator is unavailable, not a successful or denied authorization', native, async t => {
  const s = await typedOwner(t, ({ root, policy }) => { policy.tool.command = path.join(root, 'missing-native-validator'); });
  const before = await s.manager.describe();
  assert.ok(before.policies.every(policy => !policy.available && policy.error === 'SOURCE_INSPECTION_UNAVAILABLE'));
  await assert.rejects(save(s, s.declarations.pg), { code: 'SOURCE_INSPECTION_UNAVAILABLE' });
  assert.deepEqual((await s.manager.describe()).entries, []);
  assert.equal(s.connections(), 0);
});
// @verify_spec("CONTRACTS.NATIVE_CONFIGURATION.NATIVE_SOURCE_CONFIGURATION", mode="system")
test('all eight native source families register and deactivate without contacting providers', native, async t => {
  const s = await typedOwner(t);
  const initial = await s.manager.describe();
  assert.deepEqual(initial.entries, []);
  assert.equal(initial.policies.length, 8);
  assert.ok(initial.policies.every(policy => policy.available));
  assert.equal(JSON.stringify(initial).includes(s.root), false);
  for (const declaration of Object.values(s.declarations)) {
    const saved = await s.manager.apply({ declaration, expectedRevision: (await s.manager.describe()).revision });
    assert.equal(saved.saved, true);
    assert.match(saved.operationId, /^[a-f0-9]{64}$/);
    assert.equal(saved.activation, 'next-read');
    await s.manager.apply({ declaration: { ...declaration, active: false }, expectedRevision: saved.revision });
  }
  const current = await s.manager.describe();
  assert.equal(current.entries.length, 8);
  assert.deepEqual(await (await s.reopen()).describe(), current);
  const resolved = await (await s.reopen()).resolve();
  for (const source of resolved.sources) {
    assert.equal(source.active, false);
    const first = source.kind === 'graph' ? 'configured-query' : ['pg','mysql'].includes(source.kind) ? 'snapshot-open' : 'snapshot-page';
    assert.equal(source.operations[0].id, first);
    assert.equal(source.operations[0].request.operation, first);
    assert.equal(source.operations[0].request.cursor, undefined);
  }
  assert.equal(s.connections(), 0);
});

// @verify_spec("CONTRACTS.NATIVE_CONFIGURATION.NATIVE_SOURCE_CONFIGURATION", mode="system")
test('owner rejects inline secrets, target and scope expansion without committing', native, async t => {
  const s = await typedOwner(t), pg = s.declarations.pg;
  const before = await s.manager.describe();
  const denied = [
    { ...pg, connection: { ...pg.connection, host: '127.0.0.2' } },
    { ...pg, connection: { ...pg.connection, port: 1 } },
    { ...pg, connection: { ...pg.connection, database: '..' } },
    { ...pg, connection: { ...pg.connection, password: 'INLINE_SECRET_SENTINEL' } },
    { ...pg, connection: { uri: 'postgres://user:secret@127.0.0.1/db' } },
    { ...pg, credentialRef: 'ABSENT_TYPED_PASS' },
    { ...pg, objectScope: { ...pg.objectScope, table: 'private_records' } },
    { ...pg, objectScope: { ...pg.objectScope, schema: 'private' } },
    { ...pg, grantEpoch: 999 },
    { ...s.declarations.es, objectScope: { index: 'private-records' } },
    { ...s.declarations.s3, objectScope: { bucket: 'samples', prefix: 'private/' } },
    { ...s.declarations.graph, objectScope: { queryId: 'unregistered' } },
    { ...s.declarations.graph, objectScope: { queryId: 'count', query: 'CREATE (n)' } },
    { ...s.declarations['local-fs'], connection: { rootRef: 'files', directory: '../' } },
  ];
  for (const declaration of denied) await assert.rejects(s.manager.apply({ declaration, expectedRevision: before.revision }));
  assert.deepEqual(await s.manager.describe(), before);
  assert.equal(s.connections(), 0);
});

// @verify_spec("CONTRACTS.NATIVE_CONFIGURATION.NATIVE_SOURCE_CONFIGURATION", mode="system")
test('concurrent configuration CAS, immutable bytes and restart preserve one committed owner revision', native, async t => {
  const s = await typedOwner(t), other = await s.reopen();
  const expectedRevision = (await s.manager.describe()).revision;
  const values = await Promise.allSettled([s.manager,other].map((manager,i) => manager.apply({
    declaration: { ...s.declarations['local-fs'], id: 'files_' + i }, expectedRevision,
  })));
  assert.equal(values.filter(value => value.status === 'fulfilled').length, 1);
  assert.equal(values.filter(value => value.status === 'rejected').length, 1);
  const before = await (await s.reopen()).describe();
  assert.equal(before.entries.length, 1);
  const entry = before.entries[0], file = path.join(s.configuration.directory, entry.id + '-' + entry.nativeDigest + '.json');
  const raw = await fs.readFile(file, 'utf8');
  assert.deepEqual(JSON.parse(raw).retained_native_extension, { preserve: true });
  const stopped = await s.manager.apply({ declaration: { ...entry.declaration, active:false }, expectedRevision:before.revision });
  assert.equal(stopped.active,false);
  assert.equal(await fs.readFile(file,'utf8'),raw);
  const inventory = await (await s.reopen()).describe();
  assert.equal(inventory.revision,stopped.revision);
  await assert.rejects(s.manager.apply({ declaration:entry.declaration,expectedRevision }));
});

// @verify_spec("CONTRACTS.NATIVE_CONFIGURATION.NATIVE_SOURCE_CONFIGURATION", mode="system")
test('failed audit cannot activate a source and owner-root symlinks are rejected', native, async t => {
  const s = await typedOwner(t), before = await s.manager.describe();
  await fs.mkdir(s.configuration.directory,{mode:0o700});
  await fs.writeFile(path.join(s.configuration.directory,'operations'),'not a directory',{mode:0o600});
  await assert.rejects(s.manager.apply({ declaration:s.declarations['local-fs'],expectedRevision:before.revision }));
  assert.deepEqual(await s.manager.describe(),before);
  await assert.rejects(fs.stat(path.join(s.configuration.directory,'sources.json')),{code:'ENOENT'});
  const alias=path.join(s.root,'owner-alias');
  await fs.symlink(s.configuration.directory,alias);
  await assert.rejects(SourceConfigurationManager.open({...s.configuration,directory:alias}),{code:'SOURCE_OWNER_ROOT_NOT_PRIVATE'});
});
