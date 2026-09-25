// ASSERTION INTENT (Black-box tests — public traits and APIs only). Do not introduce white-box testing logic or internal workarounds.
// Disposable real PostgreSQL + the public native inspector. This certifies the
// generic metadata contract, not production library coverage or business data.
import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import { randomBytes } from 'node:crypto';
import { spawn } from 'node:child_process';

async function command(bin, args, input, env = process.env, timeout = 30000) {
  return new Promise((resolve, reject) => {
    const child = spawn(bin, args, { env, stdio: ['pipe', 'pipe', 'pipe'] });
    const chunks = [], errors = []; let bytes = 0;
    const timer = setTimeout(() => child.kill('SIGKILL'), timeout);
    child.stdout.on('data', chunk => { bytes += chunk.length; if (bytes > 4194304) child.kill('SIGKILL'); else chunks.push(chunk); });
    child.stderr.on('data', chunk => { bytes += chunk.length; if (bytes > 4194304) child.kill('SIGKILL'); else errors.push(chunk); }); child.stdin.on('error', () => {});
    child.on('error', reject);
    child.on('close', code => { clearTimeout(timer); if (code !== 0) reject(Error('native test command failed: ' + bin + ' (' + code + '): ' + Buffer.concat(errors).toString().slice(-2000))); else resolve(Buffer.concat(args[0] === 'logs' ? [...chunks, ...errors] : chunks).toString()); });
    child.stdin.end(input);
  });
}

// @verify_spec("CONTRACTS.INSPECTION.CATALOG_METADATA", mode="system")
test('native catalog metadata is bounded, scoped, read-only and honestly estimated', { timeout: 120000 }, async t => {
  const tool = process.env.FUSTOR_SOURCE_INSPECT;
  assert.ok(tool && path.isAbsolute(tool), 'supply the newly built native inspector');
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'fustor-catalog-test-'));
  const name = 'fustor-catalog-test-' + randomBytes(8).toString('hex');
  const password = randomBytes(24).toString('hex');
  const env = { ...process.env, POSTGRES_PASSWORD: password, CATALOG_PG_USER: 'postgres', CATALOG_PG_PASSWORD: password };
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const container = (await command('docker', ['run', '-d', '--pull=never', '--name', name,
    '--tmpfs', '/var/lib/postgresql/data:rw,size=256m',
    '-e', 'POSTGRES_PASSWORD', '-e', 'POSTGRES_DB=catalog_test', '-p', '127.0.0.1::5432',
    process.env.FUSTOR_TEST_PG_IMAGE ?? 'postgres:16-alpine'], undefined, env)).trim();
  assert.match(container, /^[a-f0-9]{64}$/);
  t.after(() => command('docker', ['rm', '-f', container]));
  const port = Number((await command('docker', ['port', container, '5432/tcp'])).trim().split(':').at(-1));
  assert.ok(port > 1024 && ![25432, 25433].includes(port));
  for (let attempt = 0; ; attempt++) {
    if (attempt === 60) t.diagnostic(await command('docker', ['logs', container]));
    assert.ok(attempt < 60, 'disposable PostgreSQL did not become ready');
    try { await command('docker', ['exec', container, 'pg_isready', '-h', '127.0.0.1', '-U', 'postgres', '-d', 'catalog_test']); break; }
    catch { await new Promise(resolve => setTimeout(resolve, 250)); }
  }
  const sql = statement => command('docker', ['exec', '-i', container, 'psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'catalog_test'], statement);
  await sql(`CREATE SCHEMA visible; CREATE SCHEMA forbidden;
    CREATE TABLE visible.visible_unknown(id bigint PRIMARY KEY);
    CREATE TABLE visible.visible_known(id bigint PRIMARY KEY);
    INSERT INTO visible.visible_known VALUES (1),(2),(3); ANALYZE visible.visible_known;
    CREATE VIEW visible.visible_view AS SELECT * FROM visible.visible_known;
    CREATE TABLE visible.hidden_table(id bigint); CREATE TABLE forbidden.visible_secret(id bigint);
    CREATE TABLE visible.visible(id bigint);
    CREATE ROLE catalog_reader LOGIN PASSWORD '${password}';
    REVOKE EXECUTE ON FUNCTION pg_catalog.pg_control_system() FROM PUBLIC;
    GRANT USAGE ON SCHEMA visible TO catalog_reader;
    GRANT SELECT ON visible.visible_known TO catalog_reader;`);
  const before = (await sql("SELECT json_build_object('rows',(SELECT count(*) FROM visible.visible_known),'database_oid',(SELECT oid::bigint FROM pg_database WHERE datname=current_database()),'system_identifier',(SELECT system_identifier::text FROM pg_control_system()))")).trim();
  const identity = JSON.parse(before);
  const config = { default_page_size: 100, max_page_size: 100, endpoints: [{ object_ref: 'test-catalog', active: true,
    endpoint_uri: 'postgresql://127.0.0.1:' + port + '/catalog_test?sslmode=disable', credential_ref: 'reader', schema_scopes: ['visible'], table_scopes: ['*'] }],
    credentials: [{ credential_ref: 'reader', auth_type: 'basic_env', username_env: 'CATALOG_PG_USER', password_env: 'CATALOG_PG_PASSWORD' }],
    __cnx_runtime: { resource_grants: [{ resource_kind: 'pg', object_ref: 'test-catalog', grant_state: 'active', grant_epoch: 1,
      interfaces: ['read'], schema_scopes: ['visible'], table_scopes: ['visible_*'] }] } };
  const file = path.join(root, 'native.json'); await fs.writeFile(file, JSON.stringify(config), { mode: 0o600 });
  const invoke = async (patch = {}, envelopePatch = {}, environment = env) => {
    const now = Date.now();
    const request = { wire_version: 2, context: { request_id: randomBytes(8).toString('hex'), grant_epoch: 1, issued_at_ms: now, deadline_ms: now + 10000,
      limits: { max_rows: 100, max_bytes: 1048576, max_calls: 3, max_fanout: 1, max_latency_ms: 10000, max_cost_units: 3 }, ...envelopePatch },
      request: { object_ref: 'test-catalog', operation: 'catalog-metadata', limit: 100, ...patch } };
    return JSON.parse(await command(tool, ['read', '--kind', 'pg', '--config', file], JSON.stringify(request), environment));
  };
  const result = await invoke();
  assert.equal(result.response.payload.kind, 'catalog', JSON.stringify(result.response.payload));
  const catalog = result.response.payload.payload, receipt = result.response.receipt;
  assert.equal(catalog.database, 'catalog_test'); assert.equal(catalog.database_oid, identity.database_oid);
  assert.equal(catalog.system_identifier, identity.system_identifier);
  assert.equal(catalog.read_only, true); assert.equal(catalog.complete, true); assert.equal(catalog.grant_epoch, 1);
  assert.deepEqual(catalog.objects.map(x => x.name), ['visible_known', 'visible_unknown', 'visible_view']);
  assert.equal(catalog.objects[0].estimated_rows, 3); assert.equal(catalog.objects[1].estimated_rows, null); assert.equal(catalog.objects[2].estimated_rows, null);
  assert.ok(catalog.objects.every(x => x.row_count_method === 'estimate' && x.schema === 'visible'));
  assert.match(catalog.objects[2].view_definition_md5, /^[a-f0-9]{32}$/);
  assert.equal(receipt.operation, 'catalog-metadata'); assert.equal(receipt.authority_object_ref, 'test-catalog');
  assert.match(receipt.receipt_digest, /^source-receipt:[a-f0-9]{64}$/);
  assert.equal(receipt.actual.rows, 3); assert.equal(receipt.actual.calls, 3);
  const low = await invoke({ limit: 1 }); assert.equal(low.response.payload.kind, 'error'); assert.ok(low.response.receipt == null);
  assert.equal((await invoke({ schema: 'forbidden' })).response.payload.kind, 'error');
  assert.equal((await invoke({}, { grant_epoch: 2 })).response.payload.kind, 'error');
  const limited = await invoke({}, {}, { ...env, CATALOG_PG_USER: 'catalog_reader' });
  assert.equal(limited.response.payload.kind, 'catalog');
  assert.deepEqual(limited.response.payload.payload.objects.map(x => x.name), ['visible_known']);
  assert.equal(limited.response.payload.payload.system_identifier, null);
  assert.equal((await sql('SELECT count(*) FROM visible.visible_known')).trim(), '3');
  assert.equal((await sql('SELECT count(*) FROM visible.visible_unknown')).trim(), '0');
  const overlap = structuredClone(config); overlap.__cnx_runtime.resource_grants[0].table_scopes = ['visible*ble'];
  await fs.writeFile(file, JSON.stringify(overlap));
  const scoped = await invoke(); assert.equal(scoped.response.payload.kind, 'catalog');
  assert.deepEqual(scoped.response.payload.payload.objects, [], 'scope prefix/suffix must not overlap to widen native authority');
  await fs.writeFile(file, JSON.stringify(config));
  const described = JSON.parse(await command(tool, ['describe', '--kind', 'pg', '--config', file], undefined, env));
  assert.ok(described.status.payload.capabilities.operations.includes('catalog-metadata'));
});
