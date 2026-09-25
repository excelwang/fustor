// ASSERTION INTENT (Black-box tests — public traits and APIs only).
// Do not introduce white-box testing logic or internal workarounds.
import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { SourceConfigurationManager } from '../index.mjs';

// This is an actual native validator, not a source-response stub. Configuration
// checks do not read business content; the nonexistent selected file proves it.
const inspector = process.env.FUSTOR_SOURCE_INSPECT;
test('native source configuration without console, provider reads or manual preview', { skip: !inspector }, async t => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'fustor-source-config-'));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const files = path.join(root, 'files'); await fs.mkdir(files);
  const limits = { max_rows: 2, max_bytes: 65536, max_calls: 3, max_fanout: 1, max_latency_ms: 3000, max_cost_units: 3 };
  const template = path.join(root, 'native.json');
  await fs.writeFile(template, JSON.stringify({
    endpoints: [{ object_ref: 'readable', root_path: '/unbound', active: true, path_scopes: ['*'] }],
    __cnx_runtime: { resource_grants: [{ resource_kind: 'jsonl', object_ref: 'readable', grant_state: 'active', grant_epoch: 1, interfaces: ['read'], path_scopes: ['*'] }] },
    default_page_size: 2, max_page_size: 3, retained_native_extension: { keep: true },
  }), { mode: 0o600 });
  const policyFile = path.join(root, 'policy.json');
  await fs.writeFile(policyFile, JSON.stringify({
    schemaVersion: 1, tool: { command: inspector }, allowedTargets: [], roots: { files },
    policies: [{ kind: 'jsonl', nativeTemplate: template, objectRef: 'readable', grantEpoch: 1,
      permissionContext: 'installed-read-scope', defaults: limits, hardCaps: [limits] }],
  }), { mode: 0o600 });
  const configuration = { id: 'sources', directory: path.join(root, 'owner'), policyFile };
  const manager = await SourceConfigurationManager.open(configuration);
  const initial = await manager.describe();
  assert.equal(initial.entries.length, 0);
  await assert.rejects(fs.stat(configuration.directory), { code: 'ENOENT' });
  const declaration = { id: 'records', label: 'Records', kind: 'jsonl', connection: { rootRef: 'files' },
    objectScope: { path: 'not-read.jsonl' }, active: true };
  const input = { declaration, expectedRevision: initial.revision };
  // @verify_spec("CONTRACTS.NATIVE_CONFIGURATION.NATIVE_SOURCE_CONFIGURATION", mode="system")
  const saved = await manager.apply(input);
  assert.equal(saved.saved, true);
  assert.deepEqual(saved.warnings, []);
  await assert.rejects(manager.apply(input), { code: 'REGISTRY_REVISION_CONFLICT' });
  const registered = await manager.describe();
  assert.equal(registered.entries.length, 1);
  const version = path.join(configuration.directory, 'records-' + registered.entries[0].nativeDigest + '.json');
  const bytes = await fs.readFile(version, 'utf8');
  assert.equal(JSON.parse(bytes).retained_native_extension.keep, true);
  const restarted = await SourceConfigurationManager.open(configuration);
  assert.equal((await restarted.describe()).revision, saved.revision);
  const disabled = await restarted.apply({ declaration: { ...declaration, active: false }, expectedRevision: saved.revision });
  assert.equal(disabled.active, false);
  assert.equal(await fs.readFile(version, 'utf8'), bytes);
  const events = await fs.readdir(path.join(configuration.directory, 'operations'));
  assert.equal(events.length, 4);
  const bad = { ...declaration, connection: { rootRef: 'files', directory: '../escape' } };
  await assert.rejects(restarted.apply({ declaration: bad, expectedRevision: disabled.revision }), { code: 'SOURCE_ROOT_FORBIDDEN' });
  assert.equal((await restarted.describe()).revision, disabled.revision);
});
