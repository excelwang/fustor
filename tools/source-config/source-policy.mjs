import fs from 'node:fs/promises';
import path from 'node:path';
import net from 'node:net';
import { isDeepStrictEqual } from 'node:util';
import { ApiError } from './support.mjs';
import { checkHost, invoke } from './native.mjs';
import { digest, readBounded } from './support.mjs';
import { validateTargetScope, scopedEndpoint } from './target-scope.mjs';

const kinds = new Set(['mysql', 'es', 'local-fs', 's3', 'pg', 'jsonl', 'csv', 'graph']);
const fields = ['max_rows', 'max_bytes', 'max_calls', 'max_fanout', 'max_latency_ms', 'max_cost_units'];
const id = value => typeof value === 'string' && /^[a-zA-Z0-9_-]{1,80}$/.test(value);
const text = value => typeof value === 'string' && value.trim().length > 0 && value.length <= 128 && !/[\x00-\x1f\x7f]/.test(value);
const objectName = value => text(value) && !['.', '..'].includes(value) && /^[a-zA-Z0-9_$.-]+$/.test(value);
function shape(value, required, optional = []) {
  if (!value || typeof value !== 'object' || Array.isArray(value) ||
      required.some(key => !Object.hasOwn(value, key)) || Object.keys(value).some(key => ![...required, ...optional].includes(key))) throw new ApiError(422, 'SOURCE_DECLARATION_INVALID');
}
function relative(value, allowRoot = false) {
  if (allowRoot && value === '.') return true;
  return typeof value === 'string' && value.length > 0 && value.length <= 1024 &&
    !/[\x00-\x1f\x7f\\*?\[\]{}]/.test(value) && !path.isAbsolute(value) && value.split('/').every(part => part && part !== '.' && part !== '..');
}
async function privateJson(file) {
  if (!path.isAbsolute(file) || await fs.realpath(file) !== file) throw new Error('private source policy path required');
  const stat = await fs.lstat(file);
  if (stat.uid !== process.getuid() || stat.mode & 0o077) throw new Error('private source policy file required');
  const raw = await readBounded(file, [path.dirname(file)]);
  return { doc: JSON.parse(raw), revision: digest(raw) };
}
function limits(value) {
  shape(value, fields);
  if (fields.some(key => !Number.isSafeInteger(value[key]) || value[key] < 1)) throw new Error('invalid native read limits');
  if (value.max_rows > 10000 || value.max_bytes > 4194304 || value.max_latency_ms > 30000 || value.max_fanout > 1 || value.max_calls > 10 || value.max_cost_units > 10) throw new Error('inspection limits exceeded');
}

// Deployment capability policy only. It has no business inventory, provider
// client, grant generator, query interpreter or workflow responsibilities.
export async function loadSourcePolicy(file) {
  if (!file) return null;
  try {
    const loaded = await privateJson(file), policy = loaded.doc;
    shape(policy, ['schemaVersion', 'tool', 'allowedTargets', 'roots', 'policies'], ['targetScope']);
    if (policy.targetScope) validateTargetScope(policy.targetScope);
    shape(policy.tool, ['command'], ['args', 'envNames', 'cwd']);
    if (policy.schemaVersion !== 1 || !path.isAbsolute(policy.tool.command) ||
        !Array.isArray(policy.tool.args ?? []) || (policy.tool.args ?? []).some(arg => typeof arg !== 'string') ||
        !Array.isArray(policy.tool.envNames ?? []) || (policy.tool.envNames ?? []).some(key => !/^[A-Z_][A-Z0-9_]*$/.test(key)) ||
        (policy.tool.cwd !== undefined && !path.isAbsolute(policy.tool.cwd)) ||
        !Array.isArray(policy.allowedTargets) || policy.allowedTargets.length > 50 ||
        !Array.isArray(policy.policies) || policy.policies.length > kinds.size ||
        !policy.roots || Array.isArray(policy.roots) || typeof policy.roots !== 'object' || Object.keys(policy.roots).length > 30) throw new Error();
    const probePolicy = { allowedHosts: policy.allowedTargets.map(target => target.host), allowedTargets: policy.allowedTargets, sourceScope: policy.targetScope };
    for (const target of policy.allowedTargets) {
      shape(target, ['host', 'ports']);
      if (net.isIP(target.host) !== 4 || !Array.isArray(target.ports) || !target.ports.length || target.ports.length > 20) throw new Error();
      for (const port of target.ports) {
        if (!Number.isSafeInteger(port) || port < 1 || port > 65535) throw new Error();
        if (policy.targetScope && !scopedEndpoint(policy.targetScope, target.host, port)) throw new Error();
        checkHost(probePolicy, 'http://' + target.host + ':' + port);
      }
    }
    for (const [key, root] of Object.entries(policy.roots)) if (!id(key) || typeof root !== 'string' || !path.isAbsolute(root) || root === '/') throw new Error();
    const seen = new Set();
    for (const item of policy.policies) {
      shape(item, ['kind', 'nativeTemplate', 'objectRef', 'grantEpoch', 'permissionContext', 'defaults', 'hardCaps'], ['statusBindings']);
      if (!kinds.has(item.kind) || seen.has(item.kind) || !path.isAbsolute(item.nativeTemplate) || !text(item.objectRef) ||
          !Number.isSafeInteger(item.grantEpoch) || item.grantEpoch < 1 || !text(item.permissionContext) ||
          !Array.isArray(item.hardCaps) || !item.hardCaps.length || item.hardCaps.length > 8) throw new Error();
      limits(item.defaults); item.hardCaps.forEach(limits); seen.add(item.kind);
      if (fields.some(key => item.hardCaps.some(cap => item.defaults[key] > cap[key]))) throw new Error();
      if (item.statusBindings !== undefined) {
        if (item.kind !== 'graph' || !Array.isArray(item.statusBindings) || item.statusBindings.length > 16) throw new Error();
        const identities = new Set();
        for (const binding of item.statusBindings) {
          shape(binding, ['endpointUri', 'statusUrl'], ['statusCredentialRef']);
          const endpoint = new URL(binding.endpointUri), status = new URL(binding.statusUrl);
          if (!['bolt:', 'bolt+s:'].includes(endpoint.protocol) || !['http:', 'https:'].includes(status.protocol)
            || endpoint.username || endpoint.password || endpoint.search || endpoint.hash || status.username || status.password || status.search || status.hash
            || identities.has(binding.endpointUri) || (binding.statusCredentialRef !== undefined && !text(binding.statusCredentialRef))) throw new Error();
          checkHost(probePolicy, binding.endpointUri); checkHost(probePolicy, binding.statusUrl);
          identities.add(binding.endpointUri);
        }
      }
    }
    return { ...policy, file, revision: loaded.revision };
  } catch { throw new Error('Invalid FUSTOR_SOURCE_POLICY_FILE; private bounded native capability policy required'); }
}

export function attachSourcePolicy(instance, policy, contextDirectory) {
  if (!contextDirectory && !instance.agentSourcePolicy) return instance;
  const directory = instance.sourceRegistry?.directory ?? path.join(contextDirectory, 'sources');
  return { ...instance, sourcePolicy: policy, sourceRegistry: { ...instance.sourceRegistry, directory, managed: !instance.sourceRegistry },
    allowedRoots: [...new Set([...instance.allowedRoots, directory, ...Object.values(policy?.roots ?? {})])],
    ...(policy ? { tools: { ...instance.tools, source: policy.tool }, allowedHosts: [...new Set([...instance.allowedHosts ?? [], ...policy.allowedTargets.map(target => target.host)])],
      allowedTargets: [...instance.allowedTargets ?? [], ...policy.allowedTargets], sourceScope: policy.targetScope } : {}) };
}

async function template(instance, kind) {
  const policy = instance.sourcePolicy, item = policy?.policies.find(item => item.kind === kind);
  if (!item) throw new ApiError(403, 'SOURCE_KIND_NOT_ALLOWED');
  try {
    if ((await privateJson(policy.file)).revision !== policy.revision) throw new ApiError(409, 'SOURCE_POLICY_CHANGED');
    const native = await privateJson(item.nativeTemplate);
    if (!Array.isArray(native.doc.endpoints) || native.doc.endpoints.length !== 1 || native.doc.endpoints[0].object_ref !== item.objectRef) throw new Error();
    return { item, native, policyDigest: digest([policy.revision, item, native.revision]) };
  } catch (error) { if (error instanceof ApiError) throw error; throw new ApiError(503, 'SOURCE_TEMPLATE_UNAVAILABLE'); }
}

export async function policyDescription(instance) {
  if (!instance.sourcePolicy) return [];
  return Promise.all(instance.sourcePolicy.policies.map(async item => {
    try {
      try { await fs.access(instance.sourcePolicy.tool.command, fs.constants.X_OK); }
      catch { throw new ApiError(503, 'SOURCE_INSPECTION_UNAVAILABLE'); }
      const { native } = await template(instance, item.kind);
      return { kind: item.kind, available: true, credentialRefs: (native.doc.credentials ?? []).map(item => item.credential_ref),
        ...(item.kind === 'graph' ? { queryIds: (native.doc.endpoints[0].queries ?? []).map(query => query.query_id) } : {}),
        ...(['local-fs', 'jsonl', 'csv'].includes(item.kind) ? { rootRefs: Object.keys(instance.sourcePolicy.roots) } : { targets: instance.sourcePolicy.allowedTargets }) };
    } catch (error) { return { kind: item.kind, available: false, error: error instanceof ApiError ? error.code : 'SOURCE_POLICY_UNAVAILABLE' }; }
  }));
}

export async function checkRegisteredPolicy(instance, entry) {
  const current = await template(instance, entry.kind);
  if (current.policyDigest !== entry.policyDigest) throw new ApiError(409, 'REGISTERED_POLICY_DRIFT');
}

export async function prepareDeclaration(instance, declaration, previousNative) {
  if (declaration && Object.hasOwn(declaration, 'objectScopes')) {
    shape(declaration, ['id', 'label', 'kind', 'connection', 'objectScopes', 'active'], ['credentialRef']);
    const scopes = declaration.objectScopes;
    if (!Array.isArray(scopes) || !scopes.length || scopes.length > 16) throw new ApiError(422, 'SOURCE_SCOPES_INVALID');
    const ids = new Set(), prepared = [];
    for (const scope of scopes) {
      shape(scope, ['id', 'objectScope'], ['label']);
      if (!id(scope.id) || ids.has(scope.id) || (scope.label !== undefined && !text(scope.label))) throw new ApiError(422, 'SOURCE_SCOPES_INVALID');
      ids.add(scope.id);
      const { objectScopes, ...single } = declaration;
      void objectScopes;
      const value = await prepareDeclaration(instance, { ...single, objectScope: scope.objectScope }, previousNative);
      if (prepared.length && (!isDeepStrictEqual(prepared[0].doc, value.doc) || prepared[0].policyDigest !== value.policyDigest)) {
        throw new ApiError(422, 'SOURCE_SCOPE_CONFIGURATION_CONFLICT');
      }
      prepared.push({ ...value, source: { ...value.source, operations: value.source.operations.map(operation => ({
        ...operation, id: scope.id + ':' + operation.id, label: (scope.label ?? scope.id) + ' · ' + operation.label,
        scopeId: scope.id, objectScope: structuredClone(scope.objectScope),
      })) } });
    }
    return { ...prepared[0], source: { ...prepared[0].source, operations: prepared.flatMap(value => value.source.operations) } };
  }
  shape(declaration, ['id', 'label', 'kind', 'connection', 'objectScope', 'active'], ['credentialRef']);
  if (!id(declaration.id) || !text(declaration.label) || declaration.label.length > 120 || typeof declaration.active !== 'boolean' || !kinds.has(declaration.kind)) throw new ApiError(422, 'SOURCE_DECLARATION_INVALID');
  const { item, native, policyDigest } = await template(instance, declaration.kind);
  // Keep previously saved unknown native fields. A changed template is rejected
  // by the registry before update; authority is never merged or manufactured.
  const doc = structuredClone(previousNative ?? native.doc), endpoint = doc.endpoints[0];
  const selection = { object_ref: item.objectRef, grant_epoch: item.grantEpoch };
  const c = declaration.connection, s = declaration.objectScope, kind = declaration.kind;
  if (['local-fs', 'jsonl', 'csv'].includes(kind)) {
    shape(c, ['rootRef'], ['directory']); shape(s, ['path']);
    if (!id(c.rootRef) || !Object.hasOwn(instance.sourcePolicy.roots, c.rootRef) ||
        (c.directory !== undefined && !relative(c.directory)) || !relative(s.path, kind === 'local-fs')) throw new ApiError(403, 'SOURCE_ROOT_FORBIDDEN');
    const allowed = instance.sourcePolicy.roots[c.rootRef], root = path.join(allowed, c.directory ?? '');
    try {
      const realAllowed = await fs.realpath(allowed), realRoot = await fs.realpath(root);
      if (realAllowed !== allowed || realRoot !== root || !(await fs.stat(root)).isDirectory()) throw new Error();
    } catch { throw new ApiError(403, 'SOURCE_ROOT_FORBIDDEN'); }
    if (declaration.credentialRef !== undefined) throw new ApiError(403, 'SOURCE_CREDENTIAL_NOT_ALLOWED');
    endpoint.root_path = root; selection.path = s.path;
  } else {
    shape(c, ['host', 'port', ...(['pg', 'mysql'].includes(kind) ? ['database'] : ['tls'])], kind === 's3' ? ['region'] : kind === 'graph' ? ['database'] : []);
    if (net.isIP(c.host) !== 4 || !Number.isSafeInteger(c.port) || c.port < 1 || c.port > 65535 ||
        (!['pg', 'mysql'].includes(kind) && typeof c.tls !== 'boolean')) throw new ApiError(422, 'SOURCE_CONNECTION_INVALID');
    const protocol = kind === 'pg' ? 'postgresql' : kind === 'mysql' ? 'mysql' : kind === 'graph' ? (c.tls ? 'bolt+s' : 'bolt') : (c.tls ? 'https' : 'http');
    const suffix = ['pg', 'mysql'].includes(kind) ? '/' + encodeURIComponent(c.database) : '';
    // URI replacement is not a transport-policy change. Keep the native legacy
    // TLS requirement; structured endpoint.tls survives in the cloned document.
    const mysqlTls = kind === 'mysql' && [...new URL(endpoint.endpoint_uri).searchParams]
      .some(([key, value]) => key.toLowerCase() === 'ssl' && value.toLowerCase() === 'true');
    const uri = protocol + '://' + c.host + ':' + c.port + suffix + (mysqlTls ? '?ssl=true' : '');
    checkHost({ allowedHosts: instance.sourcePolicy.allowedTargets.map(target => target.host), allowedTargets: instance.sourcePolicy.allowedTargets, sourceScope: instance.sourcePolicy.targetScope }, uri);
    endpoint.endpoint_uri = uri;
    if (declaration.credentialRef !== undefined && (!text(declaration.credentialRef) || !(doc.credentials ?? []).some(item => item.credential_ref === declaration.credentialRef))) throw new ApiError(403, 'SOURCE_CREDENTIAL_NOT_ALLOWED');
    if (declaration.credentialRef !== undefined) endpoint.credential_ref = declaration.credentialRef;
    else delete endpoint.credential_ref;
    if (['pg', 'mysql'].includes(kind)) {
      if (kind === 'pg' && Object.hasOwn(s, 'catalogMetadata')) {
        shape(s, ['catalogMetadata']);
        if (s.catalogMetadata !== true || !objectName(c.database)) throw new ApiError(422, 'SOURCE_OBJECT_INVALID');
        selection.catalog_metadata = true;
      } else {
        shape(s, ['schema', 'table'], ['primaryKey']);
        if (![c.database, s.schema, s.table, ...(s.primaryKey === undefined ? [] : [s.primaryKey])].every(objectName)) throw new ApiError(422, 'SOURCE_OBJECT_INVALID');
        selection.schema = s.schema; selection.table = s.table;
        if (s.primaryKey !== undefined) endpoint.primary_key = s.primaryKey; else delete endpoint.primary_key;
      }
    } else if (kind === 'es') {
      shape(s, ['index']); if (!objectName(s.index)) throw new ApiError(422, 'SOURCE_OBJECT_INVALID');
      selection.index = s.index;
    } else if (kind === 's3') {
      shape(s, ['bucket', 'prefix']);
      if (!objectName(s.bucket) || typeof s.prefix !== 'string' || s.prefix.length > 1024 || /[\x00-\x1f\x7f]/.test(s.prefix) || (c.region !== undefined && !objectName(c.region))) throw new ApiError(422, 'SOURCE_OBJECT_INVALID');
      endpoint.bucket = s.bucket; endpoint.prefix = s.prefix;
      if (c.region !== undefined) endpoint.region = c.region; else delete endpoint.region;
      selection.bucket = s.bucket; selection.prefix = s.prefix;
    } else {
      shape(s, ['queryId']);
      if (!text(s.queryId) || (c.database !== undefined && !objectName(c.database))) throw new ApiError(422, 'SOURCE_OBJECT_INVALID');
      if (c.database !== undefined) endpoint.database = c.database; else delete endpoint.database;
      // Only the owner may associate management/status with the selected graph.
      // Never inherit a status endpoint or status credential from a prior target.
      delete endpoint.status_url;
      delete endpoint.status_credential_ref;
      const status = item.statusBindings?.find(binding => binding.endpointUri === endpoint.endpoint_uri);
      if (status) {
        endpoint.status_url = status.statusUrl;
        if (status.statusCredentialRef !== undefined) {
          if (!(doc.credentials ?? []).some(value => value.credential_ref === status.statusCredentialRef)) throw new ApiError(403, 'SOURCE_STATUS_CREDENTIAL_NOT_ALLOWED');
          endpoint.status_credential_ref = status.statusCredentialRef;
        }
      }
      selection.configured_query_id = s.queryId;
    }
    const grants = doc.__cnx_runtime?.resource_grants ?? [];
    for (const grant of grants.filter(grant => grant.object_ref === item.objectRef)) {
      for (const key of ['endpoint_uri', 'credential_ref', 'bucket']) {
        if (grant[key] !== undefined && grant[key] !== null && grant[key] !== endpoint[key]) throw new ApiError(403, 'NATIVE_ENDPOINT_OVERRIDE_CONFLICT');
      }
    }
  }
  try { await invoke(instance.tools?.source, ['validate', '--kind', kind, '--config', '-'], doc); }
  catch (error) {
    if (error.code === 'source-inspect-error') throw new ApiError(422, 'NATIVE_CONFIGURATION_INVALID');
    throw new ApiError(503, 'SOURCE_INSPECTION_UNAVAILABLE');
  }
  let validation;
  try { validation = await invoke(instance.tools?.source, ['validate-scope', '--kind', kind, '--config', '-'], { configuration: doc, selection }); }
  catch (error) {
    if (error.code === 'source-scope-invalid') throw new ApiError(403, 'NATIVE_SOURCE_SCOPE_DENIED');
    throw new ApiError(503, 'SOURCE_SCOPE_PREFLIGHT_UNAVAILABLE');
  }
  if (validation.valid !== true || validation.scopeAuthorized !== true || validation.readiness !== 'not-observed') throw new ApiError(502, 'NATIVE_SCOPE_RESPONSE_INVALID');
  const request = { ...selection }; delete request.grant_epoch;
  delete request.catalog_metadata;
  let operations;
  const operation = (id, request) => ({ id, label: id, request, defaults: item.defaults, hardCaps: item.hardCaps });
  if (kind === 'pg' && s.catalogMetadata === true) {
    const limit = Math.min(item.defaults.max_rows, doc.max_page_size ?? item.defaults.max_rows, 5000);
    operations = [{ ...operation('catalog-metadata', { ...request, operation: 'catalog-metadata', limit }),
      defaults: { ...item.defaults, max_rows: limit } }];
  } else if (kind === 'graph') {
    const query = endpoint.queries.find(query => query.query_id === s.queryId);
    const defaults = Object.fromEntries(fields.map(key => [key, Math.min(item.defaults[key], query.limits[key])]));
    operations = [{ ...operation('configured-query', { ...request, operation: 'configured-query', expected_result_schema_ref: query.result_schema_ref, limit: defaults.max_rows }),
      defaults, hardCaps: [...item.hardCaps, query.limits] }];
  } else {
    const limit = Math.min(item.defaults.max_rows, doc.max_page_size ?? item.defaults.max_rows);
    // Relational snapshots require an opening request; a continuation needs a
    // live owner-issued cursor, which this one-shot inspection does not retain.
    const firstRead = ['pg', 'mysql'].includes(kind) ? 'snapshot-open' : 'snapshot-page';
    operations = [firstRead, 'discover-fields', ...(['local-fs', 's3'].includes(kind) ? ['sample-jsonl', 'sample-csv'] : [])]
      .map(name => ({ ...operation(name, { ...request, operation: name, limit }), defaults: { ...item.defaults, max_rows: limit } }));
  }
  return { doc, validation, policyDigest, nativeRevision: native.revision,
    source: { kind, objectRef: item.objectRef, grantEpoch: item.grantEpoch, permissionContext: item.permissionContext,
      operations, probeRequest: kind === 'graph' ? {} : { ...request } } };
}
