import fs from 'node:fs/promises';
import path from 'node:path';
import { createHmac, randomBytes } from 'node:crypto';
import { ApiError } from './support.mjs';
import { digest, readBounded, resolvedRoots } from './support.mjs';
import { invoke } from './native.mjs';
import { checkRegisteredPolicy, policyDescription, prepareDeclaration } from './source-policy.mjs';
import { acquireSubmissionLock } from './submission-lock.mjs';
import { isDeepStrictEqual } from 'node:util';

const identifier = value => typeof value === 'string' && /^[a-zA-Z0-9_-]{1,80}$/.test(value);
const kinds = new Set(['mysql', 'es', 'local-fs', 's3', 'pg', 'jsonl', 'csv', 'graph']);
async function syncDirectory(directory) {
  const handle = await fs.open(directory, 'r');
  try { await handle.sync(); } finally { await handle.close(); }
}
async function immutable(file, text) {
  let handle;
  try {
    handle = await fs.open(file, 'wx', 0o600);
    await handle.writeFile(text); await handle.sync();
  } catch (error) {
    if (error.code !== 'EEXIST') throw error;
    if ((await fs.lstat(file)).isSymbolicLink() || await fs.readFile(file, 'utf8') !== text) throw new ApiError(409, 'IMMUTABLE_CONFIG_CONFLICT');
  } finally { await handle?.close(); }
  await syncDirectory(path.dirname(file));
}

// Configuration inventory only: native files are committed first and referenced by
// content digest. No source data, workflow state or credentials are stored here.
export class SourceRegistry {
  constructor(configurations, owners, audit) {
    this.configurations = configurations; this.owners = owners; this.audit = audit;
    this.key = randomBytes(32);
  }
  async directory(instance, create = false) {
    const directory = instance.sourceRegistry?.directory;
    if (!directory || !path.isAbsolute(directory)) throw new ApiError(503, 'SOURCE_MANAGEMENT_NOT_CONFIGURED');
    if (create && instance.sourceRegistry.managed) await fs.mkdir(directory, { recursive: true, mode: 0o700 });
    const resolved = await fs.realpath(directory);
    if (resolved !== directory || !(await resolvedRoots(instance.allowedRoots)).some(root => resolved === root || resolved.startsWith(root + path.sep))) throw new ApiError(403, 'REGISTRY_ROOT_FORBIDDEN');
    return resolved;
  }
  async read(instance) {
    if (!instance.sourceRegistry) return { document: { schemaVersion: 1, entries: [] }, revision: null };
    let directory;
    try { directory = await this.directory(instance); }
    catch (error) {
      if (error.code !== 'ENOENT' || !instance.sourceRegistry.managed) throw error;
      const document = { schemaVersion: 1, entries: [] }; return { document, revision: digest(JSON.stringify(document)) };
    }
    const file = path.join(directory, 'sources.json');
    let raw;
    try { raw = await readBounded(file, [directory]); }
    catch (error) { if (error.code !== 'ENOENT') throw error; raw = JSON.stringify({ schemaVersion: 1, entries: [] }); }
    const document = JSON.parse(raw);
    if (document.schemaVersion !== 1 || !Array.isArray(document.entries) || document.entries.length > 50) throw new ApiError(503, 'REGISTRY_INVALID');
    const ids = new Set();
    for (const entry of document.entries) {
      const typed = entry.declaration !== undefined;
      if (!identifier(entry.id) || ids.has(entry.id) || !/^[a-f0-9]{64}$/.test(entry.nativeDigest) ||
          (typed ? !kinds.has(entry.kind) || entry.declaration.id !== entry.id || entry.declaration.kind !== entry.kind ||
            !/^[a-f0-9]{64}$/.test(entry.policyDigest ?? '') || !entry.source?.operations?.length
            : !identifier(entry.profileId) || !/^[a-f0-9]{64}$/.test(entry.profileDigest ?? ''))) throw new ApiError(503, 'REGISTRY_INVALID');
      ids.add(entry.id);
    }
    return { document, revision: digest(raw) };
  }
  profiles(instance) {
    return (instance.sourceProfiles ?? []).slice(0, 50).map(profile => ({ id: profile.id, label: profile.label,
      kind: profile.kind, scope: profile.scopeLabel, target: profile.targetLabel, credentialRef: profile.credentialRefLabel }));
  }
  publicEntry(entry, machine = false) {
    return { id: entry.id, label: entry.label, kind: entry.kind, profileId: entry.profileId, active: entry.active, nativeDigest: entry.nativeDigest,
      ...(machine && entry.declaration ? { declaration: entry.declaration } : {}) };
  }
  async describe(instance, machine = false) {
    const state = await this.read(instance);
    return { enabled: Boolean(instance.sourcePolicy || instance.sourceProfiles?.length), profiles: this.profiles(instance), revision: state.revision,
      ...(machine ? { policies: await policyDescription(instance) } : {}),
      entries: state.document.entries.map(entry => this.publicEntry(entry, machine)) };
  }
  async resolve(instance) {
    const state = await this.read(instance);
    if (!state.document.entries.length) return instance;
    const directory = await this.directory(instance);
    const sources = [...instance.sources], configurations = [...instance.configurations];
    for (const entry of state.document.entries) {
      if (entry.declaration) {
        const configId = 'registered-' + entry.id, file = path.join(directory, entry.id + '-' + entry.nativeDigest + '.json');
        let registrationError;
        try {
          await checkRegisteredPolicy(instance, entry);
          if (digest(await readBounded(file, [directory])) !== entry.nativeDigest) throw new ApiError(409, 'REGISTERED_NATIVE_CONFIG_DRIFT');
        } catch (error) { registrationError = error instanceof ApiError ? error.code : 'REGISTERED_SOURCE_UNAVAILABLE'; }
        configurations.push({ id: configId, label: entry.label, owner: 'fustor', path: file, format: 'json', activation: 'next-read', fields: [] });
        const source = { ...entry.source, id: entry.id, kind: entry.kind, label: entry.label, active: entry.active,
          configuration: configId, registeredDeclaration: true, registrationError };
        const index = sources.findIndex(item => item.id === entry.id);
        if (index < 0) sources.push(source); else sources[index] = source;
        continue;
      }
      const profile = instance.sourceProfiles?.find(profile => profile.id === entry.profileId);
      const configId = 'registered-' + entry.id;
      const file = path.join(directory, entry.id + '-' + entry.nativeDigest + '.json');
      let registrationError;
      try {
        if (!profile) throw new ApiError(503, 'SOURCE_PROFILE_UNAVAILABLE');
        if (entry.profileDigest !== digest([profile.kind, profile.configuration, profile.source])) throw new ApiError(409, 'REGISTERED_PROFILE_DRIFT');
        if (digest(await readBounded(file, [directory])) !== entry.nativeDigest) throw new ApiError(409, 'REGISTERED_NATIVE_CONFIG_DRIFT');
      } catch (error) { registrationError = error instanceof ApiError ? error.code : 'REGISTERED_SOURCE_UNAVAILABLE'; }
      configurations.push({ id: configId, label: entry.label, owner: 'fustor', path: file, format: 'json', activation: 'next-read', fields: [] });
      const source = { operations: [], objectRef: entry.id, ...profile?.source, kind: profile?.kind ?? 'unknown', id: entry.id, label: entry.label, configuration: configId,
        active: entry.active, profileId: entry.profileId, registrationError };
      const index = sources.findIndex(source => source.id === entry.id);
      if (index < 0) sources.push(source); else sources[index] = source;
    }
    if (sources.length > 50) throw new ApiError(422, 'SOURCE_INVENTORY_LIMIT');
    return { ...instance, sources, configurations };
  }
  async prepare(instance, input) {
    if (input.declaration !== undefined) return this.prepareTyped(instance, input);
    if (Object.keys(input).some(key => !['id', 'label', 'profileId', 'active', 'expectedRevision', 'previewToken'].includes(key))) throw new ApiError(403, 'SOURCE_FIELD_FORBIDDEN');
    if (!identifier(input.id) || typeof input.label !== 'string' || input.label.length < 1 || input.label.length > 120 || /[\x00-\x1f]/.test(input.label) || typeof input.active !== 'boolean') throw new ApiError(422, 'SOURCE_INPUT_INVALID');
    const state = await this.read(instance);
    if (state.revision !== input.expectedRevision) throw new ApiError(409, 'REGISTRY_REVISION_CONFLICT');
    if (state.document.entries.some(entry => entry.id === input.id && entry.declaration)) throw new ApiError(409, 'SOURCE_KIND_IMMUTABLE');
    const profile = instance.sourceProfiles?.find(profile => profile.id === input.profileId);
    if (!profile || !kinds.has(profile.kind) || !identifier(profile.id)) throw new ApiError(403, 'SOURCE_PROFILE_NOT_ALLOWED');
    const configured = instance.configurations.find(config => config.id === profile.configuration);
    if (!configured) throw new ApiError(503, 'SOURCE_PROFILE_UNAVAILABLE');
    const native = await this.owners.native(instance, { ...profile.source, kind: profile.kind, configuration: configured.id });
    try { await invoke(instance.tools?.source, ['validate', '--kind', profile.kind, '--config', '-'], native.doc); }
    catch { throw new ApiError(422, 'NATIVE_CONFIGURATION_INVALID'); }
    const serialized = JSON.stringify(native.doc, null, 2) + '\n';
    const entry = { id: input.id, label: input.label, profileId: profile.id, active: input.active, nativeDigest: digest(serialized),
      profileDigest: digest([profile.kind, profile.configuration, profile.source]) };
    const entries = state.document.entries.filter(entry => entry.id !== input.id);
    entries.push(entry);
    if (new Set([...instance.sources.map(source => source.id), ...entries.map(entry => entry.id)]).size > 50) throw new ApiError(422, 'SOURCE_INVENTORY_LIMIT');
    return { entry, serialized, revision: state.revision, nativeRevision: native.revision, document: { schemaVersion: 1, entries },
      before: state.document.entries.find(entry => entry.id === input.id) ?? null };
  }
  async prepareTyped(instance, input) {
    if (Object.keys(input).some(key => !['declaration', 'expectedRevision', 'previewToken'].includes(key))) throw new ApiError(403, 'SOURCE_FIELD_FORBIDDEN');
    const state = await this.read(instance), declaration = input.declaration;
    if (state.revision !== input.expectedRevision) throw new ApiError(409, 'REGISTRY_REVISION_CONFLICT');
    if (!declaration || typeof declaration !== 'object' || Array.isArray(declaration)) throw new ApiError(422, 'SOURCE_DECLARATION_INVALID');
    const before = state.document.entries.find(entry => entry.id === declaration.id);
    if (instance.sources.some(source => source.id === declaration.id) && !before) throw new ApiError(409, 'SOURCE_ID_ALREADY_CONFIGURED');
    if (before && (!before.declaration || before.kind !== declaration.kind)) throw new ApiError(409, 'SOURCE_KIND_IMMUTABLE');
    let previousRaw;
    if (before) {
      previousRaw = await readBounded(path.join(await this.directory(instance), before.id + '-' + before.nativeDigest + '.json'), [instance.sourceRegistry.directory]);
      if (digest(previousRaw) !== before.nativeDigest) throw new ApiError(409, 'REGISTERED_NATIVE_CONFIG_DRIFT');
    }
    let entry, serialized, nativeRevision, validation;
    if (before && declaration.active === false && isDeepStrictEqual(declaration, { ...before.declaration, active: false })) {
      // Revoked policy must not prevent an Agent from stopping future access.
      entry = { ...before, active: false, declaration }; serialized = previousRaw; nativeRevision = before.nativeDigest;
      validation = { valid: true, scopeAuthorized: false, readiness: 'inactive' };
    } else {
      if (before) await checkRegisteredPolicy(instance, before);
      const prepared = await prepareDeclaration(instance, declaration, previousRaw ? JSON.parse(previousRaw) : undefined);
      serialized = JSON.stringify(prepared.doc, null, 2) + '\n'; nativeRevision = prepared.nativeRevision; validation = prepared.validation;
      entry = { id: declaration.id, label: declaration.label, kind: declaration.kind, active: declaration.active, declaration,
        nativeDigest: digest(serialized), policyDigest: prepared.policyDigest, source: prepared.source };
    }
    const entries = [...state.document.entries.filter(item => item.id !== entry.id), entry];
    if (new Set([...instance.sources.map(source => source.id), ...entries.map(entry => entry.id)]).size > 50) throw new ApiError(422, 'SOURCE_INVENTORY_LIMIT');
    return { entry, serialized, revision: state.revision, nativeRevision, validation, document: { schemaVersion: 1, entries }, before: before ?? null };
  }
  token(instance, prepared, expiresAt, actor) {
    return createHmac('sha256', this.key).update(JSON.stringify([actor, instance.id, prepared.revision, prepared.nativeRevision, prepared.entry, expiresAt])).digest('hex') + '.' + expiresAt;
  }
  async preview(instance, input, actor) {
    if (!instance.sourcePolicy && !instance.sourceProfiles?.length && !input.declaration) throw new ApiError(503, 'SOURCE_MANAGEMENT_NOT_CONFIGURED');
    const prepared = await this.prepare(instance, input), expiresAt = Date.now() + 300000;
    return { valid: true, diff: { before: prepared.before ? this.publicEntry(prepared.before, true) : null, after: this.publicEntry(prepared.entry, true) },
      nativeValidation: prepared.validation ?? { valid: true, readiness: 'not-observed' }, previewToken: this.token(instance, prepared, expiresAt, actor),
      expiresAt, activation: 'next-read', note: 'Native config first; registry last. Deactivation retains evidence and does not stop a provider.' };
  }
  async save(instance, input, actor) {
    const directory = await this.directory(instance, true), lock = path.join(directory, '.sources.lock');
    let handle, nativeLock;
    try {
      if (instance.sourceRegistry.managed) nativeLock = await acquireSubmissionLock(directory);
      else handle = await fs.open(lock, 'wx', 0o600);
    } catch (error) { if (error.code === 'EEXIST' || error.code === 'EXECUTION_SUBMISSION_BUSY') throw new ApiError(409, 'REGISTRY_WRITE_BUSY'); throw error; }
    let committed = false;
    const operationId = randomBytes(32).toString('hex'), warnings = [];
    const tmp = path.join(directory, '.sources-' + operationId);
    let intentRecorded = false, staged = false;
    try {
      const prepared = await this.prepare(instance, input), expiresAt = Number(String(input.previewToken).split('.')[1]);
      if (!Number.isFinite(expiresAt) || expiresAt <= Date.now() || input.previewToken !== this.token(instance, prepared, expiresAt, actor)) throw new ApiError(409, 'PREVIEW_STALE');
      await this.audit({ actor, principalId: actor, operationId, action: 'source.registration.requested', instance: instance.id, source: prepared.entry.id, from: prepared.revision, nativeDigest: prepared.entry.nativeDigest });
      intentRecorded = true; staged = true;
      await immutable(path.join(directory, prepared.entry.id + '-' + prepared.entry.nativeDigest + '.json'), prepared.serialized);
      const raw = JSON.stringify(prepared.document, null, 2) + '\n';
      await immutable(tmp, raw);
      if ((await this.read(instance)).revision !== prepared.revision) throw new ApiError(409, 'REGISTRY_REVISION_CONFLICT');
      await nativeLock?.assertOwned();
      await fs.rename(tmp, path.join(directory, 'sources.json')); committed = true;
      await syncDirectory(directory).catch(() => warnings.push('SAVED_DIRECTORY_SYNC_UNCONFIRMED'));
      await this.audit({ actor, principalId: actor, operationId, action: 'source.registration.saved', instance: instance.id, source: prepared.entry.id, to: digest(raw), active: prepared.entry.active })
        .catch(() => warnings.push('SAVED_AUDIT_CONFIRMATION_UNAVAILABLE'));
      this.owners.invalidate(instance.id);
      return { saved: true, sourceId: prepared.entry.id, operationId, revision: digest(raw), active: prepared.entry.active, activation: 'next-read', warnings };
    } catch (error) {
      const failure = error instanceof ApiError ? error : new ApiError(503, 'SOURCE_REGISTRATION_UNAVAILABLE');
      if (intentRecorded) {
        failure.operationId = operationId;
        if (staged && !committed) failure.stagedConfiguration = 'non-active';
        await this.audit({ actor, principalId: actor, operationId, instance: instance.id, action: 'source.registration.failed', code: failure.code,
          stagedConfiguration: failure.stagedConfiguration }).catch(() => {});
      }
      throw failure;
    } finally {
      if (handle) { await handle.close(); await fs.unlink(lock); }
      await nativeLock?.close().catch(error => { if (committed) warnings.push('SAVED_LOCK_RELEASE_UNCONFIRMED'); else throw error; });
      if (!committed) await fs.unlink(tmp).catch(() => {});
    }
  }
}
