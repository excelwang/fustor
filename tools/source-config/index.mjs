import fs from 'node:fs/promises';
import path from 'node:path';
import { SourceRegistry } from './source-registry.mjs';
import { loadSourcePolicy, attachSourcePolicy } from './source-policy.mjs';
import { ApiError, digest, readBounded } from './support.mjs';

export { ApiError as SourceConfigurationError, SourceRegistry, loadSourcePolicy, attachSourcePolicy };

export class SourceConfigurationManager {
  static async open(configuration) {
    if (!configuration || Object.keys(configuration).some(key => !['id', 'directory', 'policyFile'].includes(key)) ||
        !/^[a-zA-Z0-9_-]{1,80}$/.test(configuration.id) || !path.isAbsolute(configuration.directory) ||
        configuration.directory === '/' || !path.isAbsolute(configuration.policyFile)) throw new ApiError(422, 'SOURCE_OWNER_CONFIGURATION_INVALID');
    const policy = await loadSourcePolicy(configuration.policyFile);
    if (!policy) throw new ApiError(503, 'SOURCE_POLICY_UNAVAILABLE');
    const directory = configuration.directory;
    if (path.normalize(directory) !== directory) throw new ApiError(403, 'SOURCE_OWNER_ROOT_NOT_PRIVATE');
    try {
      const stat = await fs.lstat(directory);
      if (await fs.realpath(directory) !== directory || !stat.isDirectory() || stat.uid !== process.getuid() || stat.mode & 0o077) throw new ApiError(403, 'SOURCE_OWNER_ROOT_NOT_PRIVATE');
    } catch (error) { if (error.code !== 'ENOENT') throw error; }
    const scope = attachSourcePolicy({ id: configuration.id, sources: [], configurations: [], allowedRoots: [directory] }, policy, directory);
    scope.sourceRegistry = { directory, managed: true };
    const read = async (scope, config) => {
      const raw = await readBounded(config.path, scope.allowedRoots);
      return { doc: JSON.parse(raw), raw, revision: digest(raw), config };
    };
    const audit = async event => {
      const auditRoot = path.join(directory, 'operations');
      await fs.mkdir(auditRoot, { mode: 0o700, recursive: true });
      const stat = await fs.lstat(auditRoot);
      if (await fs.realpath(auditRoot) !== auditRoot || !stat.isDirectory() || stat.uid !== process.getuid() || stat.mode & 0o077) throw new ApiError(403, 'SOURCE_AUDIT_ROOT_NOT_PRIVATE');
      const raw = JSON.stringify({ ...event, observedAt: new Date().toISOString() }) + '\n', file = path.join(auditRoot, digest(raw) + '.json');
      const handle = await fs.open(file, 'wx', 0o600);
      try { await handle.writeFile(raw); await handle.sync(); } finally { await handle.close(); }
      // The receipt file and every newly created ancestor entry must survive a
      // restart before the registry can acknowledge and activate a new version.
      for (let current = auditRoot; ; current = path.dirname(current)) {
        const directoryHandle = await fs.open(current, 'r');
        try { await directoryHandle.sync(); } finally { await directoryHandle.close(); }
        if (path.dirname(current) === current) break;
      }
    };
    const owners = { invalidate() {}, native: (scope, source) => read(scope, scope.configurations.find(item => item.id === source.configuration)) };
    return new SourceConfigurationManager(scope, new SourceRegistry({ read }, owners, audit));
  }
  constructor(scope, registry) { this.scope = scope; this.registry = registry; }
  describe() { return this.registry.describe(this.scope, true); }
  resolve() { return this.registry.resolve(this.scope); }
  async apply(request) {
    if (!request || Object.keys(request).some(key => !['declaration', 'expectedRevision'].includes(key)) || !Object.hasOwn(request, 'expectedRevision')) throw new ApiError(422, 'SOURCE_APPLY_INPUT_INVALID');
    const preview = await this.registry.preview(this.scope, request, 'source-owner');
    return this.registry.save(this.scope, { ...request, previewToken: preview.previewToken }, 'source-owner');
  }
}
