import fs from 'node:fs/promises';
import path from 'node:path';
import { createHash } from 'node:crypto';

export class ApiError extends Error {
  constructor(status, code) { super(code); this.status = status; this.code = code; }
}
export const digest = value => createHash('sha256').update(typeof value === 'string' || Buffer.isBuffer(value) ? value : JSON.stringify(value)).digest('hex');
export const valueAt = (value, key) => key.split('.').reduce((object, part) => object?.[part], value);
export async function resolvedRoots(roots) {
  return (await Promise.all(roots.map(root => fs.realpath(root).catch(() => null)))).filter(Boolean);
}
export async function readBounded(file, roots, maxBytes = 2097152) {
  const real = await fs.realpath(file), allowed = await resolvedRoots(roots);
  if (!allowed.some(root => real === root || real.startsWith(root + path.sep))) throw new ApiError(403, 'PATH_OUTSIDE_ALLOWED_ROOT');
  const handle = await fs.open(real, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
  try {
    const stat = await handle.stat();
    if (!stat.isFile() || stat.size > maxBytes) throw new ApiError(413, 'FILE_LIMIT');
    const buffer = Buffer.alloc(maxBytes + 1), { bytesRead } = await handle.read(buffer, 0, buffer.length, 0);
    if (bytesRead > maxBytes) throw new ApiError(413, 'FILE_LIMIT');
    return buffer.subarray(0, bytesRead).toString('utf8');
  } finally { await handle.close(); }
}
