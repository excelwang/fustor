import { execFile } from 'node:child_process';
import path from 'node:path';
import { ApiError } from './support.mjs';

export function checkHost(scope, value) {
  const url = new URL(value);
  const port = Number(url.port || ({http: 80, https: 443, postgresql: 5432, mysql: 3306, bolt: 7687, 'bolt+s': 7687}[url.protocol.slice(0, -1)] ?? 0));
  if (url.username || url.password || !scope.allowedHosts?.includes(url.hostname) ||
      !scope.allowedTargets?.some(target => target.host === url.hostname && target.ports.includes(port))) throw new ApiError(403, 'HOST_NOT_ALLOWED');
}
export function invoke(tool, args, input) {
  if (!tool || !path.isAbsolute(tool.command) || !['validate', 'validate-scope'].includes(args[0])) throw new ApiError(503, 'SOURCE_INSPECTION_UNAVAILABLE');
  const raw = JSON.stringify(input);
  if (Buffer.byteLength(raw) > 2097152) throw new ApiError(413, 'OWNER_INPUT_LIMIT');
  // Configuration checks do not need or receive provider credentials.
  const env = { PATH: process.env.PATH, LANG: 'C.UTF-8' };
  return new Promise((resolve, reject) => {
    const child = execFile(tool.command, [...(tool.args ?? []), ...args], {
      cwd: tool.cwd, env, timeout: 10000, maxBuffer: 2097152, killSignal: 'SIGKILL',
    }, (error, stdout, stderr) => {
      if (error) {
        let code = error.killed ? 'OWNER_TIMEOUT' : 'OWNER_UNAVAILABLE';
        try { const value = JSON.parse(stderr); if (/^[a-zA-Z0-9_.:-]{1,90}$/.test(value.errorCode)) code = value.errorCode; } catch {}
        reject(new ApiError(502, code)); return;
      }
      try { resolve(JSON.parse(stdout)); } catch { reject(new ApiError(502, 'OWNER_RESPONSE_INVALID')); }
    });
    child.stdin.on('error', () => {});
    child.stdin.end(raw);
  });
}
