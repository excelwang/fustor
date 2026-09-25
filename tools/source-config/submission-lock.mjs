import fs from 'node:fs/promises';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { ApiError } from './support.mjs';

// flock belongs to the inherited open-file description, not the short helper.
// The parent descriptor retains it after helper exit and releases it on exit.
const LOCK = `import fcntl
try:
    fcntl.flock(3, fcntl.LOCK_EX | fcntl.LOCK_NB)
except BlockingIOError:
    raise SystemExit(73)
print('locked', flush=True)
`;

async function nativeLock(descriptor) {
  await new Promise((resolve, reject) => {
    const child = spawn('/usr/bin/python3', ['-I', '-B', '-c', LOCK], {
      env: { PATH: '/usr/bin:/bin', LANG: 'C.UTF-8' }, stdio: ['ignore', 'pipe', 'pipe', descriptor],
    });
    let stdout = '', bytes = 0, failed = false;
    const stop = () => { failed = true; child.kill('SIGKILL'); };
    const timer = setTimeout(stop, 5000);
    child.on('error', () => { failed = true; });
    child.stdout.on('data', chunk => { bytes += chunk.length; if (bytes > 4096) stop(); else stdout += chunk; });
    child.stderr.on('data', chunk => { bytes += chunk.length; if (bytes > 4096) stop(); });
    child.once('close', code => {
      clearTimeout(timer);
      if (!failed && code === 0 && stdout === 'locked\n') resolve();
      else reject(new ApiError(code === 73 && !failed ? 409 : 503,
        code === 73 && !failed ? 'EXECUTION_SUBMISSION_BUSY' : 'SUBMISSION_LOCK_UNAVAILABLE'));
    });
  });
}

export async function acquireSubmissionLock(root) {
  try { await fs.lstat(path.join(root, '.submission.lock')); throw new ApiError(409, 'LEGACY_SUBMISSION_LOCK_REVIEW_REQUIRED'); }
  catch (error) { if (error.code !== 'ENOENT') throw error; }
  const directory = await fs.lstat(root);
  if (await fs.realpath(root) !== root || !directory.isDirectory() || directory.uid !== process.getuid() || directory.mode & 0o077) throw new ApiError(403, 'SUBMISSION_ROOT_NOT_PRIVATE');
  const file = path.join(root, '.submission-v2.lock');
  let handle;
  try {
    handle = await fs.open(file, fs.constants.O_RDWR | fs.constants.O_CREAT | fs.constants.O_NOFOLLOW, 0o600);
    const original = await handle.stat();
    if (!original.isFile() || original.nlink !== 1 || original.uid !== process.getuid() || original.mode & 0o077 || original.size !== 0) throw new ApiError(403, 'SUBMISSION_LOCK_IDENTITY_INVALID');
    async function assertOwned() {
      const current = await fs.lstat(file), parent = await fs.lstat(root);
      if (await fs.realpath(root) !== root || current.dev !== original.dev || current.ino !== original.ino || current.mode !== original.mode || current.nlink !== 1 || current.size !== 0 ||
          parent.dev !== directory.dev || parent.ino !== directory.ino || parent.uid !== directory.uid || parent.mode !== directory.mode) throw new ApiError(409, 'SUBMISSION_LOCK_IDENTITY_CHANGED');
    }
    await assertOwned();
    await nativeLock(handle.fd);
    await assertOwned();
    return { assertOwned, close: () => handle.close() };
  } catch (error) {
    await handle?.close();
    if (error instanceof ApiError) throw error;
    throw new ApiError(503, 'SUBMISSION_LOCK_UNAVAILABLE');
  }
}
