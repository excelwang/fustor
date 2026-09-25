#!/usr/bin/env node
import fs from 'node:fs/promises';
import path from 'node:path';
import { SourceConfigurationManager, SourceConfigurationError } from './index.mjs';

try {
  const [action, flag, file] = process.argv.slice(2);
  if (!['describe', 'apply', 'resolve'].includes(action) || flag !== '--config' || !file || process.argv.length !== 5) throw new Error('SOURCE_CONFIG_ARGUMENT_INVALID');
  const stat = await fs.lstat(file);
  if (!path.isAbsolute(file) || await fs.realpath(file) !== file || !stat.isFile() || stat.size > 65536 ||
      stat.uid !== process.getuid() || stat.mode & 0o077) throw new Error('SOURCE_CONFIG_NOT_PRIVATE');
  const manager = await SourceConfigurationManager.open(JSON.parse(await fs.readFile(file, 'utf8')));
  let result;
  if (action === 'apply') {
    const chunks = []; let size = 0;
    for await (const chunk of process.stdin) { size += chunk.length; if (size > 65536) throw new Error('SOURCE_INPUT_LIMIT'); chunks.push(chunk); }
    result = await manager.apply(JSON.parse(Buffer.concat(chunks).toString('utf8')));
  } else result = await manager[action]();
  process.stdout.write(JSON.stringify(result) + '\n');
} catch (error) {
  process.stderr.write(JSON.stringify({ errorCode: error instanceof SourceConfigurationError ? error.code : 'SOURCE_CONFIG_UNAVAILABLE' }) + '\n');
  process.exitCode = 1;
}
