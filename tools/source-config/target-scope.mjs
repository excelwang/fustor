import net from 'node:net';
import { ApiError } from './support.mjs';

// A scope label carries no authority; only explicit installed endpoints do.
export function validateTargetScope(scope) {
  if (!scope || typeof scope.targetRef !== 'string' || !/^[a-zA-Z0-9_-]{1,80}$/.test(scope.targetRef) ||
      !Array.isArray(scope.allowedTargets) || !scope.allowedTargets.length || scope.allowedTargets.length > 20 ||
      scope.allowedTargets.some(target => net.isIP(target?.host) !== 4 || !Array.isArray(target.ports) ||
        !target.ports.length || target.ports.length > 20 || target.ports.some(port => !Number.isSafeInteger(port) || port < 1 || port > 65535))) {
    throw new ApiError(403, 'INSTALLED_TARGET_SCOPE_INVALID');
  }
  return scope;
}
export function scopedEndpoint(scope, host, port) {
  if (!scope) return false;
  return validateTargetScope(scope).allowedTargets.some(target => target.host === host && target.ports.includes(port));
}
