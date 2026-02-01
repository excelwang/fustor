// src/fuagent/ui/js/utils.js

/**
 * A collection of shared helper functions for the UI.
 */

/**
 * Resolves a $ref path within a JSON schema object.
 * @param {string} refPath - The reference path (e.g., "#/components/schemas/PasswdCredential").
 * @param {object} schema - The full OpenAPI schema to resolve against.
 * @returns {object|null} The resolved schema object or null if not found.
 */
export function resolveRef(refPath, schema) {
    if (!refPath || !refPath.startsWith('#/') || !schema) {
        return null;
    }
    const parts = refPath.substring(2).split('/');
    let current = schema;
    try {
        for (const part of parts) {
            current = current[part];
        }
        return current;
    } catch (e) {
        console.error(`Could not resolve $ref: ${refPath}`, e);
        return null;
    }
}