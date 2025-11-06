// src/fuagent/ui/js/apiService.js

import { showBusinessError, showSuccess, showToast } from './notification.js';
import stateStore from './stateStore.js';

// --- Private Helper Functions ---
async function apiFetch(url, options = {}, showSuccessNotification = false, targetElement = null) {
    console.log(`[API CALL] ==> URL: ${url}`, `OPTIONS: ${JSON.stringify(options)}`);
    try {
        const response = await fetch(url, options);
        if (!response.ok) {
            let errorMsg = `HTTP error ${response.status}: ${response.statusText}`;
            try {
                const errorData = await response.json();
                if (errorData.detail) {
                    if (typeof errorData.detail === 'string') {
                        errorMsg = errorData.detail;
                    } else if (Array.isArray(errorData.detail)) {
                        errorMsg = errorData.detail.map(err => err.msg).join(', ');
                    } else {
                        errorMsg = JSON.stringify(errorData.detail);
                    }
                }
            } catch (e) {
                // Stick with the default HTTP error message.
            }
            throw new Error(errorMsg);
        }

        if (response.status === 204) {
            if (showSuccessNotification) {
                if (targetElement) {
                    showToast(targetElement, '操作成功。');
                } else {
                    showSuccess('操作成功。');
                }
            }
            return null;
        }

        const data = await response.json();
        if (showSuccessNotification) {
            const message = data.message || '操作成功。';
            if (targetElement) {
                showToast(targetElement, message);
            } else {
                showSuccess(message);
            }
        }
        return data;
    } catch (error) {
        const method = options.method || 'GET';
        let paramStr = '';
        if (options.body) {
            try {
                paramStr = ' ' + JSON.stringify(JSON.parse(options.body));
            } catch {
                paramStr = ' ' + String(options.body);
            }
        }
        let respStr = '';
        if (error && error.responseText) {
            respStr = ' 响应:' + error.responseText;
        } else if (error && error.response) {
            try {
                respStr = ' 响应:' + JSON.stringify(error.response);
            } catch {
                respStr = ' 响应:' + String(error.response);
            }
        }
        const notifyMsg = `操作失败: ${error.message} [${method} ${url}]${paramStr}${respStr}`;
        showBusinessError(notifyMsg);
        throw error;
    }
}

async function apiFetchAndRefresh(url, options = {}, showSuccessNotification = false, targetElement = null) {
    const result = await apiFetch(url, options, showSuccessNotification, targetElement);
    await refreshGlobalState();
    return result;
}

async function refreshGlobalState() {
    try {
        const appConfig = await apiService.getConfig();
        const instancesStatus = await apiService.getInstancesStatus();
        stateStore.setState({ appConfig, instancesStatus });
        console.log('Global state refreshed after API call.');
    } catch (error) {
        console.error('Failed to refresh global state:', error);
    }
}

// --- Public API Service ---
const apiService = {
    get: (url, showSuccessNotification = false, targetElement = null) => apiFetch(url, { method: 'GET' }, showSuccessNotification, targetElement),
    post: (url, body, showSuccessNotification = false, targetElement = null) => apiFetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }, showSuccessNotification, targetElement),
    put: (url, body, showSuccessNotification = false, targetElement = null) => apiFetch(url, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }, showSuccessNotification, targetElement),
    delete: (url, showSuccessNotification = false, targetElement = null) => apiFetch(url, { method: 'DELETE' }, showSuccessNotification, targetElement),
    getOpenApiSchema: () => apiFetch('/openapi.json'),
    getConfig: () => apiFetch('/api/configs/'), 
    listAvailableDrivers: () => apiFetch('/api/drivers'),
    getSourceWizardDefinition: (driverType) => apiFetch(`/api/drivers/sources/${driverType}/wizard`),
    getPusherWizardDefinition: (driverType) => apiFetch(`/api/drivers/pushers/${driverType}/wizard`),
    addSourceConfig: (id, config, discoveredFields = null, targetElement = null) => {
        const payload = { 
            config: config,
            discovered_fields: discoveredFields 
        };
        return apiFetchAndRefresh(`/api/configs/sources/${id}`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) }, true, targetElement);
    },
    deleteSourceConfig: (id, targetElement = null) => apiFetchAndRefresh(`/api/configs/sources/${id}`, { method: 'DELETE' }, true, targetElement),
    disableSourceConfig: (id, targetElement = null) => apiFetchAndRefresh(`/api/configs/sources/${id}/_actions/disable`, { method: 'POST' }, true, targetElement),
    enableSourceConfig: (id, targetElement = null) => apiFetchAndRefresh(`/api/configs/sources/${id}/_actions/enable`, { method: 'POST' }, true, targetElement),
    cleanupSourceConfigs: (targetElement = null) => apiFetchAndRefresh('/api/configs/sources/_actions/cleanup', { method: 'POST' }, true, targetElement),
    addPusherConfig: (id, config, targetElement = null) => {
        const payload = { config: config };
        return apiFetchAndRefresh(`/api/configs/pushers/${id}`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) }, true, targetElement);
    },
    deletePusherConfig: (id, targetElement = null) => apiFetchAndRefresh(`/api/configs/pushers/${id}`, { method: 'DELETE' }, true, targetElement),
    disablePusherConfig: (id, targetElement = null) => apiFetchAndRefresh(`/api/configs/pushers/${id}/_actions/disable`, { method: 'POST' }, true, targetElement),
    enablePusherConfig: (id, targetElement = null) => apiFetchAndRefresh(`/api/configs/pushers/${id}/_actions/enable`, { method: 'POST' }, true, targetElement),
    cleanupPusherConfigs: (targetElement = null) => apiFetchAndRefresh('/api/configs/pushers/_actions/cleanup', { method: 'POST' }, true, targetElement),
    addSyncConfig: (id, config, targetElement = null) => {
        return apiFetchAndRefresh(`/api/configs/syncs/${id}`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(config) }, true, targetElement);
    },
    deleteSyncConfig: (id, targetElement = null) => apiFetchAndRefresh(`/api/configs/syncs/${id}`, { method: 'DELETE' }, true, targetElement),
    disableSyncConfig: (id, targetElement = null) => apiFetchAndRefresh(`/api/configs/syncs/${id}/_actions/disable`, { method: 'POST' }, true, targetElement),
    enableSyncConfig: (id, targetElement = null) => apiFetchAndRefresh(`/api/configs/syncs/${id}/_actions/enable`, { method: 'POST' }, true, targetElement),
    discoverAndCacheSourceFields: (sourceId, adminCreds, targetElement = null) => apiFetch(`/api/drivers/sources/${sourceId}/_actions/discover_and_cache_fields`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ admin_creds: adminCreds }) }, false, targetElement),
    getSourceAvailableFields: (sourceId) => apiFetch(`/api/drivers/sources/${sourceId}/_actions/get_available_fields`),
    getPusherNeededFields: (pusherId) => apiFetch(`/api/drivers/pushers/${pusherId}/_actions/get_needed_fields`),
    testSourceConnection: (driverType, uri, adminCreds, targetElement = null) => apiFetch(`/api/drivers/sources/${driverType}/_actions/test_connection`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ uri, admin_creds: adminCreds }) }, false, targetElement),
    checkSourceParams: (driverType, uri, adminCreds, targetElement = null) => apiFetch(`/api/drivers/sources/${driverType}/_actions/check_params`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ uri, admin_creds: adminCreds }) }, false, targetElement),
    testPusherConnectionByDriver: (driverType, endpoint, credential, targetElement = null) => apiFetch(`/api/drivers/pushers/${driverType}/_actions/test_connection`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ endpoint, credential }) }, false, targetElement),
    checkPusherPrivilegesByDriver: (driverType, endpoint, credential, targetElement = null) => apiFetch(`/api/drivers/pushers/${driverType}/_actions/check_privileges`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ endpoint, credential }) }, false, targetElement),
    getInstancesStatus: () => apiFetch('/api/instances/status'),
    applyChanges: (targetElement = null) => apiFetchAndRefresh('/api/instances/_actions/apply_all_pending_changes', { method: 'POST' }, true, targetElement),
    startSyncInstance: (id, targetElement = null) => apiFetchAndRefresh(`/api/instances/syncs/${id}/_actions/start`, { method: 'POST' }, true, targetElement),
    stopSyncInstance: (id, targetElement = null) => apiFetchAndRefresh(`/api/instances/syncs/${id}/_actions/stop`, { method: 'POST' }, true, targetElement),
    getLog: (params) => {
        const filteredParams = {};
        for (const key in params) {
            if (params[key] !== null && typeof params[key] !== 'undefined') {
                filteredParams[key] = params[key];
            }
        }
        const urlParams = new URLSearchParams(filteredParams).toString();
        return apiFetch(`/api/logs?${urlParams}`);
    },
    checkConfigIdExists: (type, id) => {
        const state = stateStore.getState();
        if (!state.appConfig) {
            console.error("AppConfig not found in stateStore. Cannot perform ID check.");
            return false;
        }
        let configs;
        if (type === 'sources') {
            configs = state.appConfig.sources?.root || {};
        } else if (type === 'pushers') {
            configs = state.appConfig.pushers?.root || {};
        } else if (type === 'syncs') {
            configs = state.appConfig.syncs?.root || {};
        } else {
            throw new Error('Invalid config type for ID check.');
        }
        return id in configs;
    },
};

export default apiService;
