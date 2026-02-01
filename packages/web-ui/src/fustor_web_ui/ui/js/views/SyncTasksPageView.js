// src/fuagent/ui/js/views/SyncTasksPageView.js

import apiService from '../apiService.js';
import stateStore from '../stateStore.js';
import { navigate } from '../navigation.js';

export default class SyncTasksPageView {
    constructor(elementId) {
        console.log(`[VIEW CONSTRUCTOR] SyncTasksPageView for element: ${elementId}`);
        this.el = document.getElementById(elementId);
        this.unsubscribe = null;
        this.pollInterval = null;
    }

    onActivate() {
        console.log('[VIEW ACTIVATE] SyncTasksPageView activated.');
        this.el.innerHTML = `
            <div class="page-header">
                <h1 class="page-title">同步任务</h1>
                <p class="text-muted mt-1">统一管理同步任务的配置与运行时实例。</p>
            </div>

            <div class="card">
                <div class="card-header d-flex justify-content-between align-items-center">
                    <h3 class="card-title mb-0">所有同步任务</h3>
                    <div class="btn-list">
                        <button class="btn btn-primary" data-action="add-sync-task">
                            <i class="ti ti-plus me-1"></i> 新建同步任务
                        </button>
                    </div>
                </div>
                <div class="table-responsive">
                     <table class="table table-vcenter card-table table-striped">
                        <thead>
                            <tr>
                                <th>状态</th>
                                <th>同步任务ID</th>
                                <th>Source → Pusher</th>
                                <th>已推送 / 缓冲区</th>
                                <th class="w-1 text-end">操作</th>
                            </tr>
                        </thead>
                        <tbody id="sync-tasks-table-body">
                        </tbody>
                    </table>
                </div>
             </div>
        `;
        this.addEventListeners();
        this.fetchDataAndRender(); // Fetch data and render immediately
        if (this.pollInterval) clearInterval(this.pollInterval); // Clear any existing interval
        this.pollInterval = setInterval(() => this.fetchDataAndRender(), 5000); // Start polling
    }

    onDeactivate() {
        this.el.removeEventListener('click', this.boundHandleClick);
        if (this.pollInterval) { // Clear polling interval
            clearInterval(this.pollInterval);
            this.pollInterval = null;
        }
        this.el.innerHTML = '';
    }

    addEventListeners() {
        this.boundHandleClick = this.handleClick.bind(this);
        this.el.addEventListener('click', this.boundHandleClick);
    }

    async fetchDataAndRender() {
        try {
            const appConfig = await apiService.getConfig();
            const instancesStatus = await apiService.getInstancesStatus();
            stateStore.setState({ appConfig, instancesStatus }); // Update global state
            this.render(); // Re-render the view after state update
        } catch (error) {
            console.error('SyncTasksPageView: Failed to fetch data:', error);
            const tableBody = this.el.querySelector('#sync-tasks-table-body');
            if (tableBody) {
                tableBody.innerHTML = `<td colspan="5" class="alert alert-danger m-3">加载同步任务失败: ${error.message}</td>`;
            }
        }
    }

    handleClick(e) {
        // --- NEW DEBUG LOG ---
        console.log('[EVENT HANDLER] Click event captured on SyncTasksPageView.', e.target);
        const button = e.target.closest('button[data-action]');
        if (!button) return;

        e.preventDefault();
        const action = button.dataset.action;
        const configId = button.dataset.id;
        // Disable button on click to prevent duplicate actions
        button.disabled = true;
        switch (action) {
            case 'add-sync-task':
                stateStore.setState({ wizardContext: { mode: 'add', type: 'syncs' } });
                navigate('sync-wizard');
                // Button will be gone on navigation, no need to re-enable
                break;
            case 'edit':
                stateStore.setState({ wizardContext: { mode: 'edit', type: 'syncs', id: configId } });
                navigate('sync-wizard');
                break;
            case 'delete':
                if (confirm(`确定要删除同步任务 '${configId}' 吗?\n此操作不可恢复。`)) {
                    apiService.deleteSyncConfig(configId)
                        .catch(error => {
                            if (error.message.includes('used by the following sync tasks')) {
                                const banner = document.getElementById('global-alert-banner');
                                const bannerMessage = banner.querySelector('span');
                                bannerMessage.textContent = error.message;
                                banner.classList.remove('d-none', 'alert-warning');
                                banner.classList.add('alert-danger');
                                const applyBtn = banner.querySelector('#apply-changes-btn');
                                if(applyBtn) applyBtn.classList.add('d-none');
                            }
                        })
                         .finally(() => button.disabled = false);
                } else {
                    button.disabled = false;
                }
                break;
            case 'start':
                apiService.startSyncInstance(configId).finally(() => button.disabled = false);
                break;
            case 'stop':
                apiService.stopSyncInstance(configId).finally(() => button.disabled = false);
                break;
            case 'restart':
                apiService.stopSyncInstance(configId).then(() => {
                    // Add a small delay to allow graceful stop before starting
                    setTimeout(() => apiService.startSyncInstance(configId), 1000);
                }).finally(() => button.disabled = false);
                break;
            // NEW: Handle enable/disable actions
            case 'toggle-enabled':
                const isDisabling = button.dataset.enabled === 'true';
                if (isDisabling) {
                    apiService.disableSyncConfig(configId, button)
                        .then(() => { /* Handled by apiService */ })
                        .catch(() => { /* Handled by apiService */ })
                        .finally(() => button.disabled = false);
                } else {
                    apiService.enableSyncConfig(configId, button)
                        .then(() => { /* Handled by apiService */ })
                        .catch(() => { /* Handled by apiService */ })
                        .finally(() => button.disabled = false);
                }
                break;
            default:
                button.disabled = false;
        }
    }

    async render() {
        const tableBody = this.el.querySelector('#sync-tasks-table-body');
        if (!tableBody) return;
        
        const { appConfig, instancesStatus } = stateStore.getState();

        // REFACTORED: Explicitly check if the config is still loading.
        // This check is now less critical as fetchDataAndRender ensures data is present.
        // Keeping it for initial load robustness.
        if (typeof appConfig.syncs === 'undefined' || typeof instancesStatus.pipelines === 'undefined') {
            tableBody.innerHTML = `
                <tr>
                    <td colspan="5" class="text-center p-5">
                        <div class="spinner-border" role="status"></div>
                    </td>
                </tr>`;
            return; // Exit and wait for the state update that brings the config.
        }

        try {
            const allSyncConfigs = appConfig.syncs || {};
            const runningInstances = instancesStatus.pipelines || [];

            // Create a map for quick lookup of running instances by ID
            const runningInstancesMap = new Map(runningInstances.map(inst => [inst.id, inst]));
            const pipelinesToRender = [];

            // Iterate through all configured syncs
            for (const syncId in allSyncConfigs) {
                const syncConfig = allSyncConfigs[syncId];
                const runningInstance = runningInstancesMap.get(syncId);

                if (runningInstance) {
                    // This is a running/managed instance
                    pipelinesToRender.push(runningInstance);
                } else {
                    // This is a configured but not running task
                    const isDisabled = syncConfig.disabled;
                    const statusText = isDisabled ? "任务已禁用" : "任务已停止";
                    pipelinesToRender.push({
                        id: syncId,
                        overall_status: "STOPPED",
                        source_id: syncConfig.source,
                        pusher_id: syncConfig.pusher,
                        bus_info: null,
                        sync_info: {
                            id: syncId,
                            state: "STOPPED",
                            info: statusText,
                            statistics: { events_pushed: 0, last_pushed_event_id: null },
                            bus_info: null,
                        },
                        is_disabled: isDisabled
                    });
                }
            }

            if (pipelinesToRender.length === 0) {
                tableBody.innerHTML = `<td colspan="5" class="text-center p-4 text-muted">没有已配置的同步任务。</td>`;
            } else {
                const rowsHtml = pipelinesToRender.map(p => this._createTableRowHtml(p)).join('');
                tableBody.innerHTML = rowsHtml;
            }
        } catch (error) {
            tableBody.innerHTML = `<td colspan="5" class="alert alert-danger m-3">加载同步任务失败: ${error.message}</td>`;
        }
    }
    
    _getStatusInfo(state) {
        const statuses = {
            'ERROR': { text: '错误', dot: 'danger' },
            'RUNNING_CONF_OUTDATE': { text: '配置过时', dot: 'warning' },
            'SNAPSHOT_SYNC': { text: '快照同步中', dot: 'success' },
            'MESSAGE_SYNC': { text: '消息同步中', dot: 'success' },
            'STOPPING': { text: '停止中', dot: 'secondary' },
            'STOPPED': { text: '已停止', dot: 'secondary' }
        };
        return { ...(statuses[state] || { text: state, dot: 'secondary' }) };
    }

    _createTableRowHtml(pipeline) {
        const RUNNING_STATES = ['SNAPSHOT_SYNC', 'MESSAGE_SYNC', 'RUNNING_CONF_OUTDATE'];
        const status = pipeline.overall_status;
        const statusInfo = this._getStatusInfo(status);
        const syncInfo = pipeline.sync_info;
        
        const isDisabled = pipeline.is_disabled === true;
        if (isDisabled && status === 'STOPPED') {
            statusInfo.text = '已禁用';
        }

        const isRunning = RUNNING_STATES.includes(status);
        const isStopping = status === 'STOPPING';
        const canBeDeleted = !isRunning && !isStopping;
        // REFACTORED: Generate dynamic primary action and enable/disable buttons
        let primaryActionBtn = '';
        if (isRunning) {
            primaryActionBtn = `
                <button class="btn btn-icon" data-action="stop" data-id="${pipeline.id}" title="停止" ${isStopping ? 'disabled' : ''}>
                    <i class="ti ti-player-stop"></i>
                </button>`;
        } else { // State is STOPPED or ERROR
            primaryActionBtn = `
                <button class="btn btn-icon" data-action="start" data-id="${pipeline.id}" title="启动" ${isStopping || isDisabled ? 'disabled' : ''}>
                    <i class="ti ti-player-play"></i>
                </button>`;
        }

        const enableDisableBtn = `
            <button class="btn btn-sm ${isDisabled ? 'btn-outline-success' : 'btn-outline-secondary'}" data-action="toggle-enabled" data-id="${pipeline.id}" data-enabled="${!isDisabled}" title="${isDisabled ? '启用配置' : '禁用配置'}" ${isRunning ? 'disabled' : ''}>
                <i class="ti ti-${isDisabled ? 'player-play' : 'player-pause'} me-1"></i>
                ${isDisabled ? '启用' : '禁用'}
            </button>`;
        return `
            <tr data-id="${pipeline.id}">
                <td><span class="status-dot status-dot-${statusInfo.dot}" title="${statusInfo.text}"></span> ${statusInfo.text}</td>
                <td>
                    <div class="text-truncate" title="${pipeline.id}"><strong>${pipeline.id}</strong></div>
                    ${status === 'ERROR' ? `<div class="text-danger small text-truncate" title="${syncInfo?.info}">${syncInfo?.info}</div>` : ''}
                </td>
                <td>
                    <div class="d-flex align-items-center">
                        <code title="Source: ${pipeline.source_id}">${pipeline.source_id}</code>
                        <i class="ti ti-arrow-right mx-2"></i>
                        <code title="Pusher: ${pipeline.pusher_id}">${pipeline.pusher_id}</code>
                    </div>
                </td>
                <td>
                    <span title="已推送事件">${syncInfo?.statistics.events_pushed || 0}</span> / 
                    <span title="缓冲区大小">${pipeline.bus_info?.statistics.buffer_size || 0}</span>
                </td>
                <td>
                    <div class="btn-list flex-nowrap justify-content-end">
                        ${primaryActionBtn}
                        <button class="btn btn-icon" data-action="restart" data-id="${pipeline.id}" title="重启" ${!isRunning || isDisabled ? 'disabled' : ''}>
                            <i class="ti ti-refresh"></i>
                        </button>
                        ${enableDisableBtn}
                        <button class="btn btn-icon" data-action="edit" data-id="${pipeline.id}" title="克隆并编辑">
                            <i class="ti ti-edit"></i>
                        </button>
                        <button class="btn btn-icon" data-action="delete" data-id="${pipeline.id}" title="删除" ${!canBeDeleted ? 'disabled' : ''}>
                            <i class="ti ti-trash"></i>
                        </button>
                    </div>
                </td>
            </tr>
        `;
    }
}