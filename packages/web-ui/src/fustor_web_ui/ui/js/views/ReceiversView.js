// src/fuagent/ui/js/views/PushersView.js

import apiService from '../apiService.js';
import stateStore from '../stateStore.js';
import { navigate } from '../navigation.js';
import { createConfigListItem } from '../components/ConfigListItem.js';

export default class PushersView {
    constructor(elementId) {
        this.el = document.getElementById(elementId);
        this.unsubscribe = null;
    }

    onActivate() {
        this.el.innerHTML = `
            <div class="page-header">
                <h1 class="page-title">接收端</h1>
                <p class="text-muted mt-1">管理所有数据接收端点的连接信息和驱动配置。</p>
            </div>

             <div class="card">
                <div class="card-header d-flex justify-content-between align-items-center">
                    <h3 class="card-title mb-0">已配置的接收端</h3>
                    <div class="btn-list">
                        <button class="btn btn-primary" data-action="add-pusher">
                            <i class="ti ti-plus me-1"></i> 添加接收端
                        </button>
                        
                        <button class="btn btn-outline-danger" data-action="cleanup-obsolete-pushers">
                            <i class="ti ti-trash me-1"></i> 清理无用配置
                        </button>
                    </div>
                </div>
                <div id="pushers-list-container">
                    <div class="text-center p-5">
                        <div class="spinner-border loading-spinner" role="status"></div>
                    </div>
                </div>
             </div>
        `;
        
        this.addEventListeners();
        this.unsubscribe = stateStore.subscribe(() => this.render());
        this.render();
    }

    onDeactivate() {
        this.el.removeEventListener('click', this.boundHandleClick);
        if (this.unsubscribe) {
            this.unsubscribe();
        }
        this.el.innerHTML = '';
    }

    addEventListeners() {
        this.boundHandleClick = this.handleClick.bind(this);
        this.el.addEventListener('click', this.boundHandleClick);
    }

    handleClick(e) {
        const button = e.target.closest('button[data-action]');
        if (!button) return;

        // Disable the button to prevent duplicate clicks
        button.disabled = true;

        const action = button.dataset.action;
        const configId = button.dataset.id;

        switch (action) {
            case 'add-pusher':
                stateStore.setState({ wizardContext: { mode: 'add', type: 'pushers' } });
                navigate('wizard');
                // No need to re-enable, view is changing
                break;
            case 'edit':
                stateStore.setState({ wizardContext: { mode: 'edit', type: 'pushers', id: configId } });
                navigate('wizard');
                // No need to re-enable, view is changing
                break;
            case 'toggle-enabled':
                const isDisabling = button.dataset.enabled === 'true';
                if (isDisabling) {
                    apiService.disablePusherConfig(configId, button).finally(() => button.disabled = false);
                } else {
                    apiService.enablePusherConfig(configId, button).finally(() => button.disabled = false);
                }
                break;
            case 'delete':
                if (confirm(`确定要删除接收端 '${configId}' 吗？\n此操作不可恢复。`)) {
                    // Let apiService handle notifications. Just re-enable the button on completion.
                    apiService.deletePusherConfig(configId, button)
                        .catch(() => {}) // Error is already displayed by the global handler
                        .finally(() => button.disabled = false);
                } else {
                    button.disabled = false;
                }
                break;
            case 'start-all-syncs-for-pusher':
                apiService.startSyncsByPusher(null, button).finally(() => button.disabled = false);
                // No configId needed for all
                break;
            case 'stop-all-syncs-for-pusher':
                apiService.stopSyncsByPusher(null, button).finally(() => button.disabled = false);
                // No configId needed for all
                break;
            case 'cleanup-obsolete-pushers':
                if (confirm("确定要清理所有无用的接收端配置吗？\n此操作将删除所有已禁用且未被任何同步任务使用的配置。")) {
                    apiService.cleanupPusherConfigs(button).finally(() => button.disabled = false);
                } else {
                    button.disabled = false;
                }
                break;
            default:
                // Re-enable button if no action was matched
                button.disabled = false;
        }
    }

    async render() {
        const listContainer = this.el.querySelector('#pushers-list-container');
        const { appConfig } = stateStore.getState();

        // REFACTORED: Explicitly check if the config is still loading.
        if (typeof appConfig.pushers === 'undefined') {
            listContainer.innerHTML = `<div class="text-center p-5"><div class="spinner-border loading-spinner" role="status"></div></div>`;
            return; // Exit and wait for the state update that brings the config.
        }

        try {
            const configs = appConfig.pushers || {}; // Get pushers from stateStore
            const configEntries = Object.entries(configs);
            if (configEntries.length === 0) {
                listContainer.innerHTML = `<div class="text-center p-4 text-muted">没有已配置的接收端。</div>`;
            } else {
                const listHtml = configEntries.map(([id, config]) => {
                    const statusDotClass = config.disabled ? 'status-dot-secondary' : 'status-dot-success';
                    const statusTitle = config.disabled ? '已禁用' : '已启用';
                    
                    const actionsHtml = `
                        <button class="btn btn-sm ${config.disabled ? 'btn-outline-success' : 'btn-outline-secondary'}" data-action="toggle-enabled" data-id="${id}" data-enabled="${!config.disabled}" title="${config.disabled ? '启用' : '禁用'}">
                            <i class="ti ti-${config.disabled ? 'player-play' : 'player-pause'} me-1"></i>
                            ${config.disabled ? 'Enable' : 'Disable'}
                        </button>
                        <button class="btn btn-icon" data-action="edit" data-id="${id}" title="克隆并编辑">
                            <i class="ti ti-edit"></i>
                        </button>
                        <button class="btn btn-icon" data-action="delete" data-id="${id}" title="删除">
                            <i class="ti ti-trash"></i>
                        </button>
                    `;
                    return createConfigListItem({
                        id,
                        type: 'pusher',
                        statusDotClass,
                        statusTitle,
                        mainTitleHtml: `<strong class="text-body d-block text-truncate" title="${id}">${id}</strong>`,
                        subDetailsHtml: `<small class="d-block text-muted text-truncate mt-n1">驱动: code>${config.driver}</code> | Endpoint: <code>${config.endpoint}</code></small>`,
                        actionsHtml
                    });
                }).join('');
                const listWrapper = document.createElement('div');
                listWrapper.className = 'list-group list-group-flush';
                listWrapper.innerHTML = listHtml;
                listContainer.innerHTML = ''; // Clear previous content
                listContainer.appendChild(listWrapper);
            }

        } catch (error) {
            listContainer.innerHTML = `<div class="alert alert-danger m-3">加载数据源配置失败: ${error.message}</div>`;
        }
    }
}