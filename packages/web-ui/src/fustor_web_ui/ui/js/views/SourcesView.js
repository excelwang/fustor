import apiService from '../apiService.js';
import stateStore from '../stateStore.js';
import { navigate } from '../navigation.js';
import { createConfigListItem } from '../components/ConfigListItem.js';

export default class SourcesView {
    constructor(elementId) {
        this.el = document.getElementById(elementId);
        this.unsubscribe = null;
    }

    onActivate() {
        this.el.innerHTML = `
            <div class="page-header">
                <h1 class="page-title">数据源</h1>
                <p class="text-muted mt-1">管理所有数据来源的连接信息和驱动配置。</p>
            </div>

             <div class="card">
                <div class="card-header d-flex justify-content-between align-items-center">
                    <h3 class="card-title mb-0">已配置的数据源</h3>
                    <div class="btn-list">
                        <button class="btn btn-primary" data-action="add-source">
                            <i class="ti ti-plus me-1"></i> 添加 Source
                        </button>
                        
                        <button class="btn btn-outline-danger" data-action="cleanup-obsolete-sources">
                            <i class="ti ti-trash me-1"></i> 清理无用配置
                        </button>
                    </div>
                </div>
                
                <div id="sources-list-container">
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
        const elementWithAction = e.target.closest('[data-action]');
        if (!elementWithAction) return;

        // Disable the button to prevent duplicate clicks
        elementWithAction.disabled = true;

        const action = elementWithAction.dataset.action;
        const configId = elementWithAction.dataset.id;
        const { appConfig } = stateStore.getState();
        const sourceUsageMap = this.buildSourceUsageMap(appConfig.syncs?.root || {});
        const usages = sourceUsageMap[configId] || [];

        switch (action) {
            case 'add-source':
                stateStore.setState({ wizardContext: { mode: 'add', type: 'sources' } });
                navigate('wizard');
                // No need to re-enable, view is changing
                break;
            case 'edit':
                stateStore.setState({ wizardContext: { mode: 'edit', type: 'sources', id: configId } });
                navigate('wizard');
                // No need to re-enable, view is changing
                break;
            case 'toggle-enabled':
                const isDisabling = elementWithAction.dataset.enabled === 'true';
                if (isDisabling && usages.length > 0) {
                    if (!confirm(`警告：此数据源正在被 ${usages.length} 个同步任务使用：\n\n- ${usages.join('\n- ')}\n\n禁用此数据源将导致这些任务停止运行。确定要继续吗？`)) {
                        elementWithAction.disabled = false;
                        return;
                    }
                }
                if (isDisabling) {
                    apiService.disableSourceConfig(configId, elementWithAction).finally(() => elementWithAction.disabled = false);
                } else {
                    apiService.enableSourceConfig(configId, elementWithAction).finally(() => elementWithAction.disabled = false);
                }
                break;
            case 'delete':
                let confirmMessage = `确定要删除数据源 '${configId}' 吗?\n此操作不可恢复。`;
                if (usages.length > 0) {
                    confirmMessage = `警告：此数据源正在被 ${usages.length} 个同步任务使用：\n\n- ${usages.join('\n- ')}\n\n删除此数据源将导致这些任务失败。确定要继续吗？`;
                }
                if (confirm(confirmMessage)) {
                    // Let apiService handle notifications. Just re-enable the button on completion.
                    apiService.deleteSourceConfig(configId, elementWithAction)
                        .catch(() => {}) // Error is already displayed by the global handler
                        .finally(() => elementWithAction.disabled = false);
                } else {
                    elementWithAction.disabled = false;
                }
                break;
            case 'cleanup-obsolete-sources':
                if (confirm("确定要清理所有无用的数据源配置吗？\n此操作将删除所有已禁用且未被任何同步任务使用的配置。")) {
                    apiService.cleanupSourceConfigs(elementWithAction).finally(() => elementWithAction.disabled = false);
                } else {
                    elementWithAction.disabled = false;
                }
                break;
            case 'discover-schema':
                const adminUser = prompt("请输入有权发现schema的数据库用户名:", "root");
                if (!adminUser) {
                    elementWithAction.disabled = false;
                    return;
                }
                const adminPassword = prompt("请输入该用户的密码:");

                const adminCreds = { user: adminUser, passwd: adminPassword };
                apiService.discoverAndCacheSourceFields(configId, adminCreds, elementWithAction)
                    .finally(() => elementWithAction.disabled = false);
                break;
            default:
                // Re-enable button if no action was matched
                elementWithAction.disabled = false;
        }
    }

    buildSourceUsageMap(syncConfigs) {
        const usageMap = {};
        for (const syncId in syncConfigs) {
            const sourceId = syncConfigs[syncId].source;
            if (!usageMap[sourceId]) {
                usageMap[sourceId] = [];
            }
            usageMap[sourceId].push(syncId);
        }
        return usageMap;
    }

    async render() {
        console.log('[SourcesView] Render triggered.');
        // DEBUG
        const listContainer = this.el.querySelector('#sources-list-container');
        const { appConfig } = stateStore.getState();
        console.log('[SourcesView] appConfig from stateStore:', JSON.stringify(appConfig, null, 2)); // DEBUG

        if (typeof appConfig.sources === 'undefined' || typeof appConfig.syncs === 'undefined') {
            listContainer.innerHTML = `<div class="text-center p-5"><div class="spinner-border loading-spinner" role="status"></div></div>`;
            return;
        }
        
        try {
            const sourceConfigs = appConfig.sources || {};
            console.log('[SourcesView] Parsed sourceConfigs:', JSON.stringify(sourceConfigs, null, 2)); // DEBUG
            const syncConfigs = appConfig.syncs || {};
            const sourceUsageMap = this.buildSourceUsageMap(syncConfigs);
            const configEntries = Object.entries(sourceConfigs);

            if (configEntries.length === 0) {
                listContainer.innerHTML = `<div class="text-center p-4 text-muted">没有已配置的数据源。</div>`;
            } else {
                const listHtml = configEntries.map(([id, config]) => {
                    // Handle invalid configuration entries
                    if (config.validation_error) {
                        return createConfigListItem({
                            id,
                            type: 'source',
                            statusDotClass: 'status-dot-danger',
                            statusTitle: '配置无效',
                            mainTitleHtml: `<strong class="text-body d-block text-truncate" title="${id}">${id}</strong>`,
                            subDetailsHtml: `<small class="d-block text-danger text-truncate mt-n1" title="${config.validation_error}">错误: ${config.validation_error}</small>`,
                            actionsHtml: `
                                <button class="btn btn-icon" data-action="delete" data-id="${id}" title="删除">
                                    <i class="ti ti-trash"></i>
                                </button>
                            `
                        });
                    }

                    let statusDotClass = config.disabled ? 'status-dot-secondary' : 'status-dot-success';
                    let statusTitle = config.disabled ? '已禁用' : '已启用';
                    let schemaWarningHtml = '';
                    const usages = sourceUsageMap[id] || [];
                    let usageHtml = '';

                    if (!config.disabled && config.schema_cached === false) {
                        statusDotClass = 'status-dot-warning';
                        statusTitle = 'Schema 未缓存';
                        schemaWarningHtml = `<i class="ti ti-alert-triangle text-warning ms-2" title="Schema 未缓存，请前往此配置的编辑页面，执行“校验并发现”来生成字段缓存。"></i>`;
                    }

                    if (usages.length > 0) {
                        usageHtml = `
                            <span class="badge bg-blue-lt ms-2" data-bs-toggle="tooltip" data-bs-placement="top" title="被以下同步任务使用:\n- ${usages.join('\n- ')}">
                                <i class="ti ti-link me-1"></i>
                                ${usages.length}个任务正在使用
                            </span>`;
                    }
                    
                    const actionsHtml = `
                        <button class="btn btn-sm ${config.disabled ? 'btn-outline-success' : 'btn-outline-secondary'}" data-action="toggle-enabled" data-id="${id}" data-enabled="${!config.disabled}" title="${config.disabled ? '启用' : '禁用'}">
                            <i class="ti ti-${config.disabled ? 'player-play' : 'player-pause'}" me-1></i>
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
                        type: 'source',
                        statusDotClass,
                        statusTitle,
                        mainTitleHtml: `<strong class="text-body d-block text-truncate" title="${id}">${id}</strong>${schemaWarningHtml}${usageHtml}`,
                        subDetailsHtml: `<small class="d-block text-muted text-truncate mt-n1">驱动: <code>${config.driver}</code> | URI: <code>${config.uri}</code></small>`,
                        actionsHtml
                    });
                }).join('');
                const listWrapper = document.createElement('div');
                listWrapper.className = 'list-group list-group-flush';
                listWrapper.innerHTML = listHtml;
                listContainer.innerHTML = ''; // Clear previous content
                listContainer.appendChild(listWrapper);
                // Re-initialize tooltips
                const tooltipTriggerList = [].slice.call(listContainer.querySelectorAll('[data-bs-toggle="tooltip"]'));
                tooltipTriggerList.map(function (tooltipTriggerEl) {
                    return new bootstrap.Tooltip(tooltipTriggerEl, {
                        boundary: document.body,
                        html: true
                    });
                });
            }

        } catch (error) {
            listContainer.innerHTML = `<div class="alert alert-danger m-3">加载数据源配置失败: ${error.message}</div>`;
        }
    }
}