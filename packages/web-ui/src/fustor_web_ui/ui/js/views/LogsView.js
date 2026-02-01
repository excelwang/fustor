// src/fuagent/ui/js/views/LogsView.js

import apiService from '../apiService.js';

export default class LogsView {
    constructor(elementId) {
        this.el = document.getElementById(elementId);
        this.isLoadingMore = false;
        this.hasMoreLogs = true;
        this.currentFilters = {
            limit: 200, 
            level: null,
            component: null,
            before_line: null
        };
        this.logOutput = null;
        this.filterComponentInput = null; // Changed from filterSourceSelect
        this.filterLevelSelect = null;
    }

    onActivate() {
        // REFACTORED: Removed the <div class="page-header"> element
        this.renderLayout();
        this.initEvents();
        this.loadInitialLogs();
    }

    onDeactivate() {
        this.el.innerHTML = '';
    }

    renderLayout() {
        this.el.innerHTML = `
            <div class="card">
                <div class="card-header d-flex justify-content-between align-items-center">
                    <h3 class="card-title mb-0">日志中心</h3>
                    <div class="d-flex gap-2">
                        <input type="text" class="form-control" id="logs-filter-component" placeholder="组件名称" aria-label="组件名称" style="width: 200px;">
                        <select class="form-select" id="logs-filter-level" aria-label="Log Level" style="width: 150px;">
                            <option value="">所有级别</option>
                            <option value="INFO">Info</option>
                            <option value="WARNING">Warning</option>
                            <option value="ERROR">Error</option>
                            <option value="CRITICAL">Critical</option>
                        </select>
                    </div>
                </div>
                <div id="log-viewer-main-container" class="log-output" style="height: 75vh; overflow-y: auto;">
                    <div id="log-viewer-main-content" class="log-entry-list"></div>
                </div>
            </div>
        `;
        this.logOutput = this.el.querySelector('#log-viewer-main-content');
        this.filterComponentInput = this.el.querySelector('#logs-filter-component'); // Changed assignment
        this.filterLevelSelect = this.el.querySelector('#logs-filter-level');
    }

    initEvents() {
        const container = this.el.querySelector('#log-viewer-main-container');
        container.addEventListener('scroll', () => {
            // A simple threshold check to load more when user is near the top
            if (container.scrollTop < 100) {
                this.fetchLogs(false); // Fetch older logs, don't clear view
            }
        });
        
        const handleFilterChange = () => {
            this.currentFilters.component = this.filterComponentInput.value || null; // Changed reference
            this.currentFilters.level = this.filterLevelSelect.value || null;
            this.loadInitialLogs(); // Reload with new filters
        };

        this.filterComponentInput.addEventListener('input', handleFilterChange); // Changed event listener and reference
        this.filterLevelSelect.addEventListener('change', handleFilterChange);
    }
    
    async loadInitialLogs() {
        this.logOutput.innerHTML = '<div class="text-center p-5"><div class="spinner-border loading-spinner" role="status"></div></div>';
        this.hasMoreLogs = true;
        this.isLoadingMore = false;
        this.currentFilters.before_line = null; // Reset pagination
        this.fetchLogs(true); // Removed populateSourceFilter call
    }

    async fetchLogs(isInitialLoad) {
        if (this.isLoadingMore || !this.hasMoreLogs) return;
        this.isLoadingMore = true;

        const loader = document.createElement('div');
        loader.className = 'log-loader text-center text-muted p-2';
        loader.innerHTML = '<div class="spinner-border spinner-border-sm" role="status"></div>';
        
        if (isInitialLoad) {
            this.logOutput.innerHTML = '';
        }
        
        this.logOutput.prepend(loader);
        try {
            const logs = await apiService.getLog(this.currentFilters);
            loader.remove();
            if (logs.length === 0) {
                this.hasMoreLogs = false;
                if (isInitialLoad) {
                    this.logOutput.innerHTML = '<div class="text-center text-muted p-5 fst-italic">没有符合条件的日志。</div>';
                } else {
                    const noMoreLogsEl = document.createElement('div');
                    noMoreLogsEl.className = 'text-center text-muted p-2 fst-italic';
                    noMoreLogsEl.textContent = '没有更早的日志了';
                    this.logOutput.prepend(noMoreLogsEl);
                }
            } else {
                const newHtml = logs.map(this.formatLogEntry).join('');
                this.logOutput.insertAdjacentHTML('beforeend', newHtml);
                
                this.currentFilters.before_line = logs[logs.length - 1].line_number;
                
                if (isInitialLoad) {
                    const container = this.el.querySelector('#log-viewer-main-container');
                    container.scrollTop = container.scrollHeight;
                }
            }
        } catch (error) {
            console.error('Failed to fetch logs:', error);
            loader.innerHTML = `<div class="text-center text-danger p-3">加载日志失败: ${error.message}</div>`;
        } finally {
            this.isLoadingMore = false;
        }
    }
    
    formatLogEntry(entry) {
        const levelClass = `log-level-${entry.level.toLowerCase()}`;
        const time = new Date(entry.ts).toLocaleTimeString([], { hour12: false });
        const sanitizedMsg = entry.msg.replace(/</g, "&lt;").replace(/>/g, "&gt;");
        return `<div data-line-number="${entry.line_number}"><span class="log-meta">${time} [${entry.component}]</span> <span class="${levelClass}">${entry.level}</span>: ${sanitizedMsg}</div>`;
    }
}