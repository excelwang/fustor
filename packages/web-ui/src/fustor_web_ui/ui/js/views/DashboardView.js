// src/fuagent/ui/js/views/DashboardView.js

import apiService from '../apiService.js';
// Removed LogPreview import

export default class DashboardView {
  constructor(elementId) {
    this.el = document.getElementById(elementId);
    this.pollInterval = null;
    this.container = null;
    // Removed logPreview property
  }

  onActivate() {
    // REFACTORED: The main container is now a 'list-group' to support a row-based layout.
    this.el.innerHTML = `
      <div id="global-health-summary" class="row g-3 mb-4"></div>

      <h4>数据管道 (Data Pipelines)</h4>
      <div id="data-pipelines-container" class="list-group">
        <div class="list-group-item">
            <div class="text-center p-5" id="pipelines-loading-spinner">
                <div class="spinner-border" role="status"></div>
                <p class="mt-2">正在加载管道状态...</p>
            </div>
        </div>
      </div>
    `;
    this.container = this.el.querySelector('#data-pipelines-container');
    // Removed log preview instantiation
    
    this.addEventListeners();
    
    this.fetchDataAndRender();
    if (this.pollInterval) clearInterval(this.pollInterval);
    this.pollInterval = setInterval(() => this.fetchDataAndRender(), 5000);
  }

  onDeactivate() {
    if (this.pollInterval) {
        clearInterval(this.pollInterval);
        this.pollInterval = null;
    }
    this.el.innerHTML = '';
  }

  addEventListeners() {
      this.el.addEventListener('click', async (e) => {
          const button = e.target.closest('button[data-action]');
          if (!button) return;

          e.preventDefault();
          const action = button.dataset.action;
          const pipelineId = button.closest('[data-pipeline-id]').dataset.pipelineId;
          
          if (!pipelineId || !action) return;

          const card = this.el.querySelector(`[data-pipeline-id="${pipelineId}"]`);
          if(card) card.querySelectorAll('button').forEach(btn => btn.disabled = true);

          try {
              if (action === 'start') {
                  await apiService.startSyncInstance(pipelineId);
              } else if (action === 'stop') {
                  await apiService.stopSyncInstance(pipelineId);
              } else if (action === 'restart') {
                  await apiService.stopSyncInstance(pipelineId);
                  setTimeout(() => apiService.startSyncInstance(pipelineId), 1000);
              }
          } catch (error) {
              if (card) {
                  card.querySelectorAll('button').forEach(btn => btn.disabled = false);
              }
          }
      });

      this.el.addEventListener('click', (e) => {
          if (e.target.matches('#view-all-logs-btn')) {
              e.preventDefault();
              // This relies on the main app navigation logic
              window.location.hash = 'logs';
          }
      });
  }
  
  async fetchDataAndRender() {
    try {
        const status = await apiService.getInstancesStatus();
        this._renderGlobalSummary(status.global_summary);
        // The API now returns all tasks, so we render them all here.
        // The filtering logic will be handled inside _renderPipelines.
        this._renderPipelines(status.pipelines || []);
            
            // Removed logPreview rendering

            const globalAlertBanner = document.getElementById('global-alert-banner');
        if (globalAlertBanner) {
            if (status.global_summary && status.global_summary.outdated_pipelines > 0) {
                globalAlertBanner.classList.remove('d-none');
            } else {
                globalAlertBanner.classList.add('d-none');
            }
        }
    } catch (error) {
        console.error("Dashboard status poll failed:", error);
        if (this.container) {
            this.container.innerHTML = `<div class="col-12"><div class="alert alert-danger">获取Dashboard状态失败: ${error.message}</div></div>`;
        }
    }
  }

  _renderGlobalSummary(summary) {
      const summaryContainer = this.el.querySelector('#global-health-summary');
      if (!summary || !summaryContainer) return;

      const { running_pipelines, error_pipelines, outdated_pipelines } = summary;
      const systemStatus = error_pipelines > 0 ?
          'ERROR' : (outdated_pipelines > 0 ? 'WARNING' : 'OPERATIONAL');
      
      const statusInfo = {
          OPERATIONAL: { icon: 'circle-check', color: 'green', text: 'Healthy' },
          WARNING: { icon: 'alert-triangle', color: 'orange', text: 'Attention Required' },
          ERROR: { icon: 'alert-circle', color: 'red', text: 'Errors Detected' }
      }[systemStatus];

      summaryContainer.innerHTML = `
          <div class="col-sm-6 col-lg-4">
              <div class="card">
                  <div class="card-body">
                      <div class="d-flex align-items-center">
                          <div class="subheader">系统状态</div>
                      </div>
                      <div class="h2 mb-3 mt-2 text-${statusInfo.color}">
                          <i class="ti ti-${statusInfo.icon} me-2"></i>
                          ${statusInfo.text}
                      </div>
                  </div>
              </div>
          </div>
          <div class="col-sm-6 col-lg-4">
              <div class="card">
                   <div class="card-body">
                      <div class="d-flex align-items-center">
                          <div class="subheader">运行中管道</div>
                      </div>
                       <div class="h2 mb-3 mt-2">${running_pipelines}</div>
                  </div>
              </div>
          </div>
          <div class="col-sm-6 col-lg-4">
              <div class="card">
                  <div class="card-body">
                      <div class="d-flex align-items-center">
                          <div class="subheader">错误管道</div>
                      </div>
                      <div class="h2 mb-3 mt-2 ${error_pipelines > 0 ? 'text-danger' : ''}">${error_pipelines}</div>
                  </div>
              </div>
          </div>
      `;
  }

  _renderPipelines(pipelines) {
      if (!this.container) return;

      // Filter out stopped tasks for the dashboard view
      const activePipelines = pipelines.filter(p => p.overall_status !== 'STOPPED');
      
      this.container.innerHTML = ''; // Clear previous content

      if (activePipelines.length === 0) {
         this.container.innerHTML = '<div class="col-12"><div class="text-muted p-3 text-center">当前没有正在运行的实例。您可以前往“同步任务”页面启动任务。</div></div>';
          return;
      }
      
      // REFACTORED: Wrap each card in a full-width column for proper grid layout.
      activePipelines.forEach(pipeline => {
          const cardHtml = this._createPipelineCardHtml(pipeline);
          const newCardWrapper = document.createElement('div');
          newCardWrapper.className = 'col-12'; 
          newCardWrapper.dataset.pipelineId = pipeline.id;
          newCardWrapper.innerHTML = cardHtml;
          this.container.appendChild(newCardWrapper);
      });
  }

  _createPipelineCardHtml(pipeline) {
      const { overall_status: status, sync_info, bus_info, id, source_id, pusher_id } = pipeline;
      const busState = bus_info?.state ?? 'IDLE';
      const syncState = sync_info?.state ?? 'STOPPED';

      const RUNNING_STATES = ['SNAPSHOT_SYNC', 'MESSAGE_SYNC', 'RUNNING_CONF_OUTDATE'];
      const statusClasses = {
          'ERROR': { border: 'border-danger' },
          'RUNNING_CONF_OUTDATE': { border: 'border-warning' },
          'SNAPSHOT_SYNC': { border: 'border-success' },
          'MESSAGE_SYNC': { border: 'border-success' },
          'DEFAULT': { border: 'border-secondary' }
      };
      const cardBorderClass = (statusClasses[status] || statusClasses['DEFAULT']).border;

      const nodeStatusColor = (state) => {
          if (state === 'ERROR') return 'danger';
          if (state === 'PRODUCING' || RUNNING_STATES.includes(state)) return 'success';
          return 'secondary';
      };

      const isRunning = RUNNING_STATES.includes(status);
      const isStopping = status === 'STOPPING';
      const isActionable = !isStopping;

      return `
        <div class="card ${cardBorderClass}">
            <div class="card-header py-2">
                <h3 class="card-title text-truncate mb-0" title="${id}">
                    ${id}
                    <span class="badge bg-${nodeStatusColor(status)}-lt ms-2">${status}</span>
                </h3>
                <div class="card-actions btn-list">
                    <button class="btn btn-sm btn-icon" data-action="start" title="启动" ${!isActionable || isRunning ? 'disabled' : ''}><i class="ti ti-player-play"></i></button>
                    <button class="btn btn-sm btn-icon" data-action="stop" title="停止" ${!isActionable || !isRunning ? 'disabled' : ''}><i class="ti ti-player-stop"></i></button>
                    <button class="btn btn-sm btn-icon" data-action="restart" title="重启" ${!isActionable ? 'disabled' : ''}><i class="ti ti-refresh"></i></button>
                </div>
            </div>
            <div class="card-body py-3">
                <div class="d-flex align-items-stretch">
                    <div class="flex-fill d-flex align-items-center justify-content-around pe-4">
                        <div class="pipeline-flow-node">
                            <span class="badge bg-${nodeStatusColor(busState)}-lt node-label">Source</span>
                            <div class="node-detail" title="Source: ${source_id}"><code>${source_id}</code></div>
                        </div>
                        <i class="ti ti-arrow-right text-muted mx-2 pipeline-flow-arrow"></i>
                        <div class="pipeline-flow-node">
                            <span class="badge bg-${nodeStatusColor(busState)}-lt node-label">Event Bus</span>
                            <div class="node-detail">${bus_info?.statistics.events_produced ?? 0} produced</div>
                        </div>
                        <i class="ti ti-arrow-right text-muted mx-2 pipeline-flow-arrow"></i>
                        <div class="pipeline-flow-node">
                            <span class="badge bg-${nodeStatusColor(syncState)}-lt node-label">Pusher</span>
                            <div class="node-detail" title="Pusher: ${pusher_id}"><code>${pusher_id}</code></div>
                        </div>
                    </div>

                    <div class="border-start ps-4">

                    <div class="d-flex flex-column justify-content-center h-100" style="min-width: 100px;">
                            <div class="d-flex justify-content-between align-items-center">
                                <span class="text-muted small">Pushed</span>
                                <strong class="ms-2">${sync_info?.statistics.events_pushed ?? 0}</strong>
                            </div>
                            <div class="d-flex justify-content-between align-items-center mt-2">
                                <span class="text-muted small">Buffer</span>
                                <strong class="ms-2">${bus_info?.statistics.buffer_size ?? 0}</strong>
                            </div>
                        </div>

                    </div>
                </div>
            </div>
            ${status === 'ERROR' || status === 'RUNNING_CONF_OUTDATE' ? `
              <div class="card-footer py-2">
                ${status === 'ERROR' ? `<div class="text-danger small text-truncate" title="${sync_info?.info || ''}"><i class="ti ti-alert-circle me-1"></i>${sync_info?.info || 'An unknown error occurred.'}</div>` : ''}
                ${status === 'RUNNING_CONF_OUTDATE' ? `<div class="text-warning small"><i class="ti ti-alert-triangle me-1"></i>配置已变更，请重启以应用。</div>` : ''}
              </div>
            ` : ''}
        </div>
      `;
  }
}