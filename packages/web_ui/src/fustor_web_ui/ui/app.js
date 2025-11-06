// src/fuagent/ui/app.js

/**
 * Fuagent - 主应用入口
 */

import { initNavigation, navigate } from './js/navigation.js';
import { initGlobalState } from './js/state.js';
import apiService from './js/apiService.js';
import stateStore from './js/stateStore.js';

// View imports for the new flattened navigation structure
import DashboardView from './js/views/DashboardView.js';
import LogsView from './js/views/LogsView.js';
import WizardView from './js/views/WizardView.js';
import SyncWizardView from './js/views/SyncWizardView.js';
import SourcesView from './js/views/SourcesView.js';
import PushersView from './js/views/PushersView.js';
import SyncTasksPageView from './js/views/SyncTasksPageView.js';

const viewInstances = {
    dashboard: null,
    sources: null,
    pushers: null,
    'sync-tasks': null,
    logs: null,
    wizard: null,
    'sync-wizard': null,
};

/**
 * NEW: A dedicated function to set up event listeners for global UI elements.
 */
function setupGlobalEventListeners() {
    const applyBtn = document.getElementById('apply-changes-btn');
    const banner = document.getElementById('global-alert-banner');

    if (applyBtn && banner) {
        applyBtn.addEventListener('click', async () => {
            const originalText = applyBtn.innerHTML;
            applyBtn.disabled = true;
            applyBtn.innerHTML = `<span class="spinner-border spinner-border-sm me-2" role="status"></span>Applying...`;

            try {
                // apiService handles its own success/error notifications.
                await apiService.applyChanges();
                // Hide the banner on success.
                banner.classList.add('d-none');
            } catch (error) {
                console.error("Failed to apply changes:", error);
                // Button is re-enabled in the finally block.
            } finally {
                applyBtn.disabled = false;
                applyBtn.innerHTML = originalText;
            }
        });
    }
}


export async function initializeApp() {
    initGlobalState();
    
    viewInstances.dashboard = new DashboardView('dashboard-view');
    viewInstances.sources = new SourcesView('sources-view');
    viewInstances.pushers = new PushersView('pushers-view');
    viewInstances['sync-tasks'] = new SyncTasksPageView('sync-tasks-view');
    viewInstances.logs = new LogsView('logs-view');
    viewInstances.wizard = new WizardView('wizard-view');
    viewInstances['sync-wizard'] = new SyncWizardView('sync-wizard-view');
    
    initNavigation(getViewInstance); 
    
    // NEW: Call the function to attach the event listener on app startup.
    setupGlobalEventListeners();
    
    await loadInitialConfig();
}

export function getViewInstance(viewId) {
    const instance = viewInstances[viewId];
    console.log(`[GET VIEW INSTANCE] Requesting view: ${viewId}. Returning instance:`, instance);
    if (!instance) {
        console.warn(`No view instance found for viewId: ${viewId}`);
    }
    return instance;
}

async function loadInitialConfig() {
    try {
        const appConfig = await apiService.getConfig();
        const instancesStatus = await apiService.getInstancesStatus();
        console.log('app.js: Initial appConfig fetched:', appConfig);
        console.log('app.js: Initial instancesStatus fetched:', instancesStatus);
        stateStore.setState({ appConfig, instancesStatus });
    } catch (error) {
        console.error('初始化失败:', error);
        const pageBody = document.querySelector('#main-content-area');
        if (pageBody) {
            pageBody.innerHTML = `<div class="alert alert-danger">应用初始化失败: ${error.message}. 请检查后端服务是否正在运行并刷新页面。</div>`;
        }
    }
}

// 启动应用
initializeApp();