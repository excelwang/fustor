// src/fuagent/ui/js/components/LogPreview.js
import apiService from '../apiService.js';

export default class LogPreview {
    /**
     * A reusable component to render a snapshot of recent logs.
     * @param {HTMLElement} containerElement - The DOM element to render the log preview into.
     */
    constructor(containerElement) {
        this.container = containerElement;
    }

    /**
     * Fetches log data and renders it into the container.
     * @param {object} [filterParams={ limit: 50 }] - Optional parameters for the log API call.
     */
    async render(filterParams = { limit: 50 }) {
        if (!this.container) return;

        this.container.innerHTML = '<div class="text-muted text-center p-4">Loading logs...</div>';

        try {
            const logs = await apiService.getLog(filterParams);
            if (logs && logs.length > 0) {
                this.container.innerHTML = logs.map(entry => {
                    const levelClass = {
                        'INFO': 'text-info',
                        'WARNING': 'text-warning',
                        'ERROR': 'text-danger',
                        'CRITICAL': 'text-danger fw-bold'
                    }[entry.level] || 'text-muted';
                    const time = new Date(entry.ts).toLocaleTimeString([], { hour12: false });
                    const component = entry.component || 'system';
                    const sanitizedMsg = entry.msg.replace(/</g, "&lt;").replace(/>/g, "&gt;");
                    return `<div class="log-entry"><span class="text-muted">${time}</span> [<span class="${levelClass}">${entry.level}</span>] <span class="text-muted">[${component}]</span> ${sanitizedMsg}</div>`;
                }).join('');
            } else {
                this.container.innerHTML = '<div class="text-muted text-center p-4">No logs available.</div>';
            }
        } catch (error) {
            this.container.innerHTML = `<div class="text-danger text-center p-4">Failed to load log preview: ${error.message}</div>`;
        }
    }
}