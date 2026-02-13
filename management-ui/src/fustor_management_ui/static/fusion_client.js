/**
 * Fusion JS Client - JavaScript version of FusionClient for the web frontend.
 * Mirrors the Python SDK's management and orchestration APIs.
 */
class FusionClient {
    constructor(baseUrl = '', apiKey = null) {
        this.baseUrl = baseUrl;
        this.apiKey = apiKey;
        this.managementPath = '/api/v1/management';
    }

    setApiKey(key) {
        this.apiKey = key;
    }

    async request(path, opts = {}) {
        const url = this.baseUrl + path;
        opts.headers = opts.headers || {};
        if (this.apiKey) {
            opts.headers['X-Management-Key'] = this.apiKey;
        }

        const res = await fetch(url, opts);
        
        if (res.status === 401 || res.status === 403) {
            // Signal auth required via custom event or rejection
            const error = new Error('Authentication required');
            error.status = res.status;
            throw error;
        }

        if (!res.ok) {
            let detail = res.statusText;
            try {
                const data = await res.json();
                if (data.detail) detail = data.detail;
            } catch (e) {}
            const error = new Error(detail);
            error.status = res.status;
            throw error;
        }

        return res.json();
    }

    // --- Management API Methods ---

    async getDashboard() {
        return this.request(`${this.managementPath}/dashboard`);
    }

    async getDrivers() {
        return this.request(`${this.managementPath}/drivers`);
    }

    async getFusionConfig(filename = 'default.yaml') {
        return this.request(`${this.managementPath}/config?filename=${filename}`);
    }

    async updateFusionConfigStructured(config, filename = 'default.yaml') {
        return this.request(`${this.managementPath}/config/structured`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ ...config, filename })
        });
    }

    async getAgentConfig(agentId, trigger = false, filename = 'default.yaml') {
        return this.request(`${this.managementPath}/agents/${agentId}/config?trigger=${trigger}&filename=${filename}`);
    }

    async updateAgentConfigStructured(agentId, config, filename = 'default.yaml') {
        return this.request(`${this.managementPath}/agents/${agentId}/config/structured`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ ...config, filename })
        });
    }

    async sendAgentCommand(agentId, command) {
        return this.request(`${this.managementPath}/agents/${agentId}/command`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(command)
        });
    }

    async reloadFusion() {
        return this.request(`${this.managementPath}/reload`, { method: 'POST' });
    }
}
