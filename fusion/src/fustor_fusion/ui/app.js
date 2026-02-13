const API = '/api/v1/management';
let _editingAgentId = null, _isEditingFusion = false, _currentConfigDict = null;

async function api(path, opts = {}) {
    opts.headers = opts.headers || {};
    let key = getKey();
    if (key) opts.headers['X-Management-Key'] = key;
    let res = await fetch(API + path, opts);
    if (res.status === 401 || res.status === 403) {
        promptKey();
        throw new Error('Authentication required');
    }
    if (!res.ok) {
        let err = res.statusText;
        try { let d = await res.json(); if (d.detail) err = d.detail; } catch (e) { }
        throw new Error(err);
    }
    return res.json();
}

function promptKey() {
    let k = prompt('API Key:', getKey());
    if (k !== null) {
        setKey(k);
        refresh();
    }
}

async function refresh() {
    try {
        let d = await api('/dashboard');
        renderDashboard(d);
        
        let cfg = await api('/config');
        if (cfg.config_dict) {
            document.getElementById('configOverview').textContent = JSON.stringify({
                receivers: cfg.config_dict.receivers,
                views: cfg.config_dict.views,
                pipes: cfg.config_dict.pipes
            }, null, 2);
        }
        document.getElementById('lastRefresh').textContent = 'Updated: ' + new Date().toLocaleTimeString();
    } catch (e) {
        const errorEl = document.getElementById('error');
        if (errorEl) errorEl.innerHTML = `<div class="error-msg">${e.message}</div>`;
    }
}

function renderDashboard(data) {
    document.getElementById('statAgents').textContent = Object.keys(data.agents || {}).length;
    document.getElementById('statPipes').textContent = Object.keys(data.pipes || {}).length;
    document.getElementById('statViews').textContent = Object.keys(data.views || {}).length;
    document.getElementById('statSessions').textContent = Object.values(data.sessions || {}).flat().length;

    document.getElementById('agentsBody').innerHTML = Object.entries(data.agents || {}).map(([id, a]) => `
        <tr><td class="mono">${id}</td><td class="mono">${a.client_ip || '-'}</td><td>${a.sessions.length}</td>
        <td><button class="btn btn-sm" onclick="editConfig('${id}')">Config</button>
        <button class="btn btn-sm" onclick="sendCmd('${id}','reload_config')">Reload</button>
        <button class="btn btn-sm" onclick="promptUpgrade('${id}')">Upgrade</button></td></tr>
    `).join('');

    document.getElementById('pipesBody').innerHTML = Object.entries(data.pipes || {}).map(([id, p]) => `
        <tr><td class="mono">${id}</td><td class="mono">${p.view_id || id}</td><td>${p.active_sessions}</td><td>${badge(p.state, p.state === 'RUNNING' ? 'green' : 'yellow')}</td></tr>
    `).join('');

    document.getElementById('sessionsBody').innerHTML = Object.entries(data.sessions || {}).flatMap(([vid, ss]) => ss.map(s => `
        <tr><td class="mono">${s.session_id.substring(0, 8)}</td><td class="mono">${vid}</td><td class="mono">${s.agent_id}</td>
        <td>${badge(s.agent_status?.role || '-', s.agent_status?.role === 'leader' ? 'purple' : 'blue')}</td>
        <td>${badge(s.can_realtime ? 'YES' : 'NO', s.can_realtime ? 'green' : 'yellow')}</td>
        <td>${formatAge(s.age_seconds)}</td><td>${formatAge(s.idle_seconds)}</td><td>${s.agent_status?.events_pushed || 0}</td></tr>
    `)).join('');
}

async function sendCmd(id, type, extra = {}) {
    try {
        await api(`/agents/${id}/command`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ type, ...extra }) });
        alert('Command sent');
        refresh();
    } catch (e) { alert(e.message); }
}

function promptUpgrade(id) {
    let v = prompt('Version (e.g. 0.9.2):');
    if (v) sendCmd(id, 'upgrade', { version: v });
}

async function reloadFusion() {
    if (confirm('Reload Fusion?')) {
        try {
            await api('/reload', { method: 'POST' });
            alert('Reloaded');
            setTimeout(refresh, 1000);
        } catch (e) { alert(e.message); }
    }
}

function closeConfigModal() {
    document.getElementById('configModal').classList.remove('active');
}

async function editFusionConfig() {
    _isEditingFusion = true; _editingAgentId = null;
    document.getElementById('modalTitle').textContent = 'Fusion Structured Editor';
    document.getElementById('agentMetaArea').style.display = 'none';
    document.getElementById('fusionReceiversArea').style.display = 'block';
    document.getElementById('agentSendersArea').style.display = 'none';
    document.getElementById('itemsTitle').textContent = 'Views';
    document.getElementById('p_views_area').style.display = 'block';
    document.getElementById('configModal').classList.add('active');

    try {
        let d = await api('/config');
        _currentConfigDict = d.config_dict;
        renderStructured();
    } catch (e) { alert(e.message); }
}

async function editConfig(id) {
    _isEditingFusion = false; _editingAgentId = id;
    document.getElementById('modalTitle').textContent = `Agent Editor: ${id}`;
    document.getElementById('agentMetaArea').style.display = 'block';
    document.getElementById('displayAgentId').textContent = id;
    document.getElementById('fusionReceiversArea').style.display = 'none';
    document.getElementById('agentSendersArea').style.display = 'block';
    document.getElementById('itemsTitle').textContent = 'Sources';
    document.getElementById('p_views_area').style.display = 'none';
    document.getElementById('configModal').classList.add('active');

    try {
        let d = await api(`/agents/${id}/config`);
        if (d.status === 'pending') {
            await api(`/agents/${id}/config?trigger=true`);
            for (let i = 0; i < 5; i++) {
                await new Promise(r => setTimeout(r, 1000));
                d = await api(`/agents/${id}/config`);
                if (d.status === 'ok') break;
            }
        }
        _currentConfigDict = d.config_dict;
        renderStructured();
    } catch (e) { alert(e.message); }
}

function renderStructured() {
    const cfg = _currentConfigDict;
    // Items (Sources/Views)
    const items = _isEditingFusion ? (cfg.views || {}) : (cfg.sources || {});
    document.getElementById('itemsList').innerHTML = Object.entries(items).map(([id, val]) => `
        <div class="list-item"><span><b>${id}</b> (${val.driver})</span>
        <button class="btn btn-sm btn-red" onclick="removeItem('${id}')">✕</button></div>
    `).join('') || 'Empty';

    // Senders (Agent Only)
    if (!_isEditingFusion) {
        document.getElementById('sendersList').innerHTML = Object.entries(cfg.senders || {}).map(([id, val]) => `
            <div class="list-item"><span><b>${id}</b> (${val.driver})</span>
            <button class="btn btn-sm btn-red" onclick="removeSender('${id}')">✕</button></div>
        `).join('') || 'Empty';
    }

    // Receivers (Fusion Only)
    if (_isEditingFusion) {
        document.getElementById('fusionReceiversList').innerHTML = Object.entries(cfg.receivers || {}).map(([id, val]) => `
            <div class="list-item"><span><b>${id}</b> (Port: ${val.port})</span>
            <button class="btn btn-sm btn-red" onclick="removeReceiver('${id}')">✕</button></div>
        `).join('') || 'Empty';
    }

    // Pipes
    document.getElementById('pipesList').innerHTML = Object.entries(cfg.pipes || {}).map(([id, val]) => `
        <div class="list-item"><div><div class="mono" style="font-weight:bold; color:var(--accent);">${id}</div>
        <div style="font-size:10px; color:var(--text-secondary)">${_isEditingFusion ? 'Recv: ' + val.receiver + ' | Views: ' + val.views.join(',') : 'Src: ' + val.source + ' | Snd: ' + val.sender}</div></div>
        <button class="btn btn-sm btn-red" onclick="removePipe('${id}')">Delete</button></div>
    `).join('') || 'No pipes';

    // Dropdowns
    let targetList = _isEditingFusion ? Object.keys(cfg.receivers || {}) : Object.keys(cfg.senders || {});
    document.getElementById('p_target').innerHTML = '<option value="">- Select -</option>' + targetList.map(t => `<option value="${t}">${t}</option>`).join('');
}

function addItem() {
    let id = document.getElementById('item_id').value.trim(), drv = document.getElementById('item_driver').value.trim(), uri = document.getElementById('item_uri').value.trim();
    if (!id || !drv || !uri) return alert('Fill all');
    if (_isEditingFusion) { if (!_currentConfigDict.views) _currentConfigDict.views = {}; _currentConfigDict.views[id] = { driver: drv, driver_params: { root_path: uri } }; }
    else { if (!_currentConfigDict.sources) _currentConfigDict.sources = {}; _currentConfigDict.sources[id] = { driver: drv, uri: uri }; }
    toggleElement('addItemForm'); renderStructured();
}

function removeItem(id) {
    let inUse = Object.values(_currentConfigDict.pipes || {}).some(p => _isEditingFusion ? p.views.includes(id) : p.source === id);
    if (inUse) return alert('In use');
    if (_isEditingFusion) delete _currentConfigDict.views[id]; else delete _currentConfigDict.sources[id];
    renderStructured();
}

function addSender() {
    let id = document.getElementById('snd_id').value.trim(), uri = document.getElementById('snd_uri').value.trim(), key = document.getElementById('snd_key').value.trim();
    if (!id || !uri || !key) return alert('Fill all');
    if (!_currentConfigDict.senders) _currentConfigDict.senders = {};
    _currentConfigDict.senders[id] = { driver: 'fusion', uri: uri, credential: { key: key } };
    toggleElement('addSenderForm'); renderStructured();
}

function removeSender(id) { if (Object.values(_currentConfigDict.pipes || {}).some(p => p.sender === id)) return alert('In use'); delete _currentConfigDict.senders[id]; renderStructured(); }

function addFusionReceiver() {
    let id = document.getElementById('f_rcv_id').value.trim(), port = parseInt(document.getElementById('f_rcv_port').value), key = document.getElementById('f_rcv_key').value.trim(), pipe = document.getElementById('f_rcv_pipe').value.trim();
    if (!id || !port || !key || !pipe) return alert('Fill all');
    if (!_currentConfigDict.receivers) _currentConfigDict.receivers = {};
    _currentConfigDict.receivers[id] = { driver: 'http', port: port, api_keys: [{ key: key, pipe_id: pipe }] };
    toggleElement('addFusionReceiverForm'); renderStructured();
}

function removeReceiver(id) { if (Object.values(_currentConfigDict.pipes || {}).some(p => p.receiver === id)) return alert('In use'); delete _currentConfigDict.receivers[id]; renderStructured(); }

function removePipe(id) { if (confirm('Delete?')) { delete _currentConfigDict.pipes[id]; renderStructured(); } }

function confirmAddPipe() {
    let id = document.getElementById('p_id').value.trim(), target = document.getElementById('p_target').value;
    if (!id || !target) return alert('Fill all');
    if (_currentConfigDict.pipes && _currentConfigDict.pipes[id]) return alert('ID exists');

    if (_isEditingFusion) {
        let views = document.getElementById('p_views').value.split(',').map(v => v.trim()).filter(v => v);
        if (views.length === 0) return alert('At least 1 view');
        if (Object.values(_currentConfigDict.pipes).some(p => p.receiver === target && JSON.stringify(p.views.sort()) === JSON.stringify(views.sort()))) return alert('Redundant Pipe');
        if (!_currentConfigDict.pipes) _currentConfigDict.pipes = {};
        _currentConfigDict.pipes[id] = { receiver: target, views: views };
    } else {
        // Redundancy check for Agent
        const isRedundant = Object.values(_currentConfigDict.pipes || {}).some(p => p.source === target && p.sender === 'fusion-placeholder'); // This logic needs careful source/sender mapping
        // In Agent mode, p_target is actually the Source selection in the dropdown (see renderStructured)
        const source = target;
        const sender = Object.keys(_currentConfigDict.senders || {})[0]; // Auto-pick first sender if not specified
        if (!sender) return alert('Add a sender first');
        
        if (Object.values(_currentConfigDict.pipes || {}).some(p => p.source === source && p.sender === sender)) return alert('Redundant Pipe');
        
        if (!_currentConfigDict.pipes) _currentConfigDict.pipes = {};
        _currentConfigDict.pipes[id] = { source: source, sender: sender };
    }
    toggleElement('addPipeForm'); renderStructured();
}

async function saveConfig() {
    let url = _isEditingFusion ? '/config/structured' : `/agents/${_editingAgentId}/config/structured`;
    let payload = _isEditingFusion ? { receivers: _currentConfigDict.receivers, views: _currentConfigDict.views, pipes: _currentConfigDict.pipes }
        : { sources: _currentConfigDict.sources, senders: _currentConfigDict.senders, pipes: _currentConfigDict.pipes };

    if (!confirm('Save all changes?')) return;
    try {
        await api(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
        alert('Success'); closeConfigModal(); refresh();
    } catch (e) { alert('Failed: ' + e.message); }
}

// Init
refresh();
setInterval(refresh, 5000);
