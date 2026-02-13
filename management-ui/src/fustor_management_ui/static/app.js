const client = new FusionClient();
let _editingAgentId = null, _isEditingFusion = false, _currentConfigDict = null;
let _availableDrivers = { sources: [], senders: [], receivers: [], views: [] };

function promptKey() {
    let k = prompt('API Key:', getKey());
    if (k !== null) {
        setKey(k);
        client.setApiKey(k);
        refresh();
    }
}

async function handleApiError(e) {
    if (e.status === 401 || e.status === 403) {
        promptKey();
    } else {
        const errEl = document.getElementById('error');
        if (errEl) errEl.innerHTML = `<div class="error-msg">${e.message}</div>`;
        console.error(`[API Error]`, e);
    }
}

async function refresh() {
    try {
        client.setApiKey(getKey());
        const dashboard = await client.getDashboard();
        renderDashboard(dashboard);
        
        const cfg = await client.getFusionConfig();
        if (cfg.config_dict) {
            document.getElementById('configOverview').textContent = JSON.stringify({
                receivers: cfg.config_dict.receivers,
                views: cfg.config_dict.views,
                pipes: cfg.config_dict.pipes
            }, null, 2);
        }
        document.getElementById('lastRefresh').textContent = 'Updated: ' + new Date().toLocaleTimeString();
    } catch (e) { handleApiError(e); }
}

async function fetchDrivers() {
    try {
        _availableDrivers = await client.getDrivers();
        console.log("Loaded drivers:", _availableDrivers);
    } catch (e) { console.warn("Failed to fetch drivers, using defaults", e); }
}

function initAutocomplete(inputId, resultsId, suggestions) {
    const input = document.getElementById(inputId);
    const results = document.getElementById(resultsId);
    if (!input || !results) return;

    input.oninput = () => {
        const val = input.value.toLowerCase();
        results.innerHTML = '';
        if (!val) { results.classList.remove('active'); return; }
        
        const filtered = suggestions.filter(s => s.toLowerCase().includes(val));
        if (filtered.length > 0) {
            filtered.forEach(s => {
                const div = document.createElement('div');
                div.className = 'autocomplete-item';
                div.textContent = s;
                div.onclick = () => { input.value = s; results.classList.remove('active'); input.dispatchEvent(new Event('input')); };
                results.appendChild(div);
            });
            results.classList.add('active');
        } else {
            results.classList.remove('active');
        }
    };

    input.onblur = () => setTimeout(() => results.classList.remove('active'), 200);
}

function validateId(id) {
    return /^[a-zA-Z0-9_-]+$/.test(id);
}

function initIdValidation(inputId) {
    const input = document.getElementById(inputId);
    if (!input) return;
    input.oninput = (e) => {
        const val = input.value.trim();
        if (val && !validateId(val)) input.classList.add('invalid');
        else input.classList.remove('invalid');
    };
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

function closeConfigModal() { document.getElementById('configModal').classList.remove('active'); _currentConfigDict = null; }

async function editFusionConfig() {
    _isEditingFusion = true; _editingAgentId = null;
    document.getElementById('modalTitle').textContent = 'Fusion Configuration Editor';
    document.getElementById('agentMetaArea').style.display = 'none';
    document.getElementById('fusionReceiversArea').style.display = 'block';
    document.getElementById('agentSendersArea').style.display = 'none';
    document.getElementById('itemsTitle').textContent = 'Views';
    document.getElementById('agentPipeSelection').style.display = 'none';
    document.getElementById('fusionPipeSelection').style.display = 'block';
    
    // Set View Drivers
    const views = _availableDrivers.views.length ? _availableDrivers.views : ['fs', 'multi-fs'];
    document.getElementById('item_driver').innerHTML = views.map(v => `<option value="${v}">${v}</option>`).join('');

    // Set Receiver Drivers
    const recvs = _availableDrivers.receivers.length ? _availableDrivers.receivers : ['http'];
    document.getElementById('f_rcv_driver').innerHTML = recvs.map(r => `<option value="${r}">${r}</option>`).join('');
    
    document.getElementById('configModal').classList.add('active');
    try { 
        let d = await client.getFusionConfig(); 
        _currentConfigDict = d.config_dict; 
        renderStructured(); 
    } catch (e) { handleApiError(e); }
}

async function editConfig(id) {
    _isEditingFusion = false; _editingAgentId = id;
    document.getElementById('modalTitle').textContent = `Agent Config: ${id}`;
    document.getElementById('agentMetaArea').style.display = 'block';
    document.getElementById('displayAgentId').textContent = id;
    document.getElementById('fusionReceiversArea').style.display = 'none';
    document.getElementById('agentSendersArea').style.display = 'block';
    document.getElementById('itemsTitle').textContent = 'Sources';
    document.getElementById('agentPipeSelection').style.display = 'block';
    document.getElementById('fusionPipeSelection').style.display = 'none';
    
    // Set Source Drivers
    const sources = _availableDrivers.sources.length ? _availableDrivers.sources : ['fs', 'mysql', 'oss'];
    document.getElementById('item_driver').innerHTML = sources.map(s => `<option value="${s}">${s}</option>`).join('');

    // Set Sender Drivers
    const senders = _availableDrivers.senders.length ? _availableDrivers.senders : ['fusion', 'echo'];
    document.getElementById('snd_driver').innerHTML = senders.map(s => `<option value="${s}">${s}</option>`).join('');

    document.getElementById('configModal').classList.add('active');
    try {
        let d = await client.getAgentConfig(id);
        if (d.status === 'pending') {
            await client.getAgentConfig(id, true); // trigger=true
            for (let i = 0; i < 5; i++) { 
                await new Promise(r => setTimeout(r, 1000)); 
                d = await client.getAgentConfig(id); 
                if (d.status === 'ok') break; 
            }
        }
        _currentConfigDict = d.config_dict; renderStructured();
    } catch (e) { handleApiError(e); }
}

function renderStructured() {
    const cfg = _currentConfigDict;
    if (!cfg) return;

    // --- Items ---
    const items = _isEditingFusion ? (cfg.views || {}) : (cfg.sources || {});
    document.getElementById('itemsList').innerHTML = Object.entries(items).map(([id, val]) => `
        <div class="list-item"><span><b>${id}</b> (${val.driver})</span>
        <button class="btn btn-sm btn-red" onclick="removeItem('${id}')">✕</button></div>
    `).join('') || '<div style="color:var(--text-secondary); font-size:11px;">Empty</div>';

    // --- Agent Senders ---
    if (!_isEditingFusion) {
        document.getElementById('sendersList').innerHTML = Object.entries(cfg.senders || {}).map(([id, val]) => `
            <div class="list-item"><span><b>${id}</b> (${val.driver})</span>
            <button class="btn btn-sm btn-red" onclick="removeSender('${id}')">✕</button></div>
        `).join('') || '<div style="color:var(--text-secondary); font-size:11px;">Empty</div>';
        
        document.getElementById('p_source').innerHTML = '<option value="">- Select Source -</option>' + Object.keys(cfg.sources || {}).map(s => `<option value="${s}">${s}</option>`).join('');
        document.getElementById('p_sender').innerHTML = '<option value="">- Select Sender -</option>' + Object.keys(cfg.senders || {}).map(s => `<option value="${s}">${s}</option>`).join('');
    }

    // --- Fusion Receivers ---
    if (_isEditingFusion) {
        document.getElementById('fusionReceiversList').innerHTML = Object.entries(cfg.receivers || {}).map(([id, val]) => `
            <div class="list-item"><span><b>${id}</b> (Port: ${val.port})</span>
            <button class="btn btn-sm btn-red" onclick="removeReceiver('${id}')">✕</button></div>
        `).join('') || '<div style="color:var(--text-secondary); font-size:11px;">Empty</div>';
        
        document.getElementById('p_receiver').innerHTML = '<option value="">- Select Receiver -</option>' + Object.keys(cfg.receivers || {}).map(r => `<option value="${r}">${r}</option>`).join('');
        document.getElementById('p_views_checkboxes').innerHTML = Object.keys(cfg.views || {}).map(v => `
            <label style="display:block; font-size:11px; margin-bottom:4px; cursor:pointer;"><input type="checkbox" name="f_views" value="${v}" style="width:auto; margin-bottom:0; margin-right:8px;">${v}</label>
        `).join('') || '<div style="color:var(--red); font-size:11px;">Add a View first!</div>';
    }

    // --- Pipes ---
    document.getElementById('pipesList').innerHTML = Object.entries(cfg.pipes || {}).map(([id, val]) => `
        <div class="list-item"><div><div class="mono" style="font-weight:bold; color:var(--accent);">${id}</div>
        <div style="font-size:10px; color:var(--text-secondary)">${_isEditingFusion ? 'Recv: ' + val.receiver + ' | Views: ' + (val.views||[]).join(',') : 'Src: ' + val.source + ' | Snd: ' + val.sender}</div></div>
        <button class="btn btn-sm btn-red" onclick="removePipe('${id}')">Delete</button></div>
    `).join('') || '<div style="color:var(--text-secondary); font-size:11px;">No active pipes</div>';

    // Initialize Autocompletes
    const existingItemIds = Object.keys(_isEditingFusion ? (cfg.views || {}) : (cfg.sources || {}));
    initAutocomplete('item_id', 'item_id_results', existingItemIds);
    initAutocomplete('p_id', 'p_id_results', Object.keys(cfg.pipes || {}));

    // Initialize Validations
    initIdValidation('item_id');
    initIdValidation('p_id');
}

function addItem() {
    let id = document.getElementById('item_id').value.trim(), drv = document.getElementById('item_driver').value, uri = document.getElementById('item_uri').value.trim();
    if (!id || !drv || !uri) return alert('Fill all fields');
    if (!/^[a-zA-Z0-9_-]+$/.test(id)) return alert('Invalid ID format');
    
    if (_isEditingFusion) { if (!_currentConfigDict.views) _currentConfigDict.views = {}; _currentConfigDict.views[id] = { driver: drv, driver_params: { root_path: uri } }; }
    else { if (!_currentConfigDict.sources) _currentConfigDict.sources = {}; _currentConfigDict.sources[id] = { driver: drv, uri: uri }; }
    
    document.getElementById('item_id').value = ''; document.getElementById('item_uri').value = '';
    toggleElement('addItemForm'); renderStructured();
}

function removeItem(id) {
    if (!confirm('Remove this item?')) return;
    let inUse = Object.values(_currentConfigDict.pipes || {}).some(p => _isEditingFusion ? (p.views||[]).includes(id) : p.source === id);
    if (inUse) return alert('Denied: Item is being used by a Pipe');
    if (_isEditingFusion) delete _currentConfigDict.views[id]; else delete _currentConfigDict.sources[id];
    renderStructured();
}

function addReceiver() {
    let id = document.getElementById('f_rcv_id').value.trim(), drv = document.getElementById('f_rcv_driver').value, port = parseInt(document.getElementById('f_rcv_port').value);
    if (!id || !port) return alert('Fill all fields');
    if (!_currentConfigDict.receivers) _currentConfigDict.receivers = {};
    _currentConfigDict.receivers[id] = { driver: drv, port: port, api_keys: [] };
    document.getElementById('f_rcv_id').value = '';
    toggleElement('addReceiverForm'); renderStructured();
}

function removeReceiver(id) {
    if (!confirm('Remove receiver?')) return;
    if (Object.values(_currentConfigDict.pipes || {}).some(p => p.receiver === id)) return alert('Denied: Receiver is being used by a Pipe');
    delete _currentConfigDict.receivers[id]; renderStructured();
}

function addSender() {
    let id = document.getElementById('snd_id').value.trim(), drv = document.getElementById('snd_driver').value, uri = document.getElementById('snd_uri').value.trim(), key = document.getElementById('snd_key').value.trim();
    if (!id || !uri || !key) return alert('Fill all fields');
    if (!_currentConfigDict.senders) _currentConfigDict.senders = {};
    _currentConfigDict.senders[id] = { driver: drv, uri: uri, credential: { key: key } };
    document.getElementById('snd_id').value = ''; document.getElementById('snd_uri').value = ''; document.getElementById('snd_key').value = '';
    toggleElement('addSenderForm'); renderStructured();
}

function removeSender(id) {
    if (!confirm('Remove sender?')) return;
    if (Object.values(_currentConfigDict.pipes || {}).some(p => p.sender === id)) return alert('Denied: Sender is being used by a Pipe');
    delete _currentConfigDict.senders[id]; renderStructured();
}

function removePipe(id) { if (confirm('Delete pipe?')) { delete _currentConfigDict.pipes[id]; renderStructured(); } }

function confirmAddPipe() {
    let id = document.getElementById('p_id').value.trim();
    if (!id) return alert('ID required');
    if (!/^[a-zA-Z0-9_-]+$/.test(id)) return alert('ID can only contain alphanumeric, dash and underscore');
    if (_currentConfigDict.pipes && _currentConfigDict.pipes[id]) return alert('Pipe ID already exists');

    if (_isEditingFusion) {
        let target = document.getElementById('p_receiver').value;
        let views = Array.from(document.querySelectorAll('input[name="f_views"]:checked')).map(cb => cb.value);
        if (!target) return alert('Select a receiver');
        if (views.length === 0) return alert('Select at least 1 view');
        
        // Redundancy check
        if (Object.values(_currentConfigDict.pipes || {}).some(p => p.receiver === target && JSON.stringify((p.views||[]).sort()) === JSON.stringify(views.sort()))) {
            return alert('Conflict: A pipe with the same Receiver/Views already exists');
        }
        if (!_currentConfigDict.pipes) _currentConfigDict.pipes = {};
        _currentConfigDict.pipes[id] = { receiver: target, views: views };
    } else {
        let src = document.getElementById('p_source').value;
        let snd = document.getElementById('p_sender').value;
        if (!src || !snd) return alert('Select Source and Sender');
        if (Object.values(_currentConfigDict.pipes || {}).some(p => p.source === src && p.sender === snd)) {
            return alert('Conflict: A pipe with this Source/Sender already exists');
        }
        if (!_currentConfigDict.pipes) _currentConfigDict.pipes = {};
        _currentConfigDict.pipes[id] = { source: src, sender: snd };
    }
    document.getElementById('p_id').value = '';
    toggleElement('addPipeForm'); renderStructured();
}

async function saveConfig() {
    let payload = _isEditingFusion ? { receivers: _currentConfigDict.receivers, views: _currentConfigDict.views, pipes: _currentConfigDict.pipes }
        : { sources: _currentConfigDict.sources, senders: _currentConfigDict.senders, pipes: _currentConfigDict.pipes };
    if (!confirm('Apply and save these changes? This will reload the service.')) return;
    try {
        if (_isEditingFusion) {
            await client.updateFusionConfigStructured(payload);
        } else {
            await client.updateAgentConfigStructured(_editingAgentId, payload);
        }
        alert('Success! Configuration applied.'); closeConfigModal(); refresh();
    } catch (e) { handleApiError(e); }
}

async function sendCmd(id, type, extra={}) { 
    try { 
        await client.sendAgentCommand(id, { type, ...extra });
        alert('Command queued'); 
        refresh(); 
    } catch(e){ handleApiError(e); } 
}

function promptUpgrade(id) { let v = prompt('Enter version:'); if (v) sendCmd(id, 'upgrade', { version: v }); }

async function reloadFusion() { 
    if(confirm('Reload Fusion?')) {
        try { 
            await client.reloadFusion();
            alert('Reload signal sent'); 
            setTimeout(refresh, 1000); 
        } catch(e){ handleApiError(e); } 
    }
}

(async () => {
    await fetchDrivers();
    refresh();
    setInterval(refresh, 5000);
})();
