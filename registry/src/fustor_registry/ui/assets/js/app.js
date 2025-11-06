// --- GLOBAL NAVIGATION ---
function navigateTo(page) {
    App.renderPage(page);
    window.history.pushState({ page: page }, page, `#${page}`);
}

// --- MAIN APPLICATION LOGIC ---
const App = {
    deleteCallback: null,
    currentUser: null,
    
    init() {
        UI.init(); // Load templates and modals into the DOM
        this.addEventListeners();
        
        if (localStorage.getItem('authToken')) {
            this.showApp();
        } else {
            this.showLogin();
        }
    },

    addEventListeners() {
        document.getElementById('login-form').addEventListener('submit', async (e) => {
            e.preventDefault();
            const email = document.getElementById('login-email').value;
            const password = document.getElementById('login-password').value;
            const errorDiv = document.getElementById('login-error');
            try {
                await ApiService.login(email, password);
                errorDiv.classList.add('d-none');
                this.showApp();
            } catch (error) {
                errorDiv.textContent = error.message;
                errorDiv.classList.remove('d-none');
            }
        });

        document.getElementById('logout-btn').addEventListener('click', async (e) => {
            e.preventDefault();
            await ApiService.logout();
            this.showLogin();
            this.currentUser = null;
            UI.showAdminFeatures(false);
        });

        document.getElementById('save-datastore-btn').addEventListener('click', () => this.saveDatastore());
        document.getElementById('save-key-btn').addEventListener('click', () => this.saveApiKey());
        document.getElementById('save-user-btn').addEventListener('click', () => this.saveUser());
        document.getElementById('confirm-delete-btn').addEventListener('click', () => this.executeDelete());
    },
    
    showLogin() {
        document.getElementById('login-container').classList.remove('d-none');
        document.getElementById('app-container').classList.add('d-none');
    },

    async showApp() {
        document.getElementById('login-container').classList.add('d-none');
        document.getElementById('app-container').classList.remove('d-none');
        try {
            const user = await ApiService.getProfile();
            this.currentUser = user;
            document.getElementById('user-name-display').textContent = user.name;
            
            if (user.level === 'admin') {
                UI.showAdminFeatures(true);
            } else {
                UI.showAdminFeatures(false);
            }

            const initialPage = window.location.hash.replace('#', '') || 'dashboard';
            navigateTo(initialPage);
        } catch (error) {
             alert(`无法加载用户信息: ${error.message}。正在登出。`);
             await ApiService.logout();
             this.showLogin();
        }
    },

    async renderPage(page) {
        const pageContent = document.getElementById('page-content');
        const template = document.getElementById(`${page}-template`);
        if (!template) {
            console.error(`Template for page "${page}" not found.`);
            pageContent.innerHTML = `<div class="alert alert-danger">页面加载失败</div>`;
            return;
        }
        pageContent.innerHTML = template.innerHTML;

        try {
            switch (page) {
                case 'dashboard': await this.renderDashboard(); break;
                case 'datastores': await this.renderDatastores(); break;
                case 'keys': await this.renderKeys(); break;
                case 'profile': await this.renderProfile(); break;
                case 'users': await this.renderUsers(); break;
            }
        } catch (error) {
             pageContent.innerHTML = `<div class="alert alert-danger">页面内容加载失败: ${error.message}</div>`;
        }
        
        document.querySelectorAll('.navbar-nav .nav-item').forEach(item => item.classList.remove('active'));
        const activeNavItem = document.querySelector(`.navbar-nav .nav-item[data-page="${page}"]`);
        if (activeNavItem) {
            activeNavItem.classList.add('active');
        }
    },

    async renderDashboard() {
        const [datastores, keys] = await Promise.all([
            ApiService.getDatastores(),
            ApiService.getApiKeys()
        ]);
        document.getElementById('datastore-count').textContent = datastores.length;
        document.getElementById('key-count').textContent = keys.length;
    },

    async renderDatastores() {
        const content = document.getElementById('datastores-content');
        const emptyState = document.getElementById('datastores-empty-state');
        const tableBody = document.getElementById('datastores-table-body');
        if (!content || !emptyState || !tableBody) {
            console.warn("Datastores page elements not found, skipping render.");
            return;
        }

        const datastores = await ApiService.getDatastores();
        if (datastores.length === 0) {
            content.classList.add('d-none');
            emptyState.classList.remove('d-none');
        } else {
            content.classList.remove('d-none');
            emptyState.classList.add('d-none');
            document.getElementById('datastores-table-body').innerHTML = datastores.map(ds => `
                <tr>
                    <td>${ds.name}</td><td><span class="badge bg-blue-lt">${ds.type}</span></td>
                    <td>${ds.visible ? '<span class="badge bg-success-lt">公开</span>' : '<span class="badge bg-secondary-lt">私有</span>'}</td>
                    <td>${new Date(ds.created_at).toLocaleDateString()}</td>
                    <td class="text-end"><span class="dropdown"><button class="btn dropdown-toggle align-text-top" data-bs-boundary="viewport" data-bs-toggle="dropdown">操作</button><div class="dropdown-menu dropdown-menu-end"><a class="dropdown-item" href="#" onclick="App.openDatastoreModal(${ds.id})">编辑</a><a class="dropdown-item text-danger" href="#" onclick="App.confirmDelete('datastore', ${ds.id})">删除</a></div></span></td>
                </tr>`).join('');
        }
    },

    async openDatastoreModal(id = null) {
        // ... (existing code, no changes needed)
        const modalEl = document.getElementById('datastore-modal');
        const modal = new bootstrap.Modal(modalEl);
        document.getElementById('datastore-id').value = '';
        document.getElementById('datastore-name').value = '';
        document.getElementById('datastore-type').value = '汇交存储库';
        document.getElementById('datastore-visible').checked = false;
        document.getElementById('datastore-allow-concurrent-push').checked = false;
        document.getElementById('datastore-session-timeout-seconds').value = 30;
        document.getElementById('datastore-meta').value = '';
        document.getElementById('datastore-modal-title').textContent = '创建新存储库';
        if (id) {
            document.getElementById('datastore-modal-title').textContent = `编辑存储库`;
            try {
                const datastore = await ApiService.getDatastore(id);
                document.getElementById('datastore-id').value = id;
                document.getElementById('datastore-name').value = datastore.name;
                document.getElementById('datastore-type').value = datastore.type;
                document.getElementById('datastore-visible').checked = datastore.visible;
                document.getElementById('datastore-allow-concurrent-push').checked = datastore.allow_concurrent_push;
                document.getElementById('datastore-session-timeout-seconds').value = datastore.session_timeout_seconds;
                document.getElementById('datastore-meta').value = datastore.meta ? JSON.stringify(datastore.meta, null, 2) : '';
            } catch(error) { alert(`加载存储库数据失败: ${error.message}`); return; }
        }
        modal.show();
    },

    async saveDatastore() {
        // ... (existing code, no changes needed)
        const id = document.getElementById('datastore-id').value ? parseInt(document.getElementById('datastore-id').value, 10) : null;
        let meta;
        try { meta = document.getElementById('datastore-meta').value ? JSON.parse(document.getElementById('datastore-meta').value) : null; }
        catch(e) { alert('元数据格式不正确，必须是有效的JSON。'); return; }
        const data = {
            name: document.getElementById('datastore-name').value,
            type: document.getElementById('datastore-type').value,
            visible: document.getElementById('datastore-visible').checked,
            allow_concurrent_push: document.getElementById('datastore-allow-concurrent-push').checked,
            session_timeout_seconds: parseInt(document.getElementById('datastore-session-timeout-seconds').value, 10),
            meta: meta
        };
        try {
            if (id) { await ApiService.updateDatastore(id, data); } else { await ApiService.createDatastore(data); }
            await this.renderDatastores();
            bootstrap.Modal.getInstance(document.getElementById('datastore-modal')).hide();
        } catch (error) { console.error("Error saving datastore:", error); alert(`保存失败: ${error.message}`); }
    },

    async renderKeys() {
        // ... (existing code for renderKeys, no changes needed)
        const content = document.getElementById('keys-content');
        const emptyState = document.getElementById('keys-empty-state');
        const [keys, datastores] = await Promise.all([ApiService.getApiKeys(), ApiService.getDatastores()]);
        if (keys.length === 0) {
            content.classList.add('d-none');
            emptyState.classList.remove('d-none');
        } else {
            content.classList.remove('d-none');
            emptyState.classList.add('d-none');
            const datastoreMap = datastores.reduce((map, ds) => { map[ds.id] = ds.name; return map; }, {});
            document.getElementById('keys-table-body').innerHTML = keys.map(key => `
                <tr>
                    <td>${key.name}</td><td><code class="cursor-pointer" onclick="this.textContent='${key.key}'">${key.key.substring(0, 3)}...${key.key.slice(-4)}</code></td>
                    <td><a href="#" onclick="navigateTo('datastores'); return false;">${datastoreMap[key.datastore_id] || 'N/A'}</a></td>
                    <td>${new Date(key.created_at).toLocaleDateString()}</td>
                    <td class="text-end"><a href="#" class="btn btn-danger btn-sm" onclick="App.confirmDelete('key', ${key.id})">删除</a></td>
                </tr>`).join('');
        }
    },

    async openKeyModal() {
        // ... (existing code, no changes needed)
        try {
            const datastores = await ApiService.getDatastores();
            if(datastores.length === 0){ alert('请先创建一个存储库，然后才能创建API Key。'); navigateTo('datastores'); return; }
            document.getElementById('key-datastore-id').innerHTML = datastores.map(ds => `<option value="${ds.id}">${ds.name}</option>`).join('');
            document.getElementById('key-name').value = '';
            new bootstrap.Modal(document.getElementById('key-modal')).show();
        } catch (error) { alert(`加载存储库列表失败: ${error.message}`); }
    },

    async saveApiKey() {
        // ... (existing code, no changes needed)
        const data = { name: document.getElementById('key-name').value, datastore_id: parseInt(document.getElementById('key-datastore-id').value, 10) };
        try {
            const newKey = await ApiService.createApiKey(data);
            await this.renderKeys();
            bootstrap.Modal.getInstance(document.getElementById('key-modal')).hide();
            alert(`API Key 创建成功！\n\n请立即复制并保存您的 Key，它将不会再次显示。\n\n${newKey.key}`);
        } catch (error) { console.error("Error saving API key:", error); alert(`创建失败: ${error.message}`); }
    },
    
    async renderProfile() {
        // ... (existing code for renderProfile, no changes needed)
        const profileForm = document.getElementById('profile-update-form'), passwordForm = document.getElementById('password-update-form');
        if (!profileForm || !passwordForm) { throw new Error("页面模板加载不正确。"); }
        profileForm.addEventListener('submit', e => { e.preventDefault(); this.updateProfile(); });
        passwordForm.addEventListener('submit', e => { e.preventDefault(); this.updatePassword(); });
        const user = await ApiService.getProfile();
        document.getElementById('profile-name').value = user.name;
        document.getElementById('profile-email').value = user.email;
        document.getElementById('profile-level').value = user.level;
    },

    async updateProfile() {
        // ... (existing code, no changes needed)
         const data = { name: document.getElementById('profile-name').value };
         try {
            const updatedUser = await ApiService.updateProfile(data);
            document.getElementById('user-name-display').textContent = updatedUser.name;
            alert('个人信息更新成功！');
         } catch(error) { alert(`更新失败: ${error.message}`); }
    },

    async updatePassword() {
        // ... (existing code, no changes needed)
        const oldPassword = document.getElementById('old-password').value, newPassword = document.getElementById('new-password').value, confirmPassword = document.getElementById('confirm-password').value;
        if (newPassword !== confirmPassword) { alert('新密码和确认密码不匹配！'); return; }
        if(newPassword.length < 8) { alert('新密码长度不能少于8位！'); return; }
        const data = { old_password: oldPassword, password: newPassword };
        try {
            await ApiService.changePassword(data);
            alert('密码修改成功！');
            document.getElementById('password-update-form').reset();
        } catch(error) { alert(`修改失败: ${error.message}`); }
    },

    async renderUsers() {
        if (this.currentUser.level !== 'admin') {
            document.getElementById('page-content').innerHTML = `<div class="alert alert-danger">无权访问</div>`;
            return;
        }

        const content = document.getElementById('users-content');
        const emptyState = document.getElementById('users-empty-state');
        const users = await ApiService.listUsers();

        // Filter out the current admin user from the list to prevent self-deletion
        const otherUsers = users.filter(user => user.id !== this.currentUser.id);

        if (otherUsers.length === 0) {
            content.classList.add('d-none');
            emptyState.classList.remove('d-none');
        } else {
            content.classList.remove('d-none');
            emptyState.classList.add('d-none');
            document.getElementById('users-table-body').innerHTML = otherUsers.map(user => `
                <tr>
                    <td>${user.name}</td>
                    <td>${user.email}</td>
                    <td><span class="badge bg-purple-lt">${user.level}</span></td>
                    <td>${new Date(user.created_at).toLocaleDateString()}</td>
                    <td class="text-end">
                        <a href="#" class="btn btn-danger btn-sm" onclick="App.confirmDelete('user', ${user.id})">删除</a>
                    </td>
                </tr>`).join('');
        }
    },

    openUserModal() {
        document.getElementById('user-create-name').value = '';
        document.getElementById('user-create-email').value = '';
        document.getElementById('user-create-password').value = '';
        new bootstrap.Modal(document.getElementById('user-modal')).show();
    },

    async saveUser() {
        const data = {
            name: document.getElementById('user-create-name').value,
            email: document.getElementById('user-create-email').value,
            password: document.getElementById('user-create-password').value,
        };

        if (!data.name || !data.email || !data.password) {
            alert('所有字段均为必填项。');
            return;
        }

        try {
            await ApiService.createUser(data);
            await this.renderUsers();
            bootstrap.Modal.getInstance(document.getElementById('user-modal')).hide();
            alert('新用户创建成功！');
        } catch (error) {
            alert(`创建失败: ${error.message}`);
        }
    },

    confirmDelete(type, id) {
        const text = document.getElementById('delete-confirm-text');
        if (type === 'datastore') {
            text.textContent = '确定要删除这个存储库吗？所有关联的 API Key 也会被一并删除。此操作无法恢复。';
            this.deleteCallback = async () => {
                await ApiService.deleteDatastore(id);
                await this.renderDatastores();
            };
        } else if (type === 'key') {
            text.textContent = '确定要删除这个 API Key 吗？此操作无法恢复。';
            this.deleteCallback = async () => {
                await ApiService.deleteApiKey(id);
                await this.renderKeys();
            };
        } else if (type === 'user') {
            text.textContent = '确定要删除这个用户吗？此操作无法恢复。';
            this.deleteCallback = async () => {
                await ApiService.deleteUser(id);
                await this.renderUsers();
            };
        }
        new bootstrap.Modal(document.getElementById('delete-confirm-modal')).show();
    },

    async executeDelete() {
        if (typeof this.deleteCallback === 'function') {
            try {
                await this.deleteCallback();
                if(window.location.hash.includes('dashboard')) await this.renderDashboard();
            } catch(error) {
                alert(`删除失败: ${error.message}`);
            }
        }
        bootstrap.Modal.getInstance(document.getElementById('delete-confirm-modal')).hide();
        this.deleteCallback = null;
    }
};

// --- INITIALIZATION ---
document.addEventListener('DOMContentLoaded', () => {
    App.init();
    
    window.onpopstate = function(event) {
        if (localStorage.getItem('authToken')) {
            const page = (event.state && event.state.page) || 'dashboard';
            // We re-render page, so no need to call navigateTo which calls renderPage again
             App.renderPage(page);
        }
    };
});
