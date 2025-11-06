const UI = {
    init() {
        this.injectTemplates();
        this.injectModals();
    },

    showAdminFeatures(show = true) {
        const userManagementLink = document.getElementById('user-management-link');
        if (userManagementLink) {
            if (show) {
                userManagementLink.classList.remove('d-none');
            } else {
                userManagementLink.classList.add('d-none');
            }
        }
    },

    injectTemplates() {
        const container = document.getElementById('templates-container');
        if (!container) return;
        container.innerHTML = `
            <template id="dashboard-template">
                <div class="page-header">
                    <h2 class="page-title">仪表盘</h2>
                </div>
                <div class="row row-deck row-cards">
                    <div class="col-sm-6 col-lg-3">
                        <div class="card">
                            <div class="card-body">
                                <div class="d-flex align-items-center">
                                    <div class="subheader">存储库</div>
                                </div>
                                <div class="h1 mb-3" id="datastore-count">0</div>
                                <a href="#" onclick="navigateTo('datastores'); return false;" class="btn btn-primary w-100">管理存储库</a>
                            </div>
                        </div>
                    </div>
                    <div class="col-sm-6 col-lg-3">
                        <div class="card">
                            <div class="card-body">
                                <div class="d-flex align-items-center">
                                    <div class="subheader">API Keys</div>
                                </div>
                                <div class="h1 mb-3" id="key-count">0</div>
                                <a href="#" onclick="navigateTo('keys'); return false;" class="btn btn-primary w-100">管理 API Keys</a>
                            </div>
                        </div>
                    </div>
                </div>
            </template>
    
            <template id="datastores-template">
                 <div class="page-header">
                    <div class="row align-items-center">
                        <div class="col">
                            <h2 class="page-title">存储库管理</h2>
                            <div class="text-muted mt-1">管理您的所有数据存储库</div>
                        </div>
                        <div class="col-auto ms-auto d-print-none">
                            <div class="btn-list">
                                <a href="#" class="btn btn-primary" onclick="App.openDatastoreModal()">
                                    <svg xmlns="http://www.w3.org/2000/svg" class="icon" width="24" height="24" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor" fill="none" stroke-linecap="round" stroke-linejoin="round"><path stroke="none" d="M0 0h24v24H0z" fill="none"/><line x1="12" y1="5" x2="12" y2="19" /><line x1="5" y1="12" x2="19" y2="12" /></svg>
                                    创建新存储库
                                </a>
                            </div>
                        </div>
                    </div>
                </div>
                <div id="datastores-content">
                    <div class="card">
                        <div class="table-responsive">
                            <table class="table table-vcenter card-table">
                                <thead>
                                    <tr>
                                        <th>名称</th>
                                        <th>类型</th>
                                        <th>可见性</th>
                                        <th>创建时间</th>
                                        <th class="w-1"></th>
                                    </tr>
                                </thead>
                                <tbody id="datastores-table-body"></tbody>
                            </table>
                        </div>
                    </div>
                </div>
                <div class="d-none" id="datastores-empty-state">
                     <div class="empty">
                        <div class="empty-icon">
                          <svg xmlns="http://www.w3.org/2000/svg" class="icon" width="24" height="24" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor" fill="none" stroke-linecap="round" stroke-linejoin="round"><path stroke="none" d="M0 0h24v24H0z" fill="none"></path><rect x="3" y="4" width="18" height="8" rx="3"></rect><rect x="3" y="12" width="18" height="8" rx="3"></rect><line x1="7" y1="8" x2="7" y2="8.01"></line><line x1="7" y1="16" x2="7" y2="16.01"></line></svg>
                        </div>
                        <p class="empty-title">没有找到存储库</p>
                        <p class="empty-subtitle text-muted">点击下面的按钮开始创建您的第一个数据存储库。</p>
                        <div class="empty-action">
                          <a href="#" class="btn btn-primary" onclick="App.openDatastoreModal()">
                            <svg xmlns="http://www.w3.org/2000/svg" class="icon" width="24" height="24" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor" fill="none" stroke-linecap="round" stroke-linejoin="round"><path stroke="none" d="M0 0h24v24H0z" fill="none"></path><line x1="12" y1="5" x2="12" y2="19"></line><line x1="5" y1="12" x2="19" y2="12"></line></svg>
                            创建新存储库
                          </a>
                        </div>
                      </div>
                </div>
            </template>
    
            <template id="keys-template">
                <div class="page-header">
                     <div class="row align-items-center">
                        <div class="col">
                            <h2 class="page-title">API Key 管理</h2>
                             <div class="text-muted mt-1">管理您的所有 API Keys</div>
                        </div>
                        <div class="col-auto ms-auto d-print-none">
                             <a href="#" class="btn btn-primary" onclick="App.openKeyModal()">
                                <svg xmlns="http://www.w3.org/2000/svg" class="icon" width="24" height="24" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor" fill="none" stroke-linecap="round" stroke-linejoin="round"><path stroke="none" d="M0 0h24v24H0z" fill="none"/><line x1="12" y1="5" x2="12" y2="19" /><line x1="5" y1="12" x2="19" y2="12" /></svg>
                                创建新 Key
                            </a>
                        </div>
                    </div>
                </div>
                <div id="keys-content">
                    <div class="card">
                        <div class="table-responsive">
                            <table class="table table-vcenter card-table">
                                <thead>
                                    <tr>
                                        <th>名称</th>
                                        <th>Key (点击显示)</th>
                                        <th>关联存储库</th>
                                        <th>创建时间</th>
                                        <th class="w-1"></th>
                                    </tr>
                                </thead>
                                <tbody id="keys-table-body"></tbody>
                            </table>
                        </div>
                    </div>
                </div>
                <div class="d-none" id="keys-empty-state">
                    <div class="empty">
                        <div class="empty-icon">
                            <svg xmlns="http://www.w3.org/2000/svg" class="icon" width="24" height="24" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor" fill="none" stroke-linecap="round" stroke-linejoin="round"><path stroke="none" d="M0 0h24v24H0z" fill="none"></path><path d="M15 8a5 5 0 0 1 -5 5h-1a3.52 3.52 0 0 0 -3.5 3.5v.5a3.5 3.5 0 0 0 7 0v-.5a2 2 0 0 1 2 -2h1a5 5 0 0 1 0 -10z"></path></svg>
                        </div>
                        <p class="empty-title">没有找到 API Key</p>
                        <p class="empty-subtitle text-muted">点击下面的按钮来创建您的第一个 API Key。</p>
                        <div class="empty-action">
                            <a href="#" class="btn btn-primary" onclick="App.openKeyModal()">
                                <svg xmlns="http://www.w3.org/2000/svg" class="icon" width="24" height="24" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor" fill="none" stroke-linecap="round" stroke-linejoin="round"><path stroke="none" d="M0 0h24v24H0z" fill="none"></path><line x1="12" y1="5" x2="12" y2="19"></line><line x1="5" y1="12" x2="19" y2="12"></line></svg>
                                创建新 Key
                            </a>
                        </div>
                    </div>
                </div>
            </template>
            
            <template id="profile-template">
                <div class="page-header">
                    <h2 class="page-title">个人信息</h2>
                </div>
                <div class="row g-4">
                    <div class="col-md-6">
                        <div class="card">
                            <div class="card-header">
                                <h3 class="card-title">基础信息</h3>
                            </div>
                            <div class="card-body">
                                <form id="profile-update-form">
                                    <div class="mb-3">
                                        <label class="form-label">姓名</label>
                                        <input type="text" class="form-control" id="profile-name">
                                    </div>
                                    <div class="mb-3">
                                        <label class="form-label">邮箱地址</label>
                                        <input type="email" class="form-control" id="profile-email" disabled>
                                    </div>
                                    <div class="mb-3">
                                        <label class="form-label">账户等级</label>
                                        <input type="text" class="form-control" id="profile-level" disabled>
                                    </div>
                                    <div class="form-footer">
                                        <button type="submit" class="btn btn-primary">保存更改</button>
                                    </div>
                                </form>
                            </div>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="card">
                            <div class="card-header">
                                <h3 class="card-title">修改密码</h3>
                            </div>
                            <div class="card-body">
                                <form id="password-update-form">
                                     <div class="mb-3">
                                        <label class="form-label">旧密码</label>
                                        <input type="password" class="form-control" id="old-password" placeholder="输入您的旧密码">
                                    </div>
                                     <div class="mb-3">
                                        <label class="form-label">新密码</label>
                                        <input type="password" class="form-control" id="new-password" placeholder="输入您的新密码">
                                    </div>
                                     <div class="mb-3">
                                        <label class="form-label">确认新密码</label>
                                        <input type="password" class="form-control" id="confirm-password" placeholder="再次输入新密码">
                                    </div>
                                     <div class="form-footer">
                                        <button type="submit" class="btn btn-primary">修改密码</button>
                                    </div>
                                </form>
                            </div>
                        </div>
                    </div>
                </div>
            </template>

            <template id="users-template">
                <div class="page-header">
                    <div class="row align-items-center">
                        <div class="col">
                            <h2 class="page-title">用户管理</h2>
                            <div class="text-muted mt-1">创建、查看和删除系统中的用户</div>
                        </div>
                        <div class="col-auto ms-auto d-print-none">
                            <a href="#" class="btn btn-primary" onclick="App.openUserModal()">
                                <svg xmlns="http://www.w3.org/2000/svg" class="icon" width="24" height="24" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor" fill="none" stroke-linecap="round" stroke-linejoin="round"><path stroke="none" d="M0 0h24v24H0z" fill="none"></path><line x1="12" y1="5" x2="12" y2="19"></line><line x1="5" y1="12" x2="19" y2="12"></line></svg>
                                创建新用户
                            </a>
                        </div>
                    </div>
                </div>
                <div id="users-content">
                    <div class="card">
                        <div class="table-responsive">
                            <table class="table table-vcenter card-table">
                                <thead>
                                    <tr>
                                        <th>姓名</th>
                                        <th>邮箱</th>
                                        <th>角色</th>
                                        <th>创建时间</th>
                                        <th class="w-1"></th>
                                    </tr>
                                </thead>
                                <tbody id="users-table-body"></tbody>
                            </table>
                        </div>
                    </div>
                </div>
                <div class="d-none" id="users-empty-state">
                    <div class="empty">
                        <div class="empty-icon"><svg xmlns="http://www.w3.org/2000/svg" class="icon" width="24" height="24" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor" fill="none" stroke-linecap="round" stroke-linejoin="round"><path stroke="none" d="M0 0h24v24H0z" fill="none"></path><circle cx="9" cy="7" r="4"></circle><path d="M3 21v-2a4 4 0 0 1 4 -4h4a4 4 0 0 1 4 4v2"></path><path d="M16 3.13a4 4 0 0 1 0 7.75"></path><path d="M21 21v-2a4 4 0 0 0 -3 -3.85"></path></svg>
                        </div>
                        <p class="empty-title">没有其他用户</p>
                        <p class="empty-subtitle text-muted">点击按钮来创建一个新用户。</p>
                        <div class="empty-action">
                            <a href="#" class="btn btn-primary" onclick="App.openUserModal()">
                                <svg xmlns="http://www.w3.org/2000/svg" class="icon" width="24" height="24" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor" fill="none" stroke-linecap="round" stroke-linejoin="round"><path stroke="none" d="M0 0h24v24H0z" fill="none"></path><line x1="12" y1="5" x2="12" y2="19"></line><line x1="5" y1="12" x2="19" y2="12"></line></svg>
                                创建新用户
                            </a>
                        </div>
                    </div>
                </div>
            </template>
        `;
    },

    injectModals() {
        const container = document.getElementById('modals-container');
        if (!container) return;
        container.innerHTML = `
            <!-- Datastore Modal -->
            <div class="modal modal-blur fade" id="datastore-modal" tabindex="-1" role="dialog" aria-hidden="true">
                <div class="modal-dialog modal-lg modal-dialog-centered" role="document">
                    <div class="modal-content">
                        <div class="modal-header">
                            <h5 class="modal-title" id="datastore-modal-title">创建新存储库</h5>
                            <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                        </div>
                        <div class="modal-body">
                            <input type="hidden" id="datastore-id">
                            <div class="mb-3"><label class="form-label">存储库名称</label><input type="text" class="form-control" id="datastore-name" placeholder="例如：我的项目数据"></div>
                            <div class="mb-3"><label class="form-label">存储库类型</label><select class="form-select" id="datastore-type"><option value="汇交存储库">汇交存储库</option><option value="检索存储库">检索存储库</option></select></div>
                            <div class="mb-3"><label class="form-check"><input class="form-check-input" type="checkbox" id="datastore-visible"><span class="form-check-label">对公众可见</span></label></div>
                            <div class="mb-3"><label class="form-check"><input class="form-check-input" type="checkbox" id="datastore-allow-concurrent-push"><span class="form-check-label">允许并发推送</span></label></div>
                            <div class="mb-3"><label class="form-label">会话超时 (秒)</label><input type="number" class="form-control" id="datastore-session-timeout-seconds" value="30"></div>
                            <div class="mb-3"><label class="form-label">元数据 (JSON格式)</label><textarea class="form-control" id="datastore-meta" rows="3" placeholder='{ "description": "这是一个描述" }'></textarea></div>
                        </div>
                        <div class="modal-footer"><button type="button" class="btn btn-link link-secondary" data-bs-dismiss="modal">取消</button><button type="button" id="save-datastore-btn" class="btn btn-primary ms-auto">保存</button></div>
                    </div>
                </div>
            </div>
        
            <!-- API Key Modal -->
            <div class="modal modal-blur fade" id="key-modal" tabindex="-1" role="dialog" aria-hidden="true">
                <div class="modal-dialog modal-dialog-centered" role="document">
                    <div class="modal-content">
                        <div class="modal-header"><h5 class="modal-title">创建新 API Key</h5><button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button></div>
                        <div class="modal-body">
                             <div class="mb-3"><label class="form-label">Key 名称</label><input type="text" class="form-control" id="key-name" placeholder="例如：我的应用 Key"></div>
                            <div class="mb-3"><label class="form-label">关联存储库</label><select class="form-select" id="key-datastore-id"></select></div>
                        </div>
                        <div class="modal-footer"><button type="button" class="btn me-auto" data-bs-dismiss="modal">取消</button><button type="button" id="save-key-btn" class="btn btn-primary">创建 Key</button></div>
                    </div>
                </div>
            </div>
        
            <!-- Delete Confirmation Modal -->
            <div class="modal modal-blur fade" id="delete-confirm-modal" tabindex="-1" role="dialog" aria-hidden="true">
                <div class="modal-dialog modal-sm modal-dialog-centered" role="document">
                    <div class="modal-content">
                        <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                        <div class="modal-status bg-danger"></div>
                        <div class="modal-body text-center py-4">
                            <svg xmlns="http://www.w3.org/2000/svg" class="icon mb-2 text-danger icon-lg" width="24" height="24" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor" fill="none" stroke-linecap="round" stroke-linejoin="round"><path stroke="none" d="M0 0h24v24H0z" fill="none"/><path d="M12 9v2m0 4v.01" /><path d="M5 19h14a2 2 0 0 0 1.84 -2.75l-7.1 -12.25a2 2 0 0 0 -3.5 0l-7.1 12.25a2 2 0 0 0 1.75 2.75" /></svg>
                            <h3>确认删除？</h3>
                            <div class="text-muted" id="delete-confirm-text">此操作不可逆，将会永久删除该项目。</div>
                        </div>
                        <div class="modal-footer">
                            <div class="w-100"><div class="row"><div class="col"><a href="#" class="btn w-100" data-bs-dismiss="modal">取消</a></div><div class="col"><a href="#" id="confirm-delete-btn" class="btn btn-danger w-100">确认删除</a></div></div></div>
                        </div>
                    </div>
                </div>
            </div>

            <!-- User Modal -->
            <div class="modal modal-blur fade" id="user-modal" tabindex="-1" role="dialog" aria-hidden="true">
                <div class="modal-dialog modal-dialog-centered" role="document">
                    <div class="modal-content">
                        <div class="modal-header"><h5 class="modal-title">创建新用户</h5><button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button></div>
                        <div class="modal-body">
                            <div class="mb-3"><label class="form-label">姓名</label><input type="text" class="form-control" id="user-create-name" placeholder="用户姓名"></div>
                            <div class="mb-3"><label class="form-label">邮箱地址</label><input type="email" class="form-control" id="user-create-email" placeholder="user@example.com"></div>
                            <div class="mb-3"><label class="form-label">密码</label><input type="password" class="form-control" id="user-create-password" placeholder="设置初始密码"></div>
                        </div>
                        <div class="modal-footer"><button type="button" class="btn me-auto" data-bs-dismiss="modal">取消</button><button type="button" id="save-user-btn" class="btn btn-primary">创建用户</button></div>
                    </div>
                </div>
            </div>
        `;
    }
};
