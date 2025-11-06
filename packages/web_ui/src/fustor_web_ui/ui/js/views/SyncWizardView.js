import apiService from '../apiService.js';
import { showBusinessError, showSuccess, showWarning } from '../notification.js';
import { navigate } from '../navigation.js';
import stateStore from '../stateStore.js';

/**
 * REFACTORED: This class is now a self-contained, dynamic "shell" for the Sync Task wizard.
 * It fetches its structure from the backend and orchestrates all steps, rendering, and actions.
 */
export default class SyncWizardView {
    constructor(elementId) {
        this.el = document.getElementById(elementId);
        // The state manager for the current wizard session.
        this.wizardState = {
            context: {},
            wizardDefinition: null, // Will hold the definition from the backend
            configData: {},
            sourceSchema: {},
            pusherSchema: {},
            validationStatus: {},
            currentStep: 0,
            reset: function() {
                this.context = {};
                this.wizardDefinition = null;
                this.configData = {};
                this.sourceSchema = {};
                this.pusherSchema = {};
                this.validationStatus = {};
                this.currentStep = 0;
            }
        };

        this._boundHandleClick = this._handleClick.bind(this);
    }

    async onActivate() {
        this.wizardState.reset();
        const globalContext = stateStore.getState().wizardContext;
        if (!globalContext || globalContext.type !== 'syncs') {
            showBusinessError('无法启动同步向导：缺少有效的上下文。');
            navigate('sync-tasks');
            return;
        }
        
        this.wizardState.context = { ...globalContext };
        stateStore.setState({ wizardContext: null });

        try {
            // Fetch the entire wizard definition from the new backend endpoint.
            const wizardDefinition = await apiService.get('/api/configs/syncs/wizard');
            this.wizardState.wizardDefinition = wizardDefinition;
            
            if (this.wizardState.context.mode === 'edit') {
                const { appConfig } = stateStore.getState();
                const initialConfig = appConfig.syncs.root[this.wizardState.context.id];
                if (initialConfig) {
                    // Pre-populate config data for editing.
                    this.wizardState.configData = { ...initialConfig };
                }
            }

            this.renderShell();
            this.renderStep();
            this.attachEventListeners();

        } catch (error) {
            console.error('SyncWizardView: Failed to start wizard:', error);
            showBusinessError(`启动向导失败: ${error.message}`);
            navigate('sync-tasks');
        }
    }

    renderShell() {
        const title = this.wizardState.context.mode === 'edit' ?
            `克隆并编辑同步任务: ${this.wizardState.context.id}` : '添加新的同步任务';
        
        const editModeWarning = this.wizardState.context.mode === 'edit' ?
            `
            <div class="alert alert-warning" role="alert">
                <i class="ti ti-alert-triangle-filled me-2"></i>
                <strong>克隆并取代模式:</strong> 您正在编辑一个现有任务的副本。保存后，将创建一个新的同步任务，而旧的任务 <strong>(${this.wizardState.context.id})</strong> 将被自动禁用。
            </div>
        ` : '';

        this.el.innerHTML = `
            ${editModeWarning}
            <div class="card">
                <div class="card-header">
                    <h2 class="card-title">${title}</h2>
                </div>
                <div id="wizard-steps-container" class="card-header bg-light d-flex"></div>
                <div id="wizard-content-container" class="card-body p-4" style="min-height: 28rem;"></div>
                <div id="wizard-footer-container" class="card-footer d-flex align-items-center"></div>
            </div>`;
    }

    renderStep() {
        const contentContainer = this.el.querySelector('#wizard-content-container');
        const stepDef = this.wizardState.wizardDefinition.steps[this.wizardState.currentStep];
        
        contentContainer.innerHTML = `<h6>${stepDef.title}</h6><p class="text-muted">${stepDef.schema.description || ''}</p>`;
        
        if (stepDef.step_id === 'initial_selection' || stepDef.step_id === 'advanced_settings') {
            this._renderGenericForm(contentContainer, stepDef.schema);
        } else if (stepDef.step_id === 'field_mapping') {
            this._renderFieldMappingUI(contentContainer);
        }

        this._updateStepsUI();
        this._updateFooter();
    }

    _renderGenericForm(container, schema) {
        const formContainer = document.createElement('div');
        formContainer.className = 'row';
        
        for (const [key, prop] of Object.entries(schema.properties)) {
            const col = document.createElement('div');
            col.className = 'col-md-6 mb-3';
            const value = this.wizardState.configData[key] || prop.default || '';
            const required = (schema.required || []).includes(key);
            let fieldHtml = `<label class="form-label ${required ? 'required' : ''}">${prop.title}</label>`;
            if (prop.enum) { // Render as select dropdown
                const options = prop.enum.map(opt => `<option value="${opt}" ${opt === value ? 'selected' : ''}>${opt}</option>`).join('');
                fieldHtml += `<select class="form-select" data-key-path="${key}" ${required ? 'required' : ''}><option value="">请选择...</option>${options}</select>`;
            } else if (prop.type === 'boolean') { // Render as switch
                fieldHtml = `
                    <div class="form-check form-switch">
                        <input class="form-check-input" type="checkbox" data-key-path="${key}" ${value ? 'checked' : ''}>
                        <label class="form-check-label">${prop.title}</label>
                    </div>`;
            } else { // Render as text/number input
                 fieldHtml += `<input type="${prop.type === 'integer' ? 'number' : 'text'}" class="form-control" data-key-path="${key}" value="${value}" ${required ? 'required' : ''}>`;
            }
            if (prop.description) {
                fieldHtml += `<small class="form-hint">${prop.description}</small>`;
            }
            col.innerHTML = fieldHtml;
            formContainer.appendChild(col);
        }
        container.appendChild(formContainer);
    }
    
    _renderFieldMappingUI(container) {
        const validationResult = this.wizardState.validationStatus['fields_loaded'];
        
        let contentHtml = '';
        if (!validationResult) {
            contentHtml = `
                <div class="text-center p-5">
                    <div class="spinner-border" role="status"></div>
                    <p class="mt-2 text-muted">正在加载源和目标的字段信息...</p>
                </div>
            `;
        } else if (!validationResult.success) {
            contentHtml = `
                <div class="alert alert-danger">${validationResult.message}</div>
                <div class="text-center">
                    <button class="btn btn-primary" id="loadFieldsBtn">
                        <i class="ti ti-refresh me-1"></i> 重试
                    </button>
                </div>
            `;
        } else {
            const tempDiv = document.createElement('div');
            this._renderMappingGrids(tempDiv);
            contentHtml = tempDiv.innerHTML;
        }
    
        container.innerHTML += `<div id="field-mapping-content">${contentHtml}</div>`;

        if (validationResult && validationResult.success) {
            container.querySelectorAll('.multi-select-mapping').forEach(el => {
                new TomSelect(el, {
                    plugins: ['remove_button'],
                    create: false,
                });
            });
        }
    }

    _renderMappingGrids(container) {
        const sourceProperties = this.wizardState.sourceSchema?.properties || {};
        const pusherSchema = this.wizardState.pusherSchema || {};
        const pusherFields = Object.entries(pusherSchema.properties || {});
        const requiredPusherFields = pusherSchema.required || [];
        
        const currentMapping = this.wizardState.configData.fields_mapping || [];

        const mappingRowsHtml = pusherFields.map(([rFieldKey, rFieldProp]) => {
            const isRequired = requiredPusherFields.includes(rFieldKey);
            const mappingRule = currentMapping.find(m => m.to === rFieldKey);
            const selectedSources = mappingRule ? mappingRule.source : [];

            // --- START: Simplified Logic ---
            // Now that all drivers provide a column_index, the logic can be unconditional and simpler.
            const optionsWithSelection = Object.entries(sourceProperties).map(([key, prop]) => {
                const value = `${key}:${prop.column_index}`;
                const isSelected = selectedSources.includes(value);
                return `<option value="${value}" ${isSelected ? 'selected' : ''}>${key}</option>`;
            }).join('');
            // --- END: Simplified Logic ---

            return `
                <div class="row mb-3 align-items-center">
                    <div class="col-md-4">
                        <label class="form-label mb-0 ${isRequired ? 'required' : ''}" title="${rFieldKey}">
                            ${rFieldKey.split('.').pop()}
                        </label>
                        <small class="form-hint mt-0">${rFieldKey}</small>
                    </div>
                    <div class="col-md-1 text-center text-muted">
                        <i class="ti ti-arrow-left"></i>
                    </div>
                    <div class="col-md-7">
                        <select class="multi-select-mapping" data-map-to="${rFieldKey}" multiple>
                            ${optionsWithSelection}
                        </select>
                    </div>
                </div>
            `;
        }).join('');

        container.innerHTML = `
            <div class="row mb-3">
                <div class="col-md-4"><strong>目标字段 (Pusher)</strong></div>
                <div class="col-md-1"></div>
                <div class="col-md-7"><strong>来源字段 (Source)</strong></div>
            </div>
            <hr class="mt-0">
            ${mappingRowsHtml || '<p class="text-muted text-center">接收端未定义任何所需字段。</p>'}
        `;
    }

    _updateStepsUI() {
        const stepsContainer = this.el.querySelector('#wizard-steps-container');
        const steps = this.wizardState.wizardDefinition.steps;
        stepsContainer.innerHTML = steps.map((step, index) => {
            const isCompleted = index < this.wizardState.currentStep;
            const isActive = index === this.wizardState.currentStep;
            const stateClass = isActive ? 'text-primary fw-bold' : (isCompleted ? 'text-success' : 'text-muted');
            const badgeClass = isActive ? 'bg-primary' : (isCompleted ? 'bg-success' : 'bg-secondary');
            return `<div class="d-flex align-items-center p-2 ${stateClass}">
                        <span class="badge ${badgeClass}-lt me-2">${index + 1}</span>
                        <span>${step.title}</span>
                    </div>`;
        }).join('<div class="hr-text my-0" style="flex-grow: 1;"></div>');
    }

    _updateFooter() {
        const footerContainer = this.el.querySelector('#wizard-footer-container');
        const steps = this.wizardState.wizardDefinition.steps;
        const currentStepDef = steps[this.wizardState.currentStep];
        const isFirstStep = this.wizardState.currentStep === 0;
        const isLastStep = this.wizardState.currentStep === steps.length - 1;

        const requiredValidations = currentStepDef.validations || [];
        const allChecksPassed = requiredValidations.length === 0 || requiredValidations.every(key => this.wizardState.validationStatus[key]?.success);
        
        const nextButtonHtml = !isLastStep ?
            `<button class="btn btn-primary" id="nextStepBtn" ${!allChecksPassed ? 'disabled' : ''}>下一步 <i class="ti ti-arrow-right ms-1"></i></button>` : '';
        const saveButtonHtml = isLastStep ?
            `<button class="btn btn-success" id="saveConfigBtn"><i class="ti ti-check me-1"></i>保存配置</button>` : '';
        
        footerContainer.innerHTML = `
            <button class="btn" id="cancelBtn">取消</button>
            <div class="ms-auto btn-list">
                <button class="btn" id="prevStepBtn" ${isFirstStep ? 'disabled' : ''}><i class="ti ti-arrow-left me-1"></i>上一步</button>
                ${nextButtonHtml}
                ${saveButtonHtml}
            </div>`;
    }

    attachEventListeners() {
        this.el.addEventListener('click', this._boundHandleClick);
    }
    
    _handleClick(e) {
        const button = e.target.closest('button');
        if (!button) return;
        
        this.collectCurrentStepData();

        switch (button.id) {
            case 'prevStepBtn': return this.prevStep();
            case 'nextStepBtn': return this.nextStep();
            case 'saveConfigBtn': return this.handleSave(button);
            case 'cancelBtn': return navigate('sync-tasks');
            case 'loadFieldsBtn': return this.handleLoadFields();
        }
    }

    nextStep() {
        if (this.validateStep()) {
            const fromStep = this.wizardState.currentStep;
            this.wizardState.currentStep++;
            this.renderStep();

            if (fromStep === 0 && this.wizardState.currentStep === 1) {
                this.handleLoadFields();
            }
        }
    }

    prevStep() {
        this.wizardState.currentStep--;
        this.renderStep();
    }
    
    collectCurrentStepData() {
        this.el.querySelectorAll('[data-key-path]').forEach(input => {
            const key = input.dataset.keyPath;
            const value = input.type === 'checkbox' ? input.checked : input.value;
            this.wizardState.configData[key] = value;
        });

        const mappingSelects = this.el.querySelectorAll('.multi-select-mapping');
        if (mappingSelects.length > 0) {
            const newMappings = [];
            const requiredPusherFields = this.wizardState.pusherSchema?.required || [];
            mappingSelects.forEach(select => {
                const toField = select.dataset.mapTo;
                const sourceFields = select.tomselect ? select.tomselect.getValue() : [];

                if (sourceFields.length > 0) {
                    newMappings.push({
                        to: toField,
                        source: sourceFields,
                        required: requiredPusherFields.includes(toField)
                    });
                }
            });
            this.wizardState.configData.fields_mapping = newMappings;
        }
    }

    validateStep() {
        const stepDef = this.wizardState.wizardDefinition.steps[this.wizardState.currentStep];
        const schema = stepDef.schema;
        if (!schema.required) return true;

        for (const key of schema.required) {
            if (!this.wizardState.configData[key]) {
                showBusinessError(`字段 "${schema.properties[key].title}" 是必填项。`);
                return false;
            }
        }
        return true;
    }
    
    async handleLoadFields() {
        const { source, pusher } = this.wizardState.configData;
        if (!source || !pusher) {
            showWarning("请在第一步中选择一个数据源和接收端。");
            return;
        }

        try {
            const response = await apiService.post('/api/configs/syncs/_actions/load_fields_for_mapping', { source_id: source, pusher_id: pusher });
            this.wizardState.sourceSchema = response.source_schema;
            this.wizardState.pusherSchema = response.pusher_schema;
            const sourceFieldCount = Object.keys(response.source_schema?.properties || {}).length;
            const pusherFieldCount = Object.keys(response.pusher_schema?.properties || {}).length;
            this.wizardState.validationStatus['fields_loaded'] = { success: true, message: `加载成功! Source: ${sourceFieldCount} 字段, Pusher: ${pusherFieldCount} 字段.` };
            
            this._performSmartMapping();

        } catch (error) {
            this.wizardState.validationStatus['fields_loaded'] = { success: false, message: error.message };
        } finally {
            this.renderStep();
        }
    }

    _performSmartMapping() {
        const sourceProperties = this.wizardState.sourceSchema?.properties || {};
        const pusherProps = this.wizardState.pusherSchema?.properties || {};
        const requiredPusherFields = this.wizardState.pusherSchema?.required || [];
        const mapping = [];
        Object.keys(pusherProps).forEach(rField => {
            const matchingSource = Object.keys(sourceProperties).find(sField => {
                const sFieldEnd = sField.split('.').pop().toLowerCase();
                const rFieldEnd = rField.split('.').pop().toLowerCase();
                return sFieldEnd === rFieldEnd;
            });

            if (matchingSource) {
                const sourceProp = sourceProperties[matchingSource];
                const isRequired = requiredPusherFields.includes(rField);

                // --- START: Simplified Logic ---
                // Now that all drivers provide a column_index, the logic can be unconditional and simpler.
                const sourceValue = `${matchingSource}:${sourceProp.column_index}`;
                mapping.push({ to: rField, source: [sourceValue], required: isRequired });
                // --- END: Simplified Logic ---
            }
        });
        this.wizardState.configData.fields_mapping = mapping;
    }

    async handleSave(button) {
        if(!this.validateStep()) return;
        
        button.disabled = true;
        button.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>保存中...';
        
        const { id, ...configBody } = this.wizardState.configData;
        try {
            await apiService.addSyncConfig(id, configBody);
            if (this.wizardState.context.mode === 'edit' && this.wizardState.context.id) {
                await apiService.disableSyncConfig(this.wizardState.context.id);
                showSuccess(`新任务 '${id}' 已保存，旧任务 '${this.wizardState.context.id}' 已自动禁用。`);
            } else {
                showSuccess(`同步任务 '${id}' 保存成功！`);
            }
            navigate('sync-tasks');
        } catch (error) {
            showBusinessError(`保存同步任务失败: ${error.message}`);
            button.disabled = false;
            button.innerHTML = '<i class="ti ti-check me-1"></i>保存配置';
        }
    }

    onDeactivate() {
        this.el.removeEventListener('click', this._boundHandleClick);
        this.el.innerHTML = '';
        this.wizardState.reset();
    }
}