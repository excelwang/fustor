// src/fuagent/ui/js/views/WizardView.js

// --- START FIX: Corrected relative import paths ---
import stateStore from '../stateStore.js';
import { navigate } from '../navigation.js';
import { showBusinessError, showSuccess, showWarning } from '../notification.js';
import wizardState from './wizard/WizardState.js';
import apiService from '../apiService.js';
import { renderProperty, renderCredentialInputs, renderValidationSection, renderValidationStep, updateValidationStepUI, renderOneOfChoice } from './wizard/WizardUiComponents.js';
// --- END FIX ---


const ADVANCED_SETTINGS_STEP = {
    "step_id": "advanced_settings",
    "title": "高级参数",
    "schema": {
        "type": "object",
        "properties": {
            "max_queue_size": { "type": "integer", "title": "最大队列尺寸", "description": "内存中事件缓冲区的最大容量。", "default": 1000 },
            "max_retries": { "type": "integer", "title": "最大重试次数", "description": "读取事件失败时的最大重试次数。", "default": 10 },
            "retry_delay_sec": { "type": "integer", "title": "重试延迟 (秒)", "description": "每次重试前的等待秒数。", "default": 5 }
        }
    },
    "validations": []
};
export default class WizardView {
    constructor(elementId) {
        this.el = document.getElementById(elementId);
        this.activeStepModule = null;
        this.boundHandleClick = this.handleGeneralClick.bind(this);
        this.boundHandleChange = this.handleInputChange.bind(this);
    }
    async onActivate() {
        const context = stateStore.getState().wizardContext;
        if (!context || (context.type !== 'sources' && context.type !== 'pushers')) {
            showBusinessError('无法启动向导：缺少有效的上下文。');
            navigate('sources');
            return;
        }
        wizardState.initialize(context);
        try {
            const drivers = await apiService.listAvailableDrivers();
            wizardState.availableDrivers = drivers;
            this.renderShell();
            if (wizardState.context.mode === 'edit') {
                const appConfig = await apiService.getConfig();
                wizardState.configData = { ...appConfig[context.type][context.id] };
                wizardState.configData.id = '';
                wizardState.configData.idHint = `为克隆的配置输入一个全新的唯一ID。旧配置 '${context.id}' 将被禁用。`;
                await this.startWizardForDriver(wizardState.configData.driver);
            } else {
                wizardState.configData.idHint = '为新配置指定一个唯一的、易于识别的名称。';
                this.renderDriverSelectionStep();
            }
            this.attachEventListeners();
        } catch (error) {
            console.error('WizardView: Failed to start wizard:', error);
            showBusinessError(`启动向导失败: ${error.message}`);
            navigate('sources');
        }
    }
    
    async startWizardForDriver(driverType) {
        if (!driverType) {
            this.renderDriverSelectionStep();
            return;
        }
        wizardState.configData.driver = driverType;
        wizardState.wizardDefinition = null;
        wizardState.validationStatus = {};
        wizardState.currentStep = 0;
        const stepContentContainer = this.el.querySelector('#wizard-step-content-container');
        const driverSelectionContainer = this.el.querySelector('#wizard-driver-selection-container');
        stepContentContainer.innerHTML = '<div class="text-center p-5"><div class="spinner-border"></div><p class="mt-2">正在加载驱动配置向导...</p></div>';
        if(driverSelectionContainer) driverSelectionContainer.style.display = 'none';
        try {
            let wizardDefinition;
            if (wizardState.context.type === 'sources') {
                wizardDefinition = await apiService.getSourceWizardDefinition(driverType);
                wizardDefinition.steps.push(ADVANCED_SETTINGS_STEP);
            } else {
                wizardDefinition = await apiService.getPusherWizardDefinition(driverType);
            }
            wizardState.wizardDefinition = wizardDefinition;
            wizardState.openApiSchema = wizardDefinition;
            
            const credentialStepDef = wizardDefinition.steps.find(s => s.schema?.properties?.credential);
            if (credentialStepDef) {
                const firstChoice = credentialStepDef.schema.properties.credential.oneOf[0];
                wizardState.credentialChoice = firstChoice.$ref.split('/').pop();
            }

            this.renderStep();
        } catch (error) {
            console.error(`Failed to fetch wizard definition for driver ${driverType}:`, error);
            showBusinessError(`加载驱动 '${driverType}' 的配置向导失败: ${error.message}`);
            if(stepContentContainer) {
                stepContentContainer.innerHTML = `<div class="alert alert-danger">${error.message}</div>`;
            }
        }
    }

    renderShell() {
        const modeText = wizardState.context.mode === 'edit' ?
            `克隆并编辑: ${wizardState.context.id}` : '添加新的';
        const typeText = wizardState.context.type === 'sources' ? 'Source' : '接收端';
        const editModeWarning = wizardState.context.mode === 'edit' ? `
            <div class="alert alert-warning" role="alert">
                <i class="ti ti-alert-triangle-filled me-2"></i>
                <strong>克隆并取代模式:</strong> 您正在编辑一个现有配置的副本。保存后，将创建一个新的配置，而旧的配置 <strong>(${wizardState.context.id})</strong> 将被自动禁用。
            </div>
        ` : '';
        this.el.innerHTML = `
            <div class="page-header">
                <h1 class="page-title">${modeText} ${typeText} 配置</h1>
            </div>
            ${editModeWarning}
            <div class="card">
                <div id="wizard-top-container">
                    <div id="wizard-id-container" class="card-body border-bottom"></div>
                    <div id="wizard-driver-selection-container" class="card-body border-bottom"></div>
                </div>
                <div id="wizard-steps-container" class="card-header bg-light d-flex"></div>
                <div id="wizard-step-content-container" class="card-body p-4" style="min-height: 22rem;"></div>
                <div id="wizard-footer-container" class="card-footer d-flex align-items-center"></div>
            </div>`;
    }

    renderDriverSelectionStep() {
        const driverContainer = this.el.querySelector('#wizard-driver-selection-container');
        const stepContentContainer = this.el.querySelector('#wizard-step-content-container');
        const footerContainer = this.el.querySelector('#wizard-footer-container');
        const idContainer = this.el.querySelector('#wizard-id-container');
        const type = wizardState.context.type;
        const drivers = wizardState.availableDrivers[type] || [];
        const optionsHtml = drivers.map(d => `<option value="${d}">${d}</option>`).join('');
        if (idContainer) idContainer.style.display = 'none';
        driverContainer.style.display = 'block';
        driverContainer.innerHTML = `
            <div class="row">
                <div class="col-md-6">
                    <label for="driverTypeSelect" class="form-label">选择驱动类型</label>
                    <select id="driverTypeSelect" class="form-select">
                        <option value="">请选择...</option>
                        ${optionsHtml}
                    </select>
                    <small class="form-hint">选择要配置的数据驱动类型。</small>
                </div>
            </div>
        `;
        stepContentContainer.innerHTML = '';
        footerContainer.innerHTML = `<button class="btn" id="cancelBtn">取消</button>`;
        this._updateStepsUI();
    }
    renderStep() {
        const stepContentContainer = this.el.querySelector('#wizard-step-content-container');
        stepContentContainer.innerHTML = '';
        if (!wizardState.wizardDefinition || !wizardState.wizardDefinition.steps) {
            return;
        }
        const idContainer = this.el.querySelector('#wizard-id-container');
        if (idContainer) {
            if (wizardState.currentStep === 0) {
                 const configId = wizardState.configData.id ||
                    '';
                 const idHint = wizardState.configData.idHint || '';
                 idContainer.innerHTML = `
                    <div class="row">
                        <div class="col-md-6">
                            <label for="wizard-config-id" class="form-label required">配置 ID</label>
                            <input type="text" id="wizard-config-id" class="form-control" data-key-path="id" value="${configId}" required>
                            <small class="form-hint">${idHint}</small>
                        </div>
                    </div>`;
                idContainer.style.display = 'block';
            } else {
                idContainer.style.display = 'none';
            }
        }
        const stepDef = wizardState.wizardDefinition.steps[wizardState.currentStep];
        if (!stepDef) {
            showBusinessError("向导步骤定义无效。");
            return;
        }
        this._renderFormProperties(stepContentContainer, stepDef.schema, wizardState.configData);
        if (stepDef.validations && stepDef.validations.length > 0) {
            const validationContainer = document.createElement('div');
            stepContentContainer.appendChild(validationContainer);
            const resultsList = renderValidationSection(validationContainer, {
                title: "步骤校验",
                hint: "点击下方的“执行校验”按钮来验证您输入的参数。"
            });
            stepDef.validations.forEach(validationKey => {
                renderValidationStep(resultsList, validationKey, `执行校验: ${validationKey}`);
                const status = wizardState.validationStatus[validationKey];
                if(status) {
                    updateValidationStepUI(resultsList, validationKey, status.success, status.message);
                }
            });
        }
        this._updateStepsUI();
        this._updateFooter();
    }
    _renderFormProperties(parentElement, schema, configData, basePath = '') {
        if (!schema || !schema.properties) return;
        for (const [key, prop] of Object.entries(schema.properties)) {
            const currentPath = basePath ?
                `${basePath}.${key}` : key;
            const isRequired = (schema.required || []).includes(key);
            if (prop.type === 'object' && prop.properties && !prop.$ref) {
                const fieldset = document.createElement('div');
                fieldset.className = 'mb-3';
                fieldset.innerHTML = `<hr><h6>${prop.title || key}</h6>`;
                if (prop.description) {
                    fieldset.innerHTML += `<p class="form-hint mt-1">${prop.description}</p>`;
                }
                this._renderFormProperties(fieldset, prop, configData, currentPath);
                parentElement.appendChild(fieldset);
            } else if (prop.$ref) {
                const refName = prop.$ref.split('/').pop();
                const refSchema = wizardState.getRefSchema(refName);
                renderCredentialInputs(currentPath, { ...refSchema, ...prop }, configData, parentElement);
            } else {
                renderProperty(currentPath, prop, configData, parentElement, isRequired);
            }
        }
    }
    _updateStepsUI() {
        const stepsContainer = this.el.querySelector('#wizard-steps-container');
        if (!stepsContainer) return;
        const steps = wizardState.wizardDefinition?.steps || [];
        if (steps.length === 0) {
            stepsContainer.innerHTML = '';
            return;
        }
        stepsContainer.innerHTML = steps.map((step, index) => {
            const isCompleted = index < wizardState.currentStep;
            const isActive = index === wizardState.currentStep;
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
        if (!footerContainer || !wizardState.wizardDefinition) {
             if(footerContainer) footerContainer.innerHTML = `<button class="btn" id="cancelBtn">取消</button>`;
            return;
        }
        const steps = wizardState.wizardDefinition.steps;
        const currentStepDef = steps[wizardState.currentStep];
        const isFirstStep = wizardState.currentStep === 0;
        const isLastStep = wizardState.currentStep === steps.length - 1;
        const requiredValidations = currentStepDef.validations || [];
        const allChecksPassed = wizardState.areAllValidationsSuccessful(requiredValidations);
        const validationButtonHtml = !allChecksPassed && requiredValidations.length > 0
            ?
            '<button class="btn btn-primary" id="runValidationsBtn">执行校验</button>'
            : '';
        const nextButtonHtml = allChecksPassed && !isLastStep 
            ?
            '<button class="btn btn-primary" id="nextStepBtn">下一步 <i class="ti ti-arrow-right ms-1"></i></button>' 
            : '';
        const saveButtonHtml = allChecksPassed && isLastStep 
            ?
            '<button class="btn btn-success" id="saveConfigBtn"><i class="ti ti-check me-1"></i>保存配置</button>' 
            : '';
        footerContainer.innerHTML = `
            <button class="btn" id="cancelBtn">取消</button>
            <div class="ms-auto btn-list">
                <button class="btn" id="prevStepBtn" ${isFirstStep ?
            'disabled' : ''}><i class="ti ti-arrow-left me-1"></i>上一步</button>
                ${validationButtonHtml}
                ${nextButtonHtml}
                ${saveButtonHtml}
            </div>`;
    }

    attachEventListeners() {
        this.el.removeEventListener('click', this.boundHandleClick);
        this.el.addEventListener('click', this.boundHandleClick);
        
        this.el.removeEventListener('change', this.boundHandleChange);
        this.el.addEventListener('change', this.boundHandleChange);
        
        const driverSelect = this.el.querySelector('#driverTypeSelect');
        if (driverSelect) {
            driverSelect.addEventListener('change', (e) => this.startWizardForDriver(e.target.value));
        }
    }

    handleInputChange(e) {
        const selector = e.target.closest('[data-oneof-selector-for="credential"]');
        if (selector) {
            const basePath = selector.dataset.oneofSelectorFor;
            const choiceIndex = parseInt(selector.value, 10);
            
            const stepDef = wizardState.wizardDefinition.steps.find(s => s.schema?.properties?.credential);
            if (stepDef) {
                const choice = stepDef.schema.properties.credential.oneOf[choiceIndex];
                wizardState.credentialChoice = choice.$ref.split('/').pop();
            }
            
            wizardState.configData[basePath] = {};
            renderOneOfChoice(basePath, choiceIndex);
        }
    }

    handleGeneralClick(e) {
        const button = e.target.closest('button');
        if (!button) return;
        this.collectCurrentStepData();
        switch (button.id) {
            case 'prevStepBtn': return this.prevStep();
            case 'nextStepBtn': return this.nextStep();
            case 'runValidationsBtn': return this.runValidations(button);
            case 'saveConfigBtn': return this.handleSave(button);
            case 'cancelBtn': return navigate(wizardState.context.type === 'sources' ? 'sources' : 'pushers');
        }
    }
    
    nextStep() {
        wizardState.nextStep(wizardState.wizardDefinition.steps.length);
        this.renderStep();
    }
    prevStep() {
        wizardState.prevStep();
        this.renderStep();
    }

    async runValidations(button) {
        button.disabled = true;
        button.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>正在校验...';

        const currentStepDef = wizardState.wizardDefinition.steps[wizardState.currentStep];
        const checksToRun = currentStepDef.validations || [];
        
        // Mark all checks for the current step as pending
        checksToRun.forEach(key => wizardState.setValidationStatus(key, null, '等待中...'));
        this.renderStep(); // Re-render to show pending status

        let allTestsPassed = true;
        for (const check of checksToRun) {
            try {
                const endpoint = `/api/drivers/${wizardState.context.type}/${wizardState.configData.driver}/_actions/${check}`;

                // Create a clean, minimal payload containing only the fields relevant for validation.
                const validationPayload = {
                    uri: wizardState.configData.uri,
                    path: wizardState.configData.path,
                    endpoint: wizardState.configData.endpoint,
                    admin_creds: wizardState.configData.admin_creds,
                    credential: wizardState.configData.credential,
                    driver_params: wizardState.configData.driver_params
                };

                const result = await apiService.post(endpoint, validationPayload);
                
                wizardState.setValidationStatus(check, result.success, result.message);
                if (check === 'discover_fields_no_cache' && result.success && result.fields) {
                    wizardState.discoveredFields = result.fields;
                }

                if (!result.success) {
                    allTestsPassed = false;
                    break; // Stop validation on first failure
                }
            } catch (error) {
                const errorMessage = typeof error.message === 'object' ? JSON.stringify(error.message) : error.message;
                wizardState.setValidationStatus(check, false, errorMessage);
                allTestsPassed = false;
                break; // Stop validation on first failure
            } finally {
                this.renderStep(); // Re-render to show the result of the current check
            }
        }

        if (allTestsPassed) {
            showSuccess('所有校验步骤均已成功！');
        } else {
            showWarning('校验失败，请根据提示检查您的配置。');
        }
        // Final render to update footer buttons based on overall validation status
        this.renderStep(); 
    }
    
    async handleSave(button) {
        const { type, mode, id: oldId } = wizardState.context;
        const { id: newId, driver } = wizardState.configData;

        if (!newId) {
            showBusinessError('配置 ID 不能为空。');
            return;
        }
        if (mode === 'add' && apiService.checkConfigIdExists(type, newId)) {
            showBusinessError(`配置 ID '${newId}' 已存在，请使用一个新的ID。`);
            return;
        }
        
        button.disabled = true;
        button.innerHTML = '<span class="spinner-border spinner-border-sm me-2" role="status"></span>保存中...';
        
        const configForSave = { ...wizardState.configData };

        if (configForSave.credential && wizardState.credentialChoice) {
            const wizardDef = wizardState.wizardDefinition;
            const refName = wizardState.credentialChoice;
            const selectedSchema = wizardDef.components.schemas[refName];
            
            const cleanCredential = {};
            if (selectedSchema && selectedSchema.properties) {
                for (const propKey in selectedSchema.properties) {
                    if (Object.prototype.hasOwnProperty.call(configForSave.credential, propKey)) {
                        cleanCredential[propKey] = configForSave.credential[propKey];
                    }
                }
            }
            configForSave.credential = cleanCredential;
        }

        delete configForSave.id;
        delete configForSave.idHint;
        delete configForSave.admin_creds;

        const payload = {
            config: configForSave,
            discovered_fields: wizardState.discoveredFields
        };

        if (driver === 'fs') {
            payload.config.uri = payload.config.path;
            payload.config.credential = { user: 'fs-user', passwd: null };
            delete payload.config.path;
        }

        try {
            if (type === 'sources') {
                await apiService.addSourceConfig(newId, payload.config, payload.discovered_fields, button);
            } else if (type === 'pushers') {
                await apiService.addPusherConfig(newId, payload.config, button);
            } else {
                showBusinessError(`内部错误：无效的配置类型 '${type}'`);
            }
            if (mode === 'edit' && oldId && oldId !== newId) {
                await apiService.disableSourceConfig(oldId);
                showSuccess(`新配置 '${newId}' 已保存，旧配置 '${oldId}' 已自动禁用。`);
            }
            navigate(type === 'sources' ? 'sources' : 'pushers');
        } catch (error) {
            button.disabled = false;
            button.innerHTML = '<i class="ti ti-check me-1"></i>保存配置';
        }
    }

    collectCurrentStepData() {
        const data = {};
        this.el.querySelectorAll('[data-key-path]').forEach(input => {
            const path = input.dataset.keyPath;
            const value = input.type === 'checkbox' ? input.checked : (input.type === 'number' ? parseFloat(input.value) || 0 : input.value);
            let current = data;
            const keys = path.split('.');
            for (let i = 0; i < keys.length - 1; i++) {
                current = current[keys[i]] = current[keys[i]] || {};
            }
            current[keys[keys.length - 1]] = value;
        });
        wizardState.updateConfigData(data);
    }
    
    onDeactivate() {
        this.el.removeEventListener('click', this.boundHandleClick);
        this.el.removeEventListener('change', this.boundHandleChange);
        this.el.innerHTML = '';
        wizardState.reset();
    }
}