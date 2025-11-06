// src/fuagent/ui/js/views/wizard/WizardUiComponents.js

import wizardState from './WizardState.js';

export function renderProperty(key, prop, configData, parentElement, isRequired = false) {
    const pathParts = key.split('.');
    const localKey = pathParts[pathParts.length - 1];
    const fullPath = pathParts.join('.');
    const getValueFromPath = (obj, path) => path.split('.').reduce((o, k) => (o && o[k] !== undefined) ? o[k] : undefined, obj);
    let value = getValueFromPath(configData, fullPath);

    if (value === undefined) {
        value = prop.default !== undefined ?
            prop.default : '';
    }

    const requiredClass = isRequired || prop.isRequired ? 'required' : '';
    const hint = prop.description ? `<small class="form-hint">${prop.description}</small>` : '';
    const uiHintClass = prop.ui_hint ? `ui-hint-${prop.ui_hint}` : '';
    const inputId = `config-${fullPath.replace(/\./g, '-')}`;
    let inputHtml = '';

    if (prop.oneOf) {
        // --- START: Definitive fix for UI state restoration ---
        const choices = prop.oneOf.map((choice, index) => {
            const refName = choice.$ref.split('/').pop();
            return { index, refName, title: wizardState.getRefSchema(refName)?.title || refName };
        });

        // Determine the currently selected index from the wizard's state
        let selectedIndex = 0; // Default to the first option
        if (wizardState.credentialChoice) {
            const foundChoice = choices.find(c => c.refName === wizardState.credentialChoice);
            if (foundChoice) {
                selectedIndex = foundChoice.index;
            }
        }

        // Generate options HTML, marking the correct one as 'selected'
        const optionsHtml = choices.map(c => `<option value="${c.index}" ${c.index === selectedIndex ? 'selected' : ''}>${c.title}</option>`).join('');
        
        const fieldset = document.createElement('div');
        fieldset.className = 'mb-3';
        fieldset.innerHTML = `
            <hr>
            <h6>${prop.title || localKey}</h6>
            <p class="form-hint mt-1">${prop.description || ''}</p>
            <div class="row">
                <div class="col-md-6">
                    <label class="form-label">凭证类型</label>
                    <select class="form-select" data-oneof-selector-for="${key}">
                        ${optionsHtml}
                    </select>
                </div>
            </div>
            <div data-oneof-container-for="${key}" class="mt-3"></div>
        `;
        parentElement.appendChild(fieldset);

        // Render the inputs for the correctly selected choice, not the hardcoded default
        renderOneOfChoice(key, selectedIndex);
        return;
        // --- END: Definitive fix for UI state restoration ---
    }

    if (prop.type === 'string' && prop.format === 'password') {
        inputHtml = `<input type="password" class="form-control ${uiHintClass}" id="${inputId}" data-key-path="${fullPath}" value="${value}" autocomplete="new-password">`;
    } else if (prop.type === 'string' && prop.enum) {
        const options = prop.enum.map(opt => `<option value="${opt}" ${value === opt ? 'selected' : ''}>${opt}</option>`).join('');
        inputHtml = `<select class="form-select ${uiHintClass}" id="${inputId}" data-key-path="${fullPath}">${options}</select>`;
    } else if (prop.type === 'boolean') {
        inputHtml = `
            <label class="form-check form-switch ${uiHintClass}">
                <input class="form-check-input" type="checkbox" id="${inputId}" data-key-path="${fullPath}" ${value ?
            'checked' : ''}>
                <span class="form-check-label">${prop.title ||
            localKey}</span>
            </label>`;
        parentElement.insertAdjacentHTML('beforeend', `<div class="mb-3 ${uiHintClass}">${inputHtml}${hint}</div>`);
        return;
    } else {
        const inputType = prop.type === 'integer' ||
            prop.type === 'number' ? 'number' : 'text';
        inputHtml = `<input type="${inputType}" class="form-control ${uiHintClass}" id="${inputId}" data-key-path="${fullPath}" value="${value}">`;
    }

    parentElement.insertAdjacentHTML('beforeend', `
        <div class="mb-3 ${uiHintClass}">
            <label for="${inputId}" class="form-label ${requiredClass}">${prop.title || localKey}</label>
            ${inputHtml}
            ${hint}
        </div>
    `);
}

export function renderOneOfChoice(basePath, choiceIndex) {
    const container = document.querySelector(`[data-oneof-container-for="${basePath}"]`);
    if (!container) return;

    const stepDef = wizardState.wizardDefinition.steps.find(s => s.schema?.properties?.[basePath]);
    if (!stepDef) return;

    const choice = stepDef.schema.properties[basePath].oneOf[choiceIndex];
    const refName = choice.$ref.split('/').pop();
    const schema = wizardState.getRefSchema(refName);

    container.innerHTML = '';
    const rowDiv = document.createElement('div');
    rowDiv.className = 'row';
    container.appendChild(rowDiv);

    if (schema && schema.properties) {
        Object.entries(schema.properties).forEach(([subKey, subProp]) => {
            const colDiv = document.createElement('div');
            colDiv.className = 'col-md-6';
            rowDiv.appendChild(colDiv);
            const isRequired = (schema.required || []).includes(subKey);
            renderProperty(`${basePath}.${subKey}`, subProp, wizardState.configData, colDiv, isRequired);
        });
    }
}

export function renderCredentialInputs(key, definition, configData, parentElement) {
    if (!definition || !definition.properties) return;

    const fieldset = document.createElement('div');
    fieldset.innerHTML = `<h6>${definition.title || '凭证'}</h6>`;
    if (definition.description) {
        fieldset.innerHTML += `<p class="form-hint mt-1">${definition.description}</p>`;
    }

    const rowDiv = document.createElement('div');
    rowDiv.className = 'row';
    fieldset.appendChild(rowDiv);
    Object.entries(definition.properties).forEach(([subKey, subProp]) => {
        const colDiv = document.createElement('div');
        colDiv.className = 'col-md-6';
        rowDiv.appendChild(colDiv);
        const isRequired = (definition.required || []).includes(subKey);
        renderProperty(`${key}.${subKey}`, subProp, configData, colDiv, isRequired);
    });
    parentElement.appendChild(fieldset);
}

export function renderValidationSection(parentElement, { title, hint }) {
    parentElement.innerHTML = `
        <hr>
        <h6>${title}</h6>
        <p class="form-hint">${hint}</p>
        <div class="validation-results-list list-group list-group-flush"></div>
    `;
    return parentElement.querySelector('.validation-results-list');
}

export function renderValidationStep(listContainer, key, text) {
    const div = document.createElement('div');
    div.className = 'list-group-item d-flex align-items-center validation-step';
    div.dataset.validationKey = key;
    div.innerHTML = `
        <span class="status-dot status-dot-secondary me-2" title="未运行"></span>
        <span class="status-text">${text}</span>
        <span class="status-message text-muted ms-auto"></span>
    `;
    listContainer.appendChild(div);
}

export function updateValidationStepUI(listContainer, key, success, message) {
    const stepRow = listContainer.querySelector(`.validation-step[data-validation-key="${key}"]`);
    if (stepRow) {
        const statusDot = stepRow.querySelector('.status-dot');
        const statusMessage = stepRow.querySelector('.status-message');
        statusDot.className = `status-dot ${success ? 'status-dot-success' : 'status-dot-danger'} me-2`;
        statusDot.title = success ? '成功' : '失败';

        statusMessage.textContent = message ||
            (success ? '成功' : '失败');
        statusMessage.className = `status-message ms-auto small ${success ? 'text-success' : 'text-danger'}`;
    }
}