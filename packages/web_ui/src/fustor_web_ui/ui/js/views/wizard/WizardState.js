// src/fuagent/ui/js/views/wizard/WizardState.js

/**
 * REFACTORED: Manages the state for a single wizard session.
 * This has been enhanced to be the single source of truth for the entire wizard workflow,
 * including context, configuration data being built, validation results, and UI state.
 * It now holds the dynamic wizard definition fetched from the backend.
 */
class WizardState {
    constructor() {
        // The reset method is called to initialize the state.
        this.reset();
    }

    /**
     * Resets the state to its initial, empty values.
     * This is called when the wizard is first activated or when it's closed/deactivated.
     */
    reset() {
        // The initial context provided when the wizard is launched.
        // e.g., { mode: 'edit', type: 'source', id: 'my-source-1' }
        this.context = {};
        // The configuration object being built or edited throughout the steps.
        this.configData = {};
        // --- START FIX: Add state to track credential choice ---
        this.credentialChoice = null; // e.g., 'PasswdCredential' or 'ApiKeyCredential'
        // --- END FIX ---
        // A temporary store for admin credentials used only during the setup process.
        // This is cleared once the wizard is closed.
        this.tempAdminCreds = null;
        // A temporary store for fields discovered in step 1, to be cached on final save.
        this.discoveredFields = null;
        // ==============================================================================
        // NEW: This property now holds the entire wizard structure fetched from the backend.
        // It includes the list of steps and any shared component schemas.
        // It replaces the old openApiSchema and driverSchema properties.
        // ==============================================================================
        this.wizardDefinition = null;
        // DEPRECATED: These are replaced by the self-contained wizardDefinition.
        // this.openApiSchema = {};
        // this.driverSchema = null;
        // ==============================================================================
        
        // List of all available drivers fetched from the backend.
        // e.g., { sources: ['mysql', 'postgres'], pushers: ['openapi'] }
        this.availableDrivers = { sources: [], pushers: [] };
        // Tracks the success/failure of validation steps.
        // e.g., { test_connection: { success: true, message: 'Connection successful.' } }
        this.validationStatus = {};
        // The current step index of the wizard (e.g., 0 for the first step).
        this.currentStep = 0;
    }

    /**
     * Initializes the state with context from the main application.
     * @param {object} context - The wizard context from stateStore.
     */
    initialize(context) {
        this.reset();
        this.context = context || {};
        console.log('WizardState initialized with context:', this.context);
    }
    
    _isObject(item) {
        return (item && typeof item === 'object' && !Array.isArray(item));
    }

    _deepMerge(target, ...sources) {
        if (!sources.length) return target;
        const source = sources.shift();
        const output = { ...target };

        if (this._isObject(target) && this._isObject(source)) {
            for (const key in source) {
                if (this._isObject(source[key])) {
                    if (!target[key]) {
                        Object.assign(output, { [key]: source[key] });
                    } else {
                        output[key] = this._deepMerge(target[key], source[key]);
                    }
                } else {
                    Object.assign(output, { [key]: source[key] });
                }
            }
        }
        return this._deepMerge(output, ...sources);
    }

    /**
     * Updates the main configuration data object by merging new data.
     * @param {object} newData - New data to merge into the configData.
     */
    updateConfigData(newData) {
        this.configData = this._deepMerge(this.configData, newData);
    }

    /**
     * Safely retrieves a value from the configData object using a dot-separated path.
     * @param {string} path - The path to the value (e.g., 'credential.user').
     * @returns {any} The value, or undefined if not found.
     */
    getConfigValue(path) {
        if (!path) return undefined;
        return path.split('.').reduce((obj, key) => (obj && obj[key] !== 'undefined') ? obj[key] : undefined, this.configData);
    }

    /**
     * REFACTORED: Resolves a $ref pointer to its schema definition within the
     * dynamically loaded wizardDefinition.
     * @param {string} refName - The name of the reference (e.g., 'PasswdCredential').
     * @returns {object} The schema definition, or undefined if not found.
     */
    getRefSchema(refName) {
        return this.wizardDefinition?.components?.schemas?.[refName];
    }

    /**
     * Updates a specific validation step's status.
     * @param {string} key - The name of the validation check (e.g., 'test_connection').
     * @param {boolean} success - The result of the validation.
     * @param {string} message - The feedback message from the validation attempt.
     */
    setValidationStatus(key, success, message) {
        this.validationStatus[key] = { success, message };
    }

    /**
     * NEW: Explicitly sets the discovered fields.
     * @param {object} fields - The fields object returned from the API.
     */
    setDiscoveredFields(fields) {
        this.discoveredFields = fields;
    }

    /**
     * Checks if all specified validation keys are marked as successful.
     * @param {Array<string>} keys - An array of validation keys to check.
     * @returns {boolean} - True if all keys are successful, otherwise false.
     */
    areAllValidationsSuccessful(keys = []) {
        // If there are no required validations, it's trivially successful.
        // The step module is responsible for providing the keys.
        // An empty array might mean the step has no validations, which is valid.
        if (keys.length === 0) {
            return true;
        }

        // For a step to be considered complete, every single required validation
        // must have a status object with success explicitly set to true.
        // It will return false if a key is missing from validationStatus or if its success is not true.
        return keys.every(key => this.validationStatus[key]?.success === true);
    }

    /**
     * Advances the wizard to the next step.
     * @param {number} maxSteps - The total number of steps in the wizard.
     */
    nextStep(maxSteps) {
        if (this.currentStep < maxSteps - 1) {
            this.currentStep++;
        }
    }

    /**
     * Moves the wizard to the previous step.
     */
    prevStep() {
        if (this.currentStep > 0) {
            this.currentStep--;
        }
    }
}

// Export a single, shared instance of the state manager (singleton pattern)
// This ensures that all components in the wizard are accessing the exact same state object.
export default new WizardState();