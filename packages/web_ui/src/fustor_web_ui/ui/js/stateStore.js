/**
 * 集中式状态管理
 */
class StateStore {
  constructor() {
    this.state = {
      appConfig: {},
      logs: [],
      // The wizardContext will hold the necessary info for a wizard to initialize itself.
      // e.g., { mode: 'add', type: 'source' } or { mode: 'edit', type: 'sync', id: 'my-sync' }
      wizardContext: null,
      currentEngineConfig: null // This seems to be a legacy property, keeping it for now.
    };
    this.listeners = [];
  }
  
  getState() {
    return this.state;
  }
  
  setState(newState) {
    this.state = {
      ...this.state,
      ...newState
    };
    this.notifyListeners();
  }
  
  updateLogs(logEntry) {
    const logs = [logEntry, ...this.state.logs];
    if (logs.length > 200) {
      logs.pop();
    }
    this.setState({ logs });
  }
  
  /**
   * Subscribes a listener function to state changes.
   * @param {function} listener - The function to call when state changes. It receives the full state object.
   * @param {string|null} [key=null] - Optional. If provided, the listener will only be called if this specific key in the state has changed.
   */
  subscribe(listener, key = null) {
    const scopedListener = {
      callback: listener,
      key: key,
      lastValue: key ? this.state[key] : null
    };
    this.listeners.push(scopedListener);
    
    // Return an unsubscribe function
    return () => {
      this.listeners = this.listeners.filter(l => l.callback !== listener);
    };
  }
  
  notifyListeners() {
    const newState = this.state;
    this.listeners.forEach(l => {
      if (l.key) {
        const newValue = newState[l.key];
        // Only notify if the value of the subscribed key has changed.
        // This is a simple deep-equal check for objects/arrays.
        if (JSON.stringify(newValue) !== JSON.stringify(l.lastValue)) {
          l.lastValue = JSON.parse(JSON.stringify(newValue)); // Deep copy for next comparison
          l.callback(newState);
        }
      } else {
        // If no key is specified, notify on every state change.
        l.callback(newState);
      }
    });
  }
}

export default new StateStore();