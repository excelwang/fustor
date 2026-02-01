/**
 * 全局状态管理模块
 */
import stateStore from './stateStore.js';

export function initGlobalState() {
    // 订阅状态变化以更新UI
    stateStore.subscribe((state) => {
        console.log('Global state updated:', state);
        // 在此处可以根据状态变化更新UI，如果视图需要监听特定状态，它们可以单独订阅
    });

    // 初始化状态
    const initialState = {
        appConfig: {},
        logs: []
    };
    stateStore.setState(initialState);

    return stateStore;
}
