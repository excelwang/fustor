/**
 * 统一通知模块 - 全局横幅
 * 所有通知现在都显示在 #global-alert-banner 中。
 */

const banner = document.getElementById('global-alert-banner');
let currentTimeout = null;

// 确保 banner 元素存在
if (!banner) {
    console.error('Fatal: Global alert banner element #global-alert-banner not found in the DOM.');
}

// 为 banner 上的关闭按钮添加事件监听器
const closeButton = banner?.querySelector('.btn-close');
if (closeButton) {
    closeButton.addEventListener('click', () => {
        hideBanner();
    });
}

function hideBanner() {
    if (banner) {
        banner.classList.add('d-none');
        banner.classList.remove('show');
    }
    if (currentTimeout) {
        clearTimeout(currentTimeout);
        currentTimeout = null;
    }
}

/**
 * 显示通知横幅
 * @param {string} message - 通知消息
 * @param {string} type - 通知类型 ('success', 'danger', 'warning', 'info')
 * @param {number} duration - 自动隐藏的毫秒数。如果为0，则不会自动隐藏。
 */
function showBanner(message, type = 'info', duration = 5000) {
    if (!banner) return;

    // 清除之前的计时器
    if (currentTimeout) {
        clearTimeout(currentTimeout);
    }

    // 更新内容和样式
    const bannerContent = banner.querySelector('.container-lg');
    if (bannerContent) {
        bannerContent.innerHTML = `<span>${message}</span>`; // 只显示消息
    }
    
    // 移除所有 alert-* class，然后添加当前的
    banner.className = 'alert alert-dismissible fade'; // 重置 class
    banner.classList.add(`alert-${type}`);

    // 显示 banner
    banner.classList.remove('d-none');
    // 触发 Bootstrap 的 fade in 效果
    setTimeout(() => banner.classList.add('show'), 10);

    // 设置自动隐藏
    if (duration > 0) {
        currentTimeout = setTimeout(() => {
            hideBanner();
        }, duration);
    }
}

/**
 * 显示一个短暂的、上下文相关的“Toast”通知。
 * @param {HTMLElement} targetElement - 通知将显示在其旁边的DOM元素。
 * @param {string} message - 通知消息。
 * @param {string} type - 通知类型 ('success', 'danger', 'warning', 'info').
 * @param {number} duration - 自动隐藏的毫秒数。
 */
export function showToast(targetElement, message, type = 'success', duration = 2000) {
    if (!targetElement) {
        console.warn("showToast: targetElement is null or undefined.");
        return;
    }

    const toast = document.createElement('div');
    toast.className = `toast-notification toast-notification-${type}`;
    toast.textContent = message;

    // 定位到目标元素旁边
    const rect = targetElement.getBoundingClientRect();
    toast.style.position = 'absolute';
    toast.style.left = `${rect.right + 10}px`; // 目标元素右侧10px
    toast.style.top = `${rect.top}px`;
    toast.style.zIndex = '1000'; // 确保在最上层

    document.body.appendChild(toast);

    // 动画效果 (可选)
    setTimeout(() => {
        toast.style.opacity = '1';
    }, 10);

    setTimeout(() => {
        toast.style.opacity = '0';
        toast.addEventListener('transitionend', () => toast.remove());
    }, duration);
}

/**
 * 显示JS运行时错误（只在控制台输出）
 * @param {string} message - 错误消息
 * @param {Error} error - 错误对象
 */
export function showJsError(message, error = null) {
    console.error('JS Runtime Error:', message);
    if (error) {
        console.error('Error details:', {
            message: error.message,
            stack: error.stack,
            name: error.name
        });
    }
}

/**
 * 显示业务逻辑错误
 * @param {string} message - 错误消息
 */
export function showBusinessError(message) {
    showBanner(message, 'danger', 0); // 错误消息不会自动隐藏
}

/**
 * 显示成功通知
 * @param {string} message - 成功消息
 */
export function showSuccess(message) {
    showBanner(message, 'success', 3000); // 成功消息3秒后自动隐藏
}

/**
 * 显示警告通知
 * @param {string} message - 警告消息
 */
export function showWarning(message) {
    showBanner(message, 'warning', 5000); // 警告消息5秒后自动隐藏
}

/**
 * 显示信息通知
 * @param {string} message - 信息消息
 */
export function showInfo(message) {
    showBanner(message, 'info', 3000); // 信息消息3秒后自动隐藏
}

/**
 * 清空（隐藏）当前通知
 */
export function clearAllMessages() {
    hideBanner();
}


// 导出所有公共函数
export default {
    showJsError,
    showBusinessError,
    showSuccess,
    showWarning,
    showInfo,
    clearAllMessages,
    showToast
};
