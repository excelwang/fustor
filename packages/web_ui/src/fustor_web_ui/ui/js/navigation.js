/**
 * 导航控制器
 */
let currentActiveViewInstance = null;
let _getViewInstance = null;
// To store the passed getViewInstance function

export function navigate(viewId) {
    const views = document.querySelectorAll('.view');
    const navLinks = document.querySelectorAll('.navbar-nav .nav-item');
    
    // Deactivate current view instance if any
    if (currentActiveViewInstance && typeof currentActiveViewInstance.onDeactivate === 'function') {
        currentActiveViewInstance.onDeactivate();
    }

    // Hide all views
    views.forEach(v => v.classList.remove('active'));
    // Show target view
    const targetView = document.getElementById(`${viewId}-view`);
    if (targetView) {
        targetView.classList.add('active');
    }

    // REFACTORED: Use more explicit logic to ensure the correct link is highlighted. 
    navLinks.forEach(link => {
        const anchor = link.querySelector('.nav-link');
        if (anchor) {
            // Explicitly add or remove the 'active' class
            if (link.dataset.view === viewId) {
                anchor.classList.add('active');
            } else {
                anchor.classList.remove('active');
            }
        }
    });

    // Activate new view instance if any
    const newViewInstance = _getViewInstance(viewId);
    console.log(`[NAVIGATION] Attempting to activate view: ${viewId}. Instance:`, newViewInstance, `Is onActivate function: ${typeof newViewInstance?.onActivate === 'function'}`);
    if (newViewInstance && typeof newViewInstance.onActivate === 'function') {
        newViewInstance.onActivate();
    }
    currentActiveViewInstance = newViewInstance;

    // Trigger view change event
    const event = new CustomEvent('viewChange', { detail: { viewId } });
    document.dispatchEvent(event);
}

export function initNavigation(getAppViewInstance) {
    _getViewInstance = getAppViewInstance;

    // Set navigation click events
    document.querySelectorAll('.navbar-nav .nav-item').forEach(link => {
        if (link.dataset.view) {
            link.addEventListener('click', (e) => {
                e.preventDefault();
                const viewId = e.currentTarget.dataset.view;
                navigate(viewId);
                // Update URL hash without triggering a full page reload
                window.location.hash = viewId;
            });
        }
    });

    // Handle initial load and hash changes
    const handleHashChange = () => {
        const hash = window.location.hash.substring(1); // Remove the #
        const defaultView = 'dashboard';
        const validViews = Array.from(document.querySelectorAll('.navbar-nav .nav-item')).map(el => el.dataset.view);
        
        if (hash && validViews.includes(hash)) {
            navigate(hash);
        } else {
            navigate(defaultView);
            window.location.hash = defaultView; // Set default hash if none or invalid
        }
    };

    // Listen for hash changes (e.g., browser back/forward buttons)
    window.addEventListener('hashchange', handleHashChange);

    // Trigger on initial load
    handleHashChange();
}