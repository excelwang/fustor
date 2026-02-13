/** General utilities for Fustor Management UI */

function escapeHtml(t) {
    if (!t) return '';
    let d = document.createElement('div');
    d.textContent = t;
    return d.innerHTML;
}

function badge(text, type) {
    return `<span class="badge badge-${type}">${escapeHtml(text)}</span>`;
}

function formatAge(s) {
    if (s == null) return '-';
    if (s < 60) return Math.round(s) + 's';
    if (s < 3600) return Math.round(s / 60) + 'm';
    return Math.round(s / 3600) + 'h';
}

function getKey() {
    return localStorage.getItem('mgmt_key') || '';
}

function setKey(k) {
    localStorage.setItem('mgmt_key', k);
}

function toggleElement(id) {
    let el = document.getElementById(id);
    if (el) {
        el.style.display = el.style.display === 'none' ? 'block' : 'none';
    }
}
