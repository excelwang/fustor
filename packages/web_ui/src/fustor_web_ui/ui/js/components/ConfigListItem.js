// src/fuagent/ui/js/components/ConfigListItem.js

/**
 * Renders a single configuration item as a list-group-item.
 * This component standardizes the visual representation of Source, Pusher, and Sync configurations
 * within their respective tabs.
 *
 * @param {object} props - The properties for the list item.
 * @param {string} props.id - The configuration ID.
 * @param {string} props.type - The type of configuration ('source', 'pusher', 'sync'). Used for data-attributes.
 * @param {string} props.statusDotClass - CSS class for the status dot (e.g., 'status-dot-success', 'status-dot-secondary').
 * @param {string} props.statusTitle - Text for the status dot's title attribute (e.g., '已启用', '已禁用').
 * @param {string} props.mainTitleHtml - HTML string for the primary title (e.g., the config ID in strong tags).
 * @param {string} props.subDetailsHtml - HTML string for the secondary details (e.g., driver, source/pusher details).
 * @param {string} props.actionsHtml - HTML string for the action buttons list.
 * @returns {string} The HTML string for the list-group-item.
 */
export function createConfigListItem({
    id,
    type,
    statusDotClass,
    statusTitle,
    mainTitleHtml,
    subDetailsHtml,
    actionsHtml
}) {
    return `
        <div class="list-group-item" data-config-id="${id}" data-config-type="${type}">
            <div class="row align-items-center g-2">
                <div class="col-auto">
                    <span class="status-dot ${statusDotClass}" title="${statusTitle}"></span>
                </div>
                <div class="col text-truncate">
                    ${mainTitleHtml}
                    ${subDetailsHtml}
                </div>
                <div class="col-auto">
                    <div class="btn-list">
                        ${actionsHtml}
                    </div>
                </div>
            </div>
        </div>
    `;
}