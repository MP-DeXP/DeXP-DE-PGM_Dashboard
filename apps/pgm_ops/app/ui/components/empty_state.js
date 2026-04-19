export function renderEmptyState(title, description) {
    return `
        <div class="ops-empty empty-state">
            <strong>${title}</strong>
            <p>${description}</p>
        </div>
    `;
}
