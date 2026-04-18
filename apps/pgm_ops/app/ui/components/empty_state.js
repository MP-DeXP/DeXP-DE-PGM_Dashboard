export function renderEmptyState(title, description) {
    return `
        <div class="ops-empty">
            <strong>${title}</strong>
            <p>${description}</p>
        </div>
    `;
}
