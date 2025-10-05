// Dynamic TreeView for Delta Sharing Management
class TreeView {
    constructor(containerId) {
        this.container = document.getElementById(containerId);
        this.data = [];
        this.expandedNodes = new Set();
        this.selectedNode = null;
    }

    async load() {
        try {
            const response = await fetch('/api/treeview/data');
            this.data = await response.json();
            this.render();
        } catch (error) {
            console.error('Error loading tree data:', error);
            this.container.innerHTML = '<div class="tree-error">Failed to load data</div>';
        }
    }

    render() {
        this.container.innerHTML = '';
        this.data.forEach(node => {
            this.renderNode(node, this.container, 0);
        });
    }

    renderNode(node, parentElement, level) {
        const nodeDiv = document.createElement('div');
        nodeDiv.className = 'tree-node';
        nodeDiv.dataset.nodeId = node.id;
        nodeDiv.dataset.level = level;

        const isExpanded = this.expandedNodes.has(node.id);
        const hasChildren = node.children && node.children.length > 0;

        // Node content
        const nodeContent = document.createElement('div');
        nodeContent.className = 'tree-node-content';
        nodeContent.style.paddingLeft = `${level * 20 + 10}px`;
        
        if (this.selectedNode === node.id) {
            nodeContent.classList.add('selected');
        }

        // Expand/collapse icon
        if (hasChildren) {
            const expandIcon = document.createElement('span');
            expandIcon.className = 'tree-expand-icon';
            expandIcon.innerHTML = isExpanded ? '▼' : '▶';
            expandIcon.onclick = (e) => {
                e.stopPropagation();
                this.toggleNode(node.id);
            };
            nodeContent.appendChild(expandIcon);
        } else {
            const spacer = document.createElement('span');
            spacer.className = 'tree-expand-spacer';
            nodeContent.appendChild(spacer);
        }

        // Node icon
        const icon = document.createElement('span');
        icon.className = 'tree-node-icon';
        icon.innerHTML = node.icon;
        nodeContent.appendChild(icon);

        // Node label
        const label = document.createElement('span');
        label.className = 'tree-node-label';
        label.textContent = node.label;
        nodeContent.appendChild(label);

        // Child count badge
        if (hasChildren) {
            const badge = document.createElement('span');
            badge.className = 'tree-node-badge';
            badge.textContent = node.childCount;
            nodeContent.appendChild(badge);
        }

        // Status indicators
        if (node.type === 'share' && node.active) {
            const activeIndicator = document.createElement('span');
            activeIndicator.className = 'tree-node-status active';
            activeIndicator.title = 'Active';
            nodeContent.appendChild(activeIndicator);
        }

        if (node.type === 'table' && node.shareAsView) {
            const viewIndicator = document.createElement('span');
            viewIndicator.className = 'tree-node-status view';
            viewIndicator.textContent = 'V';
            viewIndicator.title = 'View';
            nodeContent.appendChild(viewIndicator);
        }

        // Click handler
        nodeContent.onclick = () => {
            this.selectNode(node);
        };

        nodeDiv.appendChild(nodeContent);

        // Children container
        if (hasChildren) {
            const childrenDiv = document.createElement('div');
            childrenDiv.className = 'tree-node-children';
            childrenDiv.style.display = isExpanded ? 'block' : 'none';

            node.children.forEach(child => {
                this.renderNode(child, childrenDiv, level + 1);
            });

            nodeDiv.appendChild(childrenDiv);
        }

        parentElement.appendChild(nodeDiv);
    }

    toggleNode(nodeId) {
        if (this.expandedNodes.has(nodeId)) {
            this.expandedNodes.delete(nodeId);
        } else {
            this.expandedNodes.add(nodeId);
        }
        this.render();
    }

    selectNode(node) {
        this.selectedNode = node.id;
        this.render();
        this.showNodeDetails(node);
    }

    showNodeDetails(node) {
        const detailsPanel = document.getElementById('tree-details-panel');
        if (!detailsPanel) return;
        
        // Show the panel
        detailsPanel.style.display = 'block';

        let detailsHTML = `
            <div class="details-header">
                <span class="details-icon">${node.icon}</span>
                <h3>${node.label}</h3>
                <span class="details-type">${node.type}</span>
            </div>
            <div class="details-body">
        `;

        if (node.description) {
            detailsHTML += `<p class="details-description">${node.description}</p>`;
        }

        detailsHTML += '<div class="details-info">';

        if (node.type === 'share') {
            detailsHTML += `
                <div class="info-item">
                    <span class="info-label">Status:</span>
                    <span class="info-value">
                        <span class="badge badge-${node.active ? 'success' : 'danger'}">
                            ${node.active ? 'Active' : 'Inactive'}
                        </span>
                    </span>
                </div>
                <div class="info-item">
                    <span class="info-label">Schemas:</span>
                    <span class="info-value">${node.childCount || 0}</span>
                </div>
            `;
        } else if (node.type === 'schema') {
            detailsHTML += `
                <div class="info-item">
                    <span class="info-label">Tables:</span>
                    <span class="info-value">${node.childCount || 0}</span>
                </div>
            `;
        } else if (node.type === 'table') {
            detailsHTML += `
                <div class="info-item">
                    <span class="info-label">Format:</span>
                    <span class="info-value"><code>${node.format || 'N/A'}</code></span>
                </div>
                <div class="info-item">
                    <span class="info-label">Type:</span>
                    <span class="info-value">
                        <span class="badge badge-${node.shareAsView ? 'warning' : 'info'}">
                            ${node.shareAsView ? 'View' : 'Table'}
                        </span>
                    </span>
                </div>
                <div class="info-item">
                    <span class="info-label">Location:</span>
                    <span class="info-value"><code class="location-code">${node.location || 'N/A'}</code></span>
                </div>
            `;
        }

        detailsHTML += '</div>';

        // Action buttons
        detailsHTML += '<div class="details-actions">';
        
        if (node.type === 'share') {
            detailsHTML += `
                <a href="/admin/shares/${node.entityId}/edit" class="btn btn-sm btn-secondary">✏️ Edit</a>
                <a href="/admin/schemas/new?shareId=${node.entityId}" class="btn btn-sm btn-primary">➕ Add Schema</a>
            `;
        } else if (node.type === 'schema') {
            detailsHTML += `
                <a href="/admin/schemas/${node.entityId}/edit" class="btn btn-sm btn-secondary">✏️ Edit</a>
                <a href="/admin/tables/new?schemaId=${node.entityId}" class="btn btn-sm btn-primary">➕ Add Table</a>
            `;
        } else if (node.type === 'table') {
            detailsHTML += `
                <a href="/admin/tables/${node.entityId}/edit" class="btn btn-sm btn-secondary">✏️ Edit</a>
                <a href="/admin/tables/${node.entityId}/delete" class="btn btn-sm btn-danger" 
                   onclick="return confirm('Delete this table?')">🗑️ Delete</a>
            `;
        }

        detailsHTML += '</div></div>';

        detailsPanel.innerHTML = detailsHTML;
    }

    expandAll() {
        this.data.forEach(node => {
            this.expandNodeRecursive(node);
        });
        this.render();
    }

    expandNodeRecursive(node) {
        this.expandedNodes.add(node.id);
        if (node.children) {
            node.children.forEach(child => this.expandNodeRecursive(child));
        }
    }

    collapseAll() {
        this.expandedNodes.clear();
        this.render();
    }

    refresh() {
        this.load();
    }
}

// Initialize tree view when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    const treeViewContainer = document.getElementById('management-treeview');
    if (treeViewContainer) {
        const treeView = new TreeView('management-treeview');
        treeView.load();

        // Expose to global scope for controls
        window.managementTreeView = treeView;

        // Setup control buttons
        const expandAllBtn = document.getElementById('tree-expand-all');
        const collapseAllBtn = document.getElementById('tree-collapse-all');
        const refreshBtn = document.getElementById('tree-refresh');

        if (expandAllBtn) expandAllBtn.onclick = () => treeView.expandAll();
        if (collapseAllBtn) collapseAllBtn.onclick = () => treeView.collapseAll();
        if (refreshBtn) refreshBtn.onclick = () => treeView.refresh();
    }
});
