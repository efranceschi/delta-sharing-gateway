package com.databricks.deltasharing.controller.api;

import com.databricks.deltasharing.dto.DeltaSchemaDTO;
import com.databricks.deltasharing.dto.DeltaShareDTO;
import com.databricks.deltasharing.dto.DeltaTableDTO;
import com.databricks.deltasharing.service.DeltaShareManagementService;
import com.databricks.deltasharing.service.DeltaSchemaManagementService;
import com.databricks.deltasharing.service.DeltaTableManagementService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/treeview")
@RequiredArgsConstructor
public class TreeViewController {
    
    private final DeltaShareManagementService shareService;
    private final DeltaSchemaManagementService schemaService;
    private final DeltaTableManagementService tableService;
    
    @GetMapping("/data")
    public ResponseEntity<List<TreeNode>> getTreeData() {
        List<DeltaShareDTO> shares = shareService.getAllShares();
        List<DeltaSchemaDTO> schemas = schemaService.getAllSchemas();
        List<DeltaTableDTO> tables = tableService.getAllTables();
        
        // Group schemas by share
        Map<Long, List<DeltaSchemaDTO>> schemasByShare = schemas.stream()
                .collect(Collectors.groupingBy(DeltaSchemaDTO::getShareId));
        
        // Group tables by schema
        Map<Long, List<DeltaTableDTO>> tablesBySchema = tables.stream()
                .collect(Collectors.groupingBy(DeltaTableDTO::getSchemaId));
        
        // Build tree structure
        List<TreeNode> treeNodes = new ArrayList<>();
        
        for (DeltaShareDTO share : shares) {
            TreeNode shareNode = new TreeNode();
            shareNode.setId("share-" + share.getId());
            shareNode.setLabel(share.getName());
            shareNode.setType("share");
            shareNode.setIcon("🗂️");
            shareNode.setEntityId(share.getId());
            shareNode.setDescription(share.getDescription());
            shareNode.setActive(share.getActive());
            
            List<TreeNode> schemaNodes = new ArrayList<>();
            List<DeltaSchemaDTO> shareSchemas = schemasByShare.getOrDefault(share.getId(), new ArrayList<>());
            
            for (DeltaSchemaDTO schema : shareSchemas) {
                TreeNode schemaNode = new TreeNode();
                schemaNode.setId("schema-" + schema.getId());
                schemaNode.setLabel(schema.getName());
                schemaNode.setType("schema");
                schemaNode.setIcon("📁");
                schemaNode.setEntityId(schema.getId());
                schemaNode.setDescription(schema.getDescription());
                schemaNode.setParentId(share.getId());
                
                List<TreeNode> tableNodes = new ArrayList<>();
                List<DeltaTableDTO> schemaTables = tablesBySchema.getOrDefault(schema.getId(), new ArrayList<>());
                
                for (DeltaTableDTO table : schemaTables) {
                    TreeNode tableNode = new TreeNode();
                    tableNode.setId("table-" + table.getId());
                    tableNode.setLabel(table.getName());
                    tableNode.setType("table");
                    tableNode.setIcon("📋");
                    tableNode.setEntityId(table.getId());
                    tableNode.setDescription(table.getDescription());
                    tableNode.setFormat(table.getFormat());
                    tableNode.setLocation(table.getLocation());
                    tableNode.setShareAsView(table.getShareAsView());
                    tableNode.setParentId(schema.getId());
                    
                    tableNodes.add(tableNode);
                }
                
                schemaNode.setChildren(tableNodes);
                schemaNode.setChildCount(tableNodes.size());
                schemaNodes.add(schemaNode);
            }
            
            shareNode.setChildren(schemaNodes);
            shareNode.setChildCount(schemaNodes.size());
            treeNodes.add(shareNode);
        }
        
        return ResponseEntity.ok(treeNodes);
    }
    
    // Inner class for tree node structure
    public static class TreeNode {
        private String id;
        private String label;
        private String type;
        private String icon;
        private Long entityId;
        private String description;
        private Boolean active;
        private String format;
        private String location;
        private Boolean shareAsView;
        private Long parentId;
        private List<TreeNode> children;
        private Integer childCount;
        
        // Getters and Setters
        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        
        public String getLabel() { return label; }
        public void setLabel(String label) { this.label = label; }
        
        public String getType() { return type; }
        public void setType(String type) { this.type = type; }
        
        public String getIcon() { return icon; }
        public void setIcon(String icon) { this.icon = icon; }
        
        public Long getEntityId() { return entityId; }
        public void setEntityId(Long entityId) { this.entityId = entityId; }
        
        public String getDescription() { return description; }
        public void setDescription(String description) { this.description = description; }
        
        public Boolean getActive() { return active; }
        public void setActive(Boolean active) { this.active = active; }
        
        public String getFormat() { return format; }
        public void setFormat(String format) { this.format = format; }
        
        public String getLocation() { return location; }
        public void setLocation(String location) { this.location = location; }
        
        public Boolean getShareAsView() { return shareAsView; }
        public void setShareAsView(Boolean shareAsView) { this.shareAsView = shareAsView; }
        
        public Long getParentId() { return parentId; }
        public void setParentId(Long parentId) { this.parentId = parentId; }
        
        public List<TreeNode> getChildren() { return children; }
        public void setChildren(List<TreeNode> children) { this.children = children; }
        
        public Integer getChildCount() { return childCount; }
        public void setChildCount(Integer childCount) { this.childCount = childCount; }
    }
}
