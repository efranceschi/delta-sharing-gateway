package com.databricks.deltasharing.controller.web;

import com.databricks.deltasharing.service.DeltaShareManagementService;
import com.databricks.deltasharing.service.DeltaSchemaManagementService;
import com.databricks.deltasharing.service.DeltaTableManagementService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("/hierarchy")
@RequiredArgsConstructor
@Slf4j
public class HierarchyController {
    
    private final DeltaShareManagementService shareService;
    private final DeltaSchemaManagementService schemaService;
    private final DeltaTableManagementService tableService;
    
    @GetMapping
    public String viewHierarchy(Model model) {
        log.info("Viewing hierarchy - all shares");
        model.addAttribute("shares", shareService.getAllShares());
        model.addAttribute("viewLevel", "shares");
        return "hierarchy/view";
    }
    
    @GetMapping("/share/{shareId}")
    public String viewShare(@PathVariable Long shareId, Model model) {
        log.info("Viewing hierarchy - share: {}", shareId);
        
        var share = shareService.getShareById(shareId);
        var schemas = schemaService.getAllSchemas().stream()
                .filter(s -> s.getShareId().equals(shareId))
                .toList();
        
        model.addAttribute("share", share);
        model.addAttribute("schemas", schemas);
        model.addAttribute("shares", shareService.getAllShares());
        model.addAttribute("viewLevel", "schemas");
        return "hierarchy/view";
    }
    
    @GetMapping("/share/{shareId}/schema/{schemaId}")
    public String viewSchema(@PathVariable Long shareId, @PathVariable Long schemaId, Model model) {
        log.info("Viewing hierarchy - schema: {}", schemaId);
        
        var share = shareService.getShareById(shareId);
        var schema = schemaService.getSchemaById(schemaId);
        var tables = tableService.getAllTables().stream()
                .filter(t -> t.getSchemaId().equals(schemaId))
                .toList();
        
        model.addAttribute("share", share);
        model.addAttribute("schema", schema);
        model.addAttribute("tables", tables);
        model.addAttribute("shares", shareService.getAllShares());
        model.addAttribute("viewLevel", "tables");
        return "hierarchy/view";
    }
}
