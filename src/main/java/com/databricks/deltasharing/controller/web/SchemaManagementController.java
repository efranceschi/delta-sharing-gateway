package com.databricks.deltasharing.controller.web;

import com.databricks.deltasharing.dto.DeltaSchemaDTO;
import com.databricks.deltasharing.service.DeltaSchemaManagementService;
import com.databricks.deltasharing.service.DeltaShareManagementService;
import com.databricks.deltasharing.service.DeltaTableManagementService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

@Controller
@RequestMapping("/admin/schemas")
@RequiredArgsConstructor
@Slf4j
public class SchemaManagementController {
    
    private final DeltaSchemaManagementService schemaService;
    private final DeltaShareManagementService shareService;
    private final DeltaTableManagementService tableService;
    
    @GetMapping
    public String listSchemas(Model model) {
        model.addAttribute("schemas", schemaService.getAllSchemas());
        return "admin/schemas/list";
    }
    
    @GetMapping("/new")
    public String showCreateForm(@RequestParam(required = false) Long shareId, Model model) {
        DeltaSchemaDTO schema = new DeltaSchemaDTO();
        
        // If shareId is provided, pre-populate it and get share details for breadcrumb
        if (shareId != null) {
            try {
                var share = shareService.getShareById(shareId);
                schema.setShareId(shareId);
                schema.setShareName(share.getName());
                log.info("Creating new schema with pre-selected share: {} (ID: {})", share.getName(), shareId);
            } catch (Exception e) {
                log.warn("Share ID {} not found, proceeding without pre-selection", shareId);
            }
        }
        
        model.addAttribute("schema", schema);
        model.addAttribute("shares", shareService.getAllShares());
        model.addAttribute("isEdit", false);
        return "admin/schemas/form";
    }
    
    @PostMapping
    public String createSchema(@Valid @ModelAttribute("schema") DeltaSchemaDTO dto,
                              BindingResult result,
                              RedirectAttributes redirectAttributes,
                              Model model) {
        if (result.hasErrors()) {
            model.addAttribute("shares", shareService.getAllShares());
            model.addAttribute("isEdit", false);
            return "admin/schemas/form";
        }
        
        try {
            schemaService.createSchema(dto);
            redirectAttributes.addFlashAttribute("successMessage", "Schema created successfully!");
            return "redirect:/admin/schemas";
        } catch (Exception e) {
            model.addAttribute("errorMessage", e.getMessage());
            model.addAttribute("shares", shareService.getAllShares());
            model.addAttribute("isEdit", false);
            return "admin/schemas/form";
        }
    }
    
    @GetMapping("/{id}")
    public String viewSchema(@PathVariable Long id) {
        return "redirect:/admin/schemas/" + id + "/tables";
    }
    
    @GetMapping("/{id}/edit")
    public String showEditForm(@PathVariable Long id, Model model) {
        try {
            model.addAttribute("schema", schemaService.getSchemaById(id));
            model.addAttribute("shares", shareService.getAllShares());
            model.addAttribute("isEdit", true);
            return "admin/schemas/form";
        } catch (Exception e) {
            return "redirect:/admin/schemas";
        }
    }
    
    @PostMapping("/{id}")
    public String updateSchema(@PathVariable Long id,
                              @Valid @ModelAttribute("schema") DeltaSchemaDTO dto,
                              BindingResult result,
                              RedirectAttributes redirectAttributes,
                              Model model) {
        if (result.hasErrors()) {
            model.addAttribute("shares", shareService.getAllShares());
            model.addAttribute("isEdit", true);
            return "admin/schemas/form";
        }
        
        try {
            schemaService.updateSchema(id, dto);
            redirectAttributes.addFlashAttribute("successMessage", "Schema updated successfully!");
            return "redirect:/admin/schemas";
        } catch (Exception e) {
            model.addAttribute("errorMessage", e.getMessage());
            model.addAttribute("shares", shareService.getAllShares());
            model.addAttribute("isEdit", true);
            return "admin/schemas/form";
        }
    }
    
    @GetMapping("/{id}/delete")
    public String deleteSchema(@PathVariable Long id, RedirectAttributes redirectAttributes) {
        try {
            schemaService.deleteSchema(id);
            redirectAttributes.addFlashAttribute("successMessage", "Schema deleted successfully!");
        } catch (Exception e) {
            redirectAttributes.addFlashAttribute("errorMessage", e.getMessage());
        }
        return "redirect:/admin/schemas";
    }
    
    @GetMapping("/{id}/tables")
    public String listSchemaTables(@PathVariable Long id, Model model) {
        try {
            DeltaSchemaDTO schema = schemaService.getSchemaById(id);
            model.addAttribute("schema", schema);
            model.addAttribute("tables", tableService.getTablesBySchemaId(id));
            return "admin/schemas/tables";
        } catch (Exception e) {
            return "redirect:/admin/schemas";
        }
    }
}
