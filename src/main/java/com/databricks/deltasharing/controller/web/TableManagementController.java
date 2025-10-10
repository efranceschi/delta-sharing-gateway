package com.databricks.deltasharing.controller.web;

import com.databricks.deltasharing.dto.DeltaTableDTO;
import com.databricks.deltasharing.service.DeltaSchemaManagementService;
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
@RequestMapping("/admin/tables")
@RequiredArgsConstructor
@Slf4j
public class TableManagementController {
    
    private final DeltaTableManagementService tableService;
    private final DeltaSchemaManagementService schemaService;
    
    @GetMapping("/new")
    public String showCreateForm(Model model) {
        model.addAttribute("table", new DeltaTableDTO());
        model.addAttribute("schemas", schemaService.getAllSchemas());
        model.addAttribute("isEdit", false);
        return "admin/tables/form";
    }
    
    @PostMapping
    public String createTable(@Valid @ModelAttribute("table") DeltaTableDTO dto,
                             BindingResult result,
                             RedirectAttributes redirectAttributes,
                             Model model) {
        if (result.hasErrors()) {
            model.addAttribute("schemas", schemaService.getAllSchemas());
            model.addAttribute("isEdit", false);
            return "admin/tables/form";
        }
        
        try {
            DeltaTableDTO createdTable = tableService.createTable(dto);
            redirectAttributes.addFlashAttribute("successMessage", "Table created successfully!");
            return "redirect:/admin/schemas/" + createdTable.getSchemaId() + "/tables";
        } catch (Exception e) {
            model.addAttribute("errorMessage", e.getMessage());
            model.addAttribute("schemas", schemaService.getAllSchemas());
            model.addAttribute("isEdit", false);
            return "admin/tables/form";
        }
    }
    
    @GetMapping("/{id}")
    public String viewTable(@PathVariable Long id) {
        return "redirect:/admin/tables/" + id + "/edit";
    }
    
    @GetMapping("/{id}/edit")
    public String showEditForm(@PathVariable Long id, Model model, RedirectAttributes redirectAttributes) {
        try {
            DeltaTableDTO table = tableService.getTableById(id);
            model.addAttribute("table", table);
            model.addAttribute("schemas", schemaService.getAllSchemas());
            model.addAttribute("isEdit", true);
            return "admin/tables/form";
        } catch (Exception e) {
            redirectAttributes.addFlashAttribute("errorMessage", "Table not found");
            return "redirect:/";
        }
    }
    
    @PostMapping("/{id}")
    public String updateTable(@PathVariable Long id,
                             @Valid @ModelAttribute("table") DeltaTableDTO dto,
                             BindingResult result,
                             RedirectAttributes redirectAttributes,
                             Model model) {
        if (result.hasErrors()) {
            model.addAttribute("schemas", schemaService.getAllSchemas());
            model.addAttribute("isEdit", true);
            return "admin/tables/form";
        }
        
        try {
            DeltaTableDTO updatedTable = tableService.updateTable(id, dto);
            redirectAttributes.addFlashAttribute("successMessage", "Table updated successfully!");
            return "redirect:/admin/schemas/" + updatedTable.getSchemaId() + "/tables";
        } catch (Exception e) {
            model.addAttribute("errorMessage", e.getMessage());
            model.addAttribute("schemas", schemaService.getAllSchemas());
            model.addAttribute("isEdit", true);
            return "admin/tables/form";
        }
    }
    
    @GetMapping("/{id}/delete")
    public String deleteTable(@PathVariable Long id, RedirectAttributes redirectAttributes) {
        try {
            // Get the table first to know which schema to redirect to
            DeltaTableDTO table = tableService.getTableById(id);
            Long schemaId = table.getSchemaId();
            
            tableService.deleteTable(id);
            redirectAttributes.addFlashAttribute("successMessage", "Table deleted successfully!");
            return "redirect:/admin/schemas/" + schemaId + "/tables";
        } catch (Exception e) {
            redirectAttributes.addFlashAttribute("errorMessage", e.getMessage());
            return "redirect:/";
        }
    }
}
