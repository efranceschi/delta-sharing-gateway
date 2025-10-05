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
    
    @GetMapping
    public String listTables(Model model) {
        model.addAttribute("tables", tableService.getAllTables());
        return "admin/tables/list";
    }
    
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
            tableService.createTable(dto);
            redirectAttributes.addFlashAttribute("successMessage", "Table created successfully!");
            return "redirect:/admin/tables";
        } catch (Exception e) {
            model.addAttribute("errorMessage", e.getMessage());
            model.addAttribute("schemas", schemaService.getAllSchemas());
            model.addAttribute("isEdit", false);
            return "admin/tables/form";
        }
    }
    
    @GetMapping("/{id}/edit")
    public String showEditForm(@PathVariable Long id, Model model) {
        try {
            model.addAttribute("table", tableService.getTableById(id));
            model.addAttribute("schemas", schemaService.getAllSchemas());
            model.addAttribute("isEdit", true);
            return "admin/tables/form";
        } catch (Exception e) {
            return "redirect:/admin/tables";
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
            tableService.updateTable(id, dto);
            redirectAttributes.addFlashAttribute("successMessage", "Table updated successfully!");
            return "redirect:/admin/tables";
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
            tableService.deleteTable(id);
            redirectAttributes.addFlashAttribute("successMessage", "Table deleted successfully!");
        } catch (Exception e) {
            redirectAttributes.addFlashAttribute("errorMessage", e.getMessage());
        }
        return "redirect:/admin/tables";
    }
}
