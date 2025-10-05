package com.databricks.deltasharing.controller.web;

import com.databricks.deltasharing.dto.DeltaSchemaDTO;
import com.databricks.deltasharing.service.DeltaSchemaManagementService;
import com.databricks.deltasharing.service.DeltaShareManagementService;
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
    
    @GetMapping
    public String listSchemas(Model model) {
        model.addAttribute("schemas", schemaService.getAllSchemas());
        return "admin/schemas/list";
    }
    
    @GetMapping("/new")
    public String showCreateForm(Model model) {
        model.addAttribute("schema", new DeltaSchemaDTO());
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
}
