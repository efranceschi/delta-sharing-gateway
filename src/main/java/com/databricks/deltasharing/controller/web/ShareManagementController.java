package com.databricks.deltasharing.controller.web;

import com.databricks.deltasharing.dto.DeltaShareDTO;
import com.databricks.deltasharing.service.DeltaShareManagementService;
import com.databricks.deltasharing.service.DeltaSchemaManagementService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

@Controller
@RequestMapping("/admin/shares")
@RequiredArgsConstructor
@Slf4j
public class ShareManagementController {
    
    private final DeltaShareManagementService shareService;
    private final DeltaSchemaManagementService schemaService;
    
    @GetMapping
    public String listShares(Model model) {
        model.addAttribute("shares", shareService.getAllShares());
        return "admin/shares/list";
    }
    
    @GetMapping("/new")
    public String showCreateForm(Model model) {
        model.addAttribute("share", new DeltaShareDTO());
        model.addAttribute("isEdit", false);
        return "admin/shares/form";
    }
    
    @PostMapping
    public String createShare(@Valid @ModelAttribute("share") DeltaShareDTO dto,
                             BindingResult result,
                             RedirectAttributes redirectAttributes,
                             Model model) {
        if (result.hasErrors()) {
            model.addAttribute("isEdit", false);
            return "admin/shares/form";
        }
        
        try {
            shareService.createShare(dto);
            redirectAttributes.addFlashAttribute("successMessage", "Share created successfully!");
            return "redirect:/admin/shares";
        } catch (Exception e) {
            model.addAttribute("errorMessage", e.getMessage());
            model.addAttribute("isEdit", false);
            return "admin/shares/form";
        }
    }
    
    @GetMapping("/{id}")
    public String viewShare(@PathVariable Long id) {
        return "redirect:/admin/shares/" + id + "/schemas";
    }
    
    @GetMapping("/{id}/edit")
    public String showEditForm(@PathVariable Long id, Model model) {
        try {
            model.addAttribute("share", shareService.getShareById(id));
            model.addAttribute("isEdit", true);
            return "admin/shares/form";
        } catch (Exception e) {
            return "redirect:/admin/shares";
        }
    }
    
    @PostMapping("/{id}")
    public String updateShare(@PathVariable Long id,
                             @Valid @ModelAttribute("share") DeltaShareDTO dto,
                             BindingResult result,
                             RedirectAttributes redirectAttributes,
                             Model model) {
        if (result.hasErrors()) {
            model.addAttribute("isEdit", true);
            return "admin/shares/form";
        }
        
        try {
            shareService.updateShare(id, dto);
            redirectAttributes.addFlashAttribute("successMessage", "Share updated successfully!");
            return "redirect:/admin/shares";
        } catch (Exception e) {
            model.addAttribute("errorMessage", e.getMessage());
            model.addAttribute("isEdit", true);
            return "admin/shares/form";
        }
    }
    
    @GetMapping("/{id}/delete")
    public String deleteShare(@PathVariable Long id, RedirectAttributes redirectAttributes) {
        try {
            shareService.deleteShare(id);
            redirectAttributes.addFlashAttribute("successMessage", "Share deleted successfully!");
        } catch (Exception e) {
            redirectAttributes.addFlashAttribute("errorMessage", e.getMessage());
        }
        return "redirect:/admin/shares";
    }
    
    @GetMapping("/{id}/schemas")
    public String listShareSchemas(@PathVariable Long id, Model model) {
        try {
            model.addAttribute("share", shareService.getShareById(id));
            model.addAttribute("schemas", schemaService.getSchemasByShareId(id));
            return "admin/shares/schemas";
        } catch (Exception e) {
            return "redirect:/admin/shares";
        }
    }
}
