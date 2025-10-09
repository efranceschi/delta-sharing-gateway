package com.databricks.deltasharing.controller.web;

import com.databricks.deltasharing.dto.UserDTO;
import com.databricks.deltasharing.service.UserManagementService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

@Controller
@RequestMapping("/system/users")
@RequiredArgsConstructor
@Slf4j
public class UserManagementController {
    
    private final UserManagementService userService;
    
    @Value("${delta.sharing.endpoint-fqdn}")
    private String deltaSharingEndpoint;
    
    @GetMapping
    public String listUsers(Model model) {
        model.addAttribute("users", userService.getAllUsers());
        return "system/users/list";
    }
    
    @GetMapping("/new")
    @PreAuthorize("hasRole('ADMIN')")
    public String showCreateForm(Model model) {
        model.addAttribute("user", new UserDTO());
        model.addAttribute("isEdit", false);
        return "system/users/form";
    }
    
    @PostMapping
    @PreAuthorize("hasRole('ADMIN')")
    public String createUser(@Valid @ModelAttribute("user") UserDTO dto,
                            BindingResult result,
                            RedirectAttributes redirectAttributes,
                            Model model) {
        if (result.hasErrors()) {
            model.addAttribute("isEdit", false);
            return "system/users/form";
        }
        
        try {
            userService.createUser(dto);
            redirectAttributes.addFlashAttribute("successMessage", "User created successfully!");
            return "redirect:/system/users";
        } catch (Exception e) {
            model.addAttribute("errorMessage", e.getMessage());
            model.addAttribute("isEdit", false);
            return "system/users/form";
        }
    }
    
    @GetMapping("/{id}/edit")
    public String showEditForm(@PathVariable Long id, Model model) {
        try {
            UserDTO user = userService.getUserById(id);
            model.addAttribute("user", user);
            model.addAttribute("isEdit", true);
            return "system/users/form";
        } catch (Exception e) {
            return "redirect:/system/users";
        }
    }
    
    @PostMapping("/{id}")
    public String updateUser(@PathVariable Long id,
                            @Valid @ModelAttribute("user") UserDTO dto,
                            BindingResult result,
                            RedirectAttributes redirectAttributes,
                            Model model) {
        if (result.hasErrors()) {
            model.addAttribute("isEdit", true);
            return "system/users/form";
        }
        
        try {
            userService.updateUser(id, dto);
            redirectAttributes.addFlashAttribute("successMessage", "User updated successfully!");
            return "redirect:/system/users";
        } catch (Exception e) {
            model.addAttribute("errorMessage", e.getMessage());
            model.addAttribute("isEdit", true);
            return "system/users/form";
        }
    }
    
    @GetMapping("/{id}/delete")
    public String deleteUser(@PathVariable Long id, RedirectAttributes redirectAttributes) {
        try {
            userService.deleteUser(id);
            redirectAttributes.addFlashAttribute("successMessage", "User deleted successfully!");
        } catch (Exception e) {
            redirectAttributes.addFlashAttribute("errorMessage", e.getMessage());
        }
        return "redirect:/system/users";
    }
    
    @PostMapping("/{id}/generate-token")
    @ResponseBody
    public String generateToken(@PathVariable Long id) {
        try {
            return userService.generateApiToken(id);
        } catch (Exception e) {
            log.error("Error generating token", e);
            throw new RuntimeException("Error generating token: " + e.getMessage());
        }
    }
    
    @PostMapping("/{id}/revoke-token")
    public String revokeToken(@PathVariable Long id, RedirectAttributes redirectAttributes) {
        try {
            userService.revokeApiToken(id);
            redirectAttributes.addFlashAttribute("successMessage", "API token revoked successfully!");
        } catch (Exception e) {
            redirectAttributes.addFlashAttribute("errorMessage", e.getMessage());
        }
        return "redirect:/system/users";
    }
    
    @GetMapping("/{id}/download-credentials")
    public ResponseEntity<String> downloadCredentials(@PathVariable Long id) {
        try {
            UserDTO user = userService.getUserById(id);
            
            // Check if user has an active token
            if (user.getApiToken() == null || user.getApiToken().isEmpty()) {
                log.warn("User {} attempted to download credentials without an active token", user.getUsername());
                return ResponseEntity.badRequest()
                        .body("User does not have an active API token. Please generate a token first.");
            }
            
            // Check if token is expired
            if (user.getTokenExpiresAt() != null && 
                user.getTokenExpiresAt().isBefore(java.time.LocalDateTime.now())) {
                log.warn("User {} attempted to download credentials with expired token", user.getUsername());
                return ResponseEntity.badRequest()
                        .body("API token has expired. Please regenerate the token.");
            }
            
            // Check if API access is enabled
            if (!user.getActive()) {
                log.warn("User {} attempted to download credentials with API access disabled", user.getUsername());
                return ResponseEntity.badRequest()
                        .body("API access is disabled for this user. Please enable 'Enable API' first.");
            }
            
            // Generate credential file in config.share format
            String credentialFile = String.format(
                "{\n" +
                "  \"shareCredentialsVersion\": 1,\n" +
                "  \"endpoint\": \"%s\",\n" +
                "  \"bearerToken\": \"%s\"\n" +
                "}\n",
                deltaSharingEndpoint,
                user.getApiToken()
            );
            
            // Generate filename with username
            String filename = String.format("%s-credentials.share", user.getUsername());
            
            log.info("User {} downloaded credentials file", user.getUsername());
            
            return ResponseEntity.ok()
                    .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(credentialFile);
            
        } catch (Exception e) {
            log.error("Error generating credential file for user {}", id, e);
            return ResponseEntity.internalServerError()
                    .body("Error generating credential file: " + e.getMessage());
        }
    }
}

