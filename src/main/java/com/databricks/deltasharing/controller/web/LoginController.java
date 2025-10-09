package com.databricks.deltasharing.controller.web;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.logout.SecurityContextLogoutHandler;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * Controller for login and logout operations
 */
@Controller
@RequiredArgsConstructor
@Slf4j
public class LoginController {
    
    /**
     * Show login page
     */
    @GetMapping("/login")
    public String login(
            @RequestParam(value = "error", required = false) String error,
            @RequestParam(value = "logout", required = false) String logout,
            Model model) {
        
        log.debug("Login page requested - error: {}, logout: {}", error, logout);
        
        if (error != null) {
            model.addAttribute("error", "Invalid username or password");
            log.warn("Login failed: Invalid credentials");
        }
        
        if (logout != null) {
            model.addAttribute("message", "You have been logged out successfully");
            log.info("User logged out successfully");
        }
        
        return "login";
    }
    
    /**
     * Perform logout
     */
    @GetMapping("/logout")
    public String logout(HttpServletRequest request) {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        
        if (auth != null) {
            String username = auth.getName();
            new SecurityContextLogoutHandler().logout(request, null, auth);
            log.info("User logged out: {}", username);
        }
        
        return "redirect:/login?logout=true";
    }
}

