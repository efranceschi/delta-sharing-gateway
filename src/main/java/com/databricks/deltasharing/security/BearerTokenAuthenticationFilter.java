package com.databricks.deltasharing.security;

import com.databricks.deltasharing.model.User;
import com.databricks.deltasharing.service.UserManagementService;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.lang.NonNull;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Collections;

/**
 * Authentication filter for Bearer Token validation
 * Validates API tokens against user database
 */
@Component
@Slf4j
public class BearerTokenAuthenticationFilter extends OncePerRequestFilter {
    
    private final UserManagementService userManagementService;
    
    // Constructor with @Lazy to break circular dependency
    public BearerTokenAuthenticationFilter(@Lazy UserManagementService userManagementService) {
        this.userManagementService = userManagementService;
    }
    
    @Value("${delta.sharing.auth.enabled:true}")
    private boolean authEnabled;
    
    @Value("${delta.sharing.auth.bearer-token:}")
    private String configuredBearerToken;
    
    @Override
    protected void doFilterInternal(@NonNull HttpServletRequest request, 
                                    @NonNull HttpServletResponse response, 
                                    @NonNull FilterChain filterChain) throws ServletException, IOException {
        
        String path = request.getRequestURI();
        
        // Skip authentication for non-Delta Sharing endpoints
        if (!path.startsWith("/delta-sharing")) {
            filterChain.doFilter(request, response);
            return;
        }
        
        // Skip authentication if disabled
        if (!authEnabled) {
            log.debug("Authentication disabled, allowing request");
            filterChain.doFilter(request, response);
            return;
        }
        
        // Extract Bearer token from Authorization header
        String authHeader = request.getHeader("Authorization");
        
        if (authHeader == null || !authHeader.startsWith("Bearer ")) {
            log.warn("Missing or invalid Authorization header for path: {}", path);
            sendUnauthorizedResponse(response, "Missing or invalid Authorization header");
            return;
        }
        
        String token = authHeader.substring(7); // Remove "Bearer " prefix
        
        // Validate token
        if (!isValidToken(token)) {
            log.warn("Invalid bearer token for path: {}", path);
            sendUnauthorizedResponse(response, "Invalid bearer token");
            return;
        }
        
        log.debug("Bearer token validated successfully for path: {}", path);
        
        // Set authentication in security context
        UsernamePasswordAuthenticationToken authentication = 
            new UsernamePasswordAuthenticationToken(
                "delta-sharing-user", 
                null, 
                Collections.singletonList(new SimpleGrantedAuthority("ROLE_USER"))
            );
        SecurityContextHolder.getContext().setAuthentication(authentication);
        
        try {
            filterChain.doFilter(request, response);
        } finally {
            // Clear security context after request
            SecurityContextHolder.clearContext();
        }
    }
    
    private boolean isValidToken(String token) {
        // First, check if there's a configured bearer token (backward compatibility)
        if (configuredBearerToken != null && !configuredBearerToken.isEmpty()) {
            if (configuredBearerToken.equals(token)) {
                log.debug("Token validated using configured bearer token");
                return true;
            }
        }
        
        // Then, validate against user database tokens
        try {
            User user = userManagementService.findByApiToken(token);
            
            if (user == null) {
                log.debug("Token not found in database");
                return false;
            }
            
            // Check if user has Enable API enabled
            if (!user.getActive()) {
                log.warn("API access disabled for user: {}", user.getUsername());
                return false;
            }
            
            // Check if token is expired
            if (user.getTokenExpiresAt() != null && 
                LocalDateTime.now().isAfter(user.getTokenExpiresAt())) {
                log.warn("Expired token for user: {}", user.getUsername());
                return false;
            }
            
            log.info("Token validated successfully for user: {}", user.getUsername());
            return true;
            
        } catch (Exception e) {
            log.error("Error validating token: {}", e.getMessage());
            return false;
        }
    }
    
    private void sendUnauthorizedResponse(HttpServletResponse response, String message) throws IOException {
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        response.setContentType("application/json");
        response.getWriter().write(String.format(
                "{\"errorCode\":\"UNAUTHENTICATED\",\"message\":\"%s\"}", message));
    }
}
