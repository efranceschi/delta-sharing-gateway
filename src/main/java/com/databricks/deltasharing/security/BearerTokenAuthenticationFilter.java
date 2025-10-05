package com.databricks.deltasharing.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.lang.NonNull;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Collections;

/**
 * Authentication filter for Bearer Token validation
 * Based on Delta Sharing Protocol authentication specification
 */
@Component
@Slf4j
public class BearerTokenAuthenticationFilter extends OncePerRequestFilter {
    
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
        // Simple token validation - in production, validate against database or JWT
        if (configuredBearerToken == null || configuredBearerToken.isEmpty()) {
            // If no token configured, accept any non-empty token (development mode)
            log.warn("No bearer token configured - accepting any token (DEVELOPMENT MODE)");
            return token != null && !token.isEmpty();
        }
        
        return configuredBearerToken.equals(token);
    }
    
    private void sendUnauthorizedResponse(HttpServletResponse response, String message) throws IOException {
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        response.setContentType("application/json");
        response.getWriter().write(String.format(
                "{\"errorCode\":\"UNAUTHENTICATED\",\"message\":\"%s\"}", message));
    }
}
