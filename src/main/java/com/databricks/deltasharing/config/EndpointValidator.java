package com.databricks.deltasharing.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/**
 * Validates that the Delta Sharing endpoint FQDN is properly configured in production.
 * 
 * In development (dev profile):
 * - Allows default localhost endpoint
 * - No validation required
 * 
 * In production (non-dev profiles):
 * - Requires explicit endpoint configuration
 * - Fails startup if endpoint is localhost or not configured
 * - Ensures public accessibility
 */
@Slf4j
@Component
public class EndpointValidator implements CommandLineRunner {

    @Value("${delta.sharing.endpoint-fqdn:}")
    private String endpointFqdn;

    private final Environment environment;

    public EndpointValidator(Environment environment) {
        this.environment = environment;
    }

    @Override
    public void run(String... args) throws Exception {
        boolean isDev = isDevProfile();
        
        log.info("Validating Delta Sharing endpoint configuration...");
        log.info("Active profiles: {}", String.join(", ", environment.getActiveProfiles()));
        log.info("Current endpoint: {}", endpointFqdn);
        
        if (isDev) {
            // Development mode - allow localhost
            log.info("Running in DEVELOPMENT mode - localhost endpoint is allowed");
            return;
        }
        
        // Production mode - validate endpoint
        validateProductionEndpoint();
    }

    private boolean isDevProfile() {
        String[] activeProfiles = environment.getActiveProfiles();
        
        // If no profiles are active, check default profiles
        if (activeProfiles.length == 0) {
            activeProfiles = environment.getDefaultProfiles();
        }
        
        for (String profile : activeProfiles) {
            if ("dev".equalsIgnoreCase(profile) || 
                "development".equalsIgnoreCase(profile)) {
                return true;
            }
        }
        
        return false;
    }

    private void validateProductionEndpoint() {
        // Check if endpoint is configured
        if (endpointFqdn == null || endpointFqdn.trim().isEmpty()) {
            String errorMessage = 
                "╔════════════════════════════════════════════════════════════════\n" +
                "║ CONFIGURATION ERROR: Delta Sharing Endpoint Not Configured\n" +
                "╠════════════════════════════════════════════════════════════════\n" +
                "║\n" +
                "║ The Delta Sharing endpoint FQDN is NOT configured.\n" +
                "║ This parameter is REQUIRED in production environments.\n" +
                "║\n" +
                "║ Configure the endpoint via:\n" +
                "║\n" +
                "║ 1. Environment Variable:\n" +
                "║    export DELTA_SHARING_ENDPOINT=https://your-domain.com/...\n" +
                "║\n" +
                "║ 2. application.yml:\n" +
                "║    delta:\n" +
                "║      sharing:\n" +
                "║        endpoint-fqdn: https://your-domain.com/delta-sharing\n" +
                "║\n" +
                "║ Example:\n" +
                "║    endpoint-fqdn: https://data.company.com/delta-sharing\n" +
                "║\n" +
                "╚════════════════════════════════════════════════════════════════";
            
            log.error(errorMessage);
            throw new IllegalStateException(
                "Delta Sharing endpoint FQDN is not configured. " +
                "Set DELTA_SHARING_ENDPOINT environment variable or configure " +
                "delta.sharing.endpoint-fqdn in application.yml"
            );
        }
        
        // Check if endpoint is localhost (not allowed in production)
        String lowerEndpoint = endpointFqdn.toLowerCase();
        if (lowerEndpoint.contains("localhost") || 
            lowerEndpoint.contains("127.0.0.1") ||
            lowerEndpoint.contains("0.0.0.0")) {
            
            String errorMessage = 
                "╔════════════════════════════════════════════════════════════════\n" +
                "║ CONFIGURATION ERROR: Localhost Endpoint in Production\n" +
                "╠════════════════════════════════════════════════════════════════\n" +
                "║\n" +
                "║ The Delta Sharing endpoint is configured with localhost.\n" +
                "║ Localhost endpoints are NOT ALLOWED in production.\n" +
                "║\n" +
                "║ Current endpoint: " + endpointFqdn + "\n" +
                "║\n" +
                "║ Please configure a PUBLIC endpoint:\n" +
                "║\n" +
                "║ export DELTA_SHARING_ENDPOINT=https://your-domain.com/...\n" +
                "║\n" +
                "║ Example:\n" +
                "║    endpoint-fqdn: https://data.company.com/delta-sharing\n" +
                "║\n" +
                "╚════════════════════════════════════════════════════════════════";
            
            log.error(errorMessage);
            throw new IllegalStateException(
                "Delta Sharing endpoint cannot use localhost in production. " +
                "Configure a public FQDN via DELTA_SHARING_ENDPOINT environment variable."
            );
        }
        
        // Additional validation: must be HTTP/HTTPS
        if (!lowerEndpoint.startsWith("http://") && !lowerEndpoint.startsWith("https://")) {
            String errorMessage = 
                "╔════════════════════════════════════════════════════════════════\n" +
                "║ CONFIGURATION ERROR: Invalid Endpoint Protocol\n" +
                "╠════════════════════════════════════════════════════════════════\n" +
                "║\n" +
                "║ The Delta Sharing endpoint must start with http:// or https://\n" +
                "║\n" +
                "║ Current endpoint: " + endpointFqdn + "\n" +
                "║\n" +
                "║ Correct format:\n" +
                "║    https://your-domain.com/delta-sharing\n" +
                "║\n" +
                "╚════════════════════════════════════════════════════════════════";
            
            log.error(errorMessage);
            throw new IllegalStateException(
                "Delta Sharing endpoint must start with http:// or https://"
            );
        }
        
        log.info("✅ Delta Sharing endpoint validation PASSED");
        log.info("   Endpoint: {}", endpointFqdn);
    }
}

