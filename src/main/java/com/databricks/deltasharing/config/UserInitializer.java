package com.databricks.deltasharing.config;

import com.databricks.deltasharing.model.User;
import com.databricks.deltasharing.repository.UserRepository;
import com.databricks.deltasharing.service.UserService;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.Environment;

import java.time.LocalDateTime;
import java.util.Arrays;

/**
 * User initializer for all profiles.
 * Creates default admin user on first startup.
 * 
 * In development profile:
 * - Creates admin user with API token (65 zeros)
 * - Enable API is activated
 * 
 * In production profile:
 * - Creates admin user without API token
 * - User must generate token manually
 * 
 * This initializer runs before DataInitializer (dev profile) to ensure
 * admin user is available for web interface authentication.
 */
@Configuration
@RequiredArgsConstructor
public class UserInitializer {
    
    private static final Logger log = LoggerFactory.getLogger(UserInitializer.class);
    
    private final UserService userService;
    private final UserRepository userRepository;
    private final Environment environment;
    
    @Value("${delta.sharing.token.expiration-days:365}")
    private long tokenExpirationDays;
    
    @Value("${delta.sharing.admin.default-password:#{null}}")
    private String defaultAdminPassword;
    
    // Default credentials
    private static final String DEFAULT_ADMIN_USERNAME = "admin";
    private static final String DEFAULT_ADMIN_FULLNAME = "Administrator";
    private static final String DEFAULT_ADMIN_EMAIL = "admin@deltasharing.local";
    private static final String DEFAULT_ADMIN_ROLE = "ADMIN";
    
    /**
     * Initialize default admin user
     * This bean is ordered to run before DataInitializer
     */
    @Bean
    @Order(1)
    public CommandLineRunner initializeDefaultUser() {
        return args -> {
            try {
                // Check if admin user already exists
                if (userService.findByUsername(DEFAULT_ADMIN_USERNAME).isPresent()) {
                    log.info("👤 Admin user already exists, skipping user initialization");
                    return;
                }
                
                boolean isDevelopment = Arrays.asList(environment.getActiveProfiles()).contains("dev");
                
                log.info("╔════════════════════════════════════════════════════════════════");
                log.info("║ Initializing Default Admin User");
                log.info("║ Profile: {}", isDevelopment ? "DEVELOPMENT" : "PRODUCTION");
                log.info("╚════════════════════════════════════════════════════════════════");
                
                // Validate password configuration in production
                if (!isDevelopment && (defaultAdminPassword == null || defaultAdminPassword.isEmpty())) {
                    String errorMsg = 
                        "\n\n" +
                        "╔════════════════════════════════════════════════════════════════\n" +
                        "║ ❌ CONFIGURATION ERROR\n" +
                        "╠════════════════════════════════════════════════════════════════\n" +
                        "║ The default admin password is not configured!\n" +
                        "║\n" +
                        "║ In PRODUCTION mode, you MUST configure the admin password:\n" +
                        "║\n" +
                        "║ Option 1: Set in application.yml (production profile)\n" +
                        "║   delta:\n" +
                        "║     sharing:\n" +
                        "║       admin:\n" +
                        "║         default-password: YourSecurePassword\n" +
                        "║\n" +
                        "║ Option 2: Set via environment variable\n" +
                        "║   export ADMIN_DEFAULT_PASSWORD=\"YourSecurePassword\"\n" +
                        "║\n" +
                        "║ Option 3: Set via JVM argument\n" +
                        "║   -Ddelta.sharing.admin.default-password=YourSecurePassword\n" +
                        "║\n" +
                        "║ Application startup ABORTED for security reasons.\n" +
                        "╚════════════════════════════════════════════════════════════════\n";
                    
                    log.error(errorMsg);
                    throw new IllegalStateException(
                        "Admin default password must be configured in production mode. " +
                        "Please set 'delta.sharing.admin.default-password' property or " +
                        "ADMIN_DEFAULT_PASSWORD environment variable."
                    );
                }
                
                // Create admin user with configurable password
                userService.createUser(
                    DEFAULT_ADMIN_USERNAME,
                    defaultAdminPassword,
                    DEFAULT_ADMIN_FULLNAME,
                    DEFAULT_ADMIN_EMAIL,
                    DEFAULT_ADMIN_ROLE
                );
                
                log.info("✅ Default admin user created successfully");
                
                // In development profile, create API token automatically
                if (isDevelopment) {
                    User adminUser = userRepository.findByUsername(DEFAULT_ADMIN_USERNAME)
                            .orElseThrow(() -> new RuntimeException("Admin user not found after creation"));
                    
                    // Generate token with 65 zeros
                    String devToken = "dss_" + "0".repeat(61); // 4 + 61 = 65 chars
                    
                    // Set token and expiration
                    adminUser.setApiToken(devToken);
                    adminUser.setTokenExpiresAt(LocalDateTime.now().plusDays(tokenExpirationDays));
                    adminUser.setActive(true); // Enable API
                    
                    userRepository.save(adminUser);
                    
                    log.info("✅ Development API token generated");
                    log.info("");
                    log.info("╔════════════════════════════════════════════════════════════════");
                    log.info("║ 🔐 DEFAULT WEB LOGIN CREDENTIALS");
                    log.info("╠════════════════════════════════════════════════════════════════");
                    log.info("║ Username: {}", DEFAULT_ADMIN_USERNAME);
                    log.info("║ Password: {}", defaultAdminPassword);
                    log.info("╠════════════════════════════════════════════════════════════════");
                    log.info("║ 🔑 DEVELOPMENT API TOKEN (65 zeros)");
                    log.info("╠════════════════════════════════════════════════════════════════");
                    log.info("║ Token:    {}", devToken);
                    log.info("║ Expires:  {}", adminUser.getTokenExpiresAt().toString());
                    log.info("╠════════════════════════════════════════════════════════════════");
                    log.info("║ ⚠️  DEVELOPMENT MODE:");
                    log.info("║ API token automatically generated for testing purposes.");
                    log.info("║ Please change the password and regenerate token in prod!");
                    log.info("╚════════════════════════════════════════════════════════════════");
                } else {
                    log.info("");
                    log.info("╔════════════════════════════════════════════════════════════════");
                    log.info("║ 🔐 DEFAULT WEB LOGIN CREDENTIALS");
                    log.info("╠════════════════════════════════════════════════════════════════");
                    log.info("║ Username: {}", DEFAULT_ADMIN_USERNAME);
                    log.info("║ Password: {}", defaultAdminPassword);
                    log.info("╠════════════════════════════════════════════════════════════════");
                    log.info("║ ⚠️  SECURITY WARNING:");
                    log.info("║ Please change the default password after first login!");
                    log.info("║ Generate API token manually in the web interface.");
                    log.info("╚════════════════════════════════════════════════════════════════");
                }
                log.info("");
                
            } catch (Exception e) {
                log.error("❌ Failed to initialize default admin user: {}", e.getMessage(), e);
                log.warn("⚠️  Application will continue, but you may not be able to login to the web interface");
            }
        };
    }
}

