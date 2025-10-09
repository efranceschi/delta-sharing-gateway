package com.databricks.deltasharing.controller.web;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

@Controller
@RequestMapping("/system")
@RequiredArgsConstructor
@Slf4j
public class SystemInformationController {
    
    private final Environment environment;
    
    @Value("${spring.application.name:delta-sharing-onprem}")
    private String applicationName;
    
    @Value("${server.port:8080}")
    private String serverPort;
    
    @Value("${delta.sharing.storage.type:fake}")
    private String storageType;
    
    @Value("${spring.datasource.url:}")
    private String datasourceUrl;
    
    @Value("${spring.jpa.hibernate.ddl-auto:update}")
    private String hibernateDdlAuto;
    
    @GetMapping("/information")
    public String systemInformation(Model model) {
        log.info("Accessing system information page");
        
        // Get active profile
        String[] activeProfiles = environment.getActiveProfiles();
        String activeProfile = activeProfiles.length > 0 ? activeProfiles[0] : "default";
        
        // Application Info
        Map<String, String> appInfo = new LinkedHashMap<>();
        appInfo.put("Application Name", applicationName);
        appInfo.put("Version", "1.0.0");
        appInfo.put("Active Profile", activeProfile);
        appInfo.put("Server Port", serverPort);
        
        // Database Info
        Map<String, String> dbInfo = new LinkedHashMap<>();
        String dbType = getDatabaseType(datasourceUrl);
        dbInfo.put("Database Type", dbType);
        dbInfo.put("Connection URL", maskSensitiveUrl(datasourceUrl));
        dbInfo.put("Hibernate DDL Auto", hibernateDdlAuto);
        
        // Storage Info
        Map<String, String> storageInfo = new LinkedHashMap<>();
        storageInfo.put("Storage Type", storageType.toUpperCase());
        
        if ("minio".equalsIgnoreCase(storageType)) {
            String minioEndpoint = environment.getProperty("delta.sharing.storage.minio.endpoint", "Not configured");
            String minioBucket = environment.getProperty("delta.sharing.storage.minio.bucket", "Not configured");
            String minioUseSSL = environment.getProperty("delta.sharing.storage.minio.use-ssl", "false");
            
            storageInfo.put("MinIO Endpoint", maskSensitiveUrl(minioEndpoint));
            storageInfo.put("Bucket", minioBucket);
            storageInfo.put("Use SSL", minioUseSSL);
        } else if ("fake".equalsIgnoreCase(storageType)) {
            String urlProtocol = environment.getProperty("delta.sharing.storage.fake.url-protocol", "http");
            String baseUrl = environment.getProperty("delta.sharing.storage.fake.base-url", "Not configured");
            
            storageInfo.put("URL Protocol", urlProtocol);
            storageInfo.put("Base URL", baseUrl);
        } else if ("http".equalsIgnoreCase(storageType)) {
            String httpBaseUrl = environment.getProperty("delta.sharing.storage.http.base-url", "Not configured");
            String httpBasePath = environment.getProperty("delta.sharing.storage.http.base-path", "Not configured");
            
            storageInfo.put("Base URL", httpBaseUrl);
            storageInfo.put("Base Path", httpBasePath);
        }
        
        // Security Info
        Map<String, String> securityInfo = new LinkedHashMap<>();
        String authEnabled = environment.getProperty("delta.sharing.auth.enabled", "false");
        String tokenConfigured = environment.getProperty("delta.sharing.auth.bearer-token", "").isEmpty() ? "No" : "Yes";
        
        securityInfo.put("Authentication Enabled", authEnabled);
        securityInfo.put("Bearer Token Configured", tokenConfigured);
        
        // System Info
        Map<String, String> systemInfo = new LinkedHashMap<>();
        systemInfo.put("Java Version", System.getProperty("java.version"));
        systemInfo.put("Java Vendor", System.getProperty("java.vendor"));
        systemInfo.put("OS Name", System.getProperty("os.name"));
        systemInfo.put("OS Version", System.getProperty("os.version"));
        systemInfo.put("OS Architecture", System.getProperty("os.arch"));
        
        // Runtime Info
        Runtime runtime = Runtime.getRuntime();
        long maxMemory = runtime.maxMemory() / (1024 * 1024); // MB
        long totalMemory = runtime.totalMemory() / (1024 * 1024); // MB
        long freeMemory = runtime.freeMemory() / (1024 * 1024); // MB
        long usedMemory = totalMemory - freeMemory;
        
        Map<String, String> runtimeInfo = new LinkedHashMap<>();
        runtimeInfo.put("Max Memory", maxMemory + " MB");
        runtimeInfo.put("Total Memory", totalMemory + " MB");
        runtimeInfo.put("Used Memory", usedMemory + " MB");
        runtimeInfo.put("Free Memory", freeMemory + " MB");
        runtimeInfo.put("Available Processors", String.valueOf(runtime.availableProcessors()));
        
        model.addAttribute("activeProfile", activeProfile);
        model.addAttribute("appInfo", appInfo);
        model.addAttribute("dbInfo", dbInfo);
        model.addAttribute("storageInfo", storageInfo);
        model.addAttribute("securityInfo", securityInfo);
        model.addAttribute("systemInfo", systemInfo);
        model.addAttribute("runtimeInfo", runtimeInfo);
        
        return "system/information";
    }
    
    private String getDatabaseType(String url) {
        if (url == null || url.isEmpty()) {
            return "Unknown";
        }
        if (url.contains("h2")) {
            return "H2 (In-Memory)";
        } else if (url.contains("postgresql")) {
            return "PostgreSQL";
        } else if (url.contains("mysql")) {
            return "MySQL";
        } else {
            return "Other";
        }
    }
    
    private String maskSensitiveUrl(String url) {
        if (url == null || url.isEmpty()) {
            return "Not configured";
        }
        
        // Mask passwords in JDBC URLs
        if (url.contains("password=")) {
            url = url.replaceAll("password=[^&;]+", "password=***");
        }
        
        // Mask credentials in URLs like https://user:pass@host
        if (url.matches(".*://[^@]+:[^@]+@.*")) {
            url = url.replaceAll("://[^@]+:[^@]+@", "://***:***@");
        }
        
        return url;
    }
}

