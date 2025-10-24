package com.databricks.deltasharing.controller.api;

import com.databricks.deltasharing.service.storage.MinIOFileStorageService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * REST controller for system health checks
 * Provides endpoints to monitor the status of external services like MinIO, Database, etc.
 */
@RestController
@RequestMapping("/api/health")
@Slf4j
public class HealthCheckController {
    
    @Autowired(required = false)
    private MinIOFileStorageService minioService;
    
    @Autowired
    private DataSource dataSource;
    
    /**
     * Check MinIO service health
     * 
     * @return Health check response with status and details
     */
    @GetMapping("/minio")
    public ResponseEntity<Map<String, Object>> checkMinioHealth() {
        Map<String, Object> response = new HashMap<>();
        response.put("service", "MinIO");
        response.put("timestamp", Instant.now().toString());
        
        // Check if MinIO service is configured
        if (minioService == null) {
            log.debug("MinIO service is not configured");
            response.put("status", "disabled");
            response.put("message", "MinIO service is not configured");
            return ResponseEntity.ok(response);
        }
        
        // Check if MinIO service is available
        if (!minioService.isAvailable()) {
            log.warn("MinIO service is configured but not available");
            response.put("status", "unavailable");
            response.put("message", "MinIO service is configured but not available");
            return ResponseEntity.ok(response);
        }
        
        // Test MinIO connection
        try {
            boolean isHealthy = minioService.testConnection();
            
            if (isHealthy) {
                log.debug("MinIO health check: OK");
                response.put("status", "healthy");
                response.put("message", "MinIO service is operational");
            } else {
                log.warn("MinIO health check failed");
                response.put("status", "unhealthy");
                response.put("message", "MinIO service is not responding");
            }
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.warn("MinIO health check failed: {}", e.getMessage());
            response.put("status", "unhealthy");
            response.put("message", "MinIO service error: " + e.getMessage());
            return ResponseEntity.ok(response);
        }
    }
    
    /**
     * Check database health
     * Result is cached for 60 seconds
     * 
     * @return Database health check response
     */
    @GetMapping("/database")
    @Cacheable(value = "databaseHealthCheck", unless = "#result == null")
    public ResponseEntity<Map<String, Object>> checkDatabaseHealth() {
        Map<String, Object> response = new HashMap<>();
        response.put("service", "Database");
        response.put("timestamp", Instant.now().toString());
        
        try (Connection connection = dataSource.getConnection()) {
            // Test connection with a simple query
            boolean isValid = connection.isValid(5); // 5 seconds timeout
            
            if (isValid) {
                response.put("status", "healthy");
                response.put("message", "Database connection is operational");
                
                // Get database metadata
                response.put("url", connection.getMetaData().getURL());
                response.put("product", connection.getMetaData().getDatabaseProductName());
                response.put("version", connection.getMetaData().getDatabaseProductVersion());
            } else {
                response.put("status", "unhealthy");
                response.put("message", "Database connection test failed");
            }
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.warn("Database health check failed: {}", e.getMessage());
            response.put("status", "unhealthy");
            response.put("message", "Cannot connect to database: " + e.getMessage());
            return ResponseEntity.ok(response);
        }
    }
    
    /**
     * Get JVM memory information
     * Result is cached for 10 seconds
     * 
     * @return JVM memory metrics
     */
    @GetMapping("/jvm")
    @Cacheable(value = "jvmHealthCheck", unless = "#result == null")
    public ResponseEntity<Map<String, Object>> getJvmInfo() {
        Map<String, Object> response = new HashMap<>();
        response.put("timestamp", Instant.now().toString());
        
        Runtime runtime = Runtime.getRuntime();
        
        long maxMemory = runtime.maxMemory();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory - freeMemory;
        
        Map<String, Object> memory = new HashMap<>();
        memory.put("max", maxMemory);
        memory.put("total", totalMemory);
        memory.put("used", usedMemory);
        memory.put("free", freeMemory);
        memory.put("usagePercent", (usedMemory * 100.0) / totalMemory);
        memory.put("maxUsagePercent", (usedMemory * 100.0) / maxMemory);
        
        response.put("memory", memory);
        response.put("processors", runtime.availableProcessors());
        
        // Determine status based on memory usage
        double usagePercent = (usedMemory * 100.0) / maxMemory;
        if (usagePercent > 90) {
            response.put("status", "critical");
        } else if (usagePercent > 75) {
            response.put("status", "warning");
        } else {
            response.put("status", "healthy");
        }
        
        return ResponseEntity.ok(response);
    }
    
    /**
     * Get MinIO cluster information
     * 
     * @return MinIO cluster metrics and health
     */
    @GetMapping("/minio/cluster")
    public ResponseEntity<Map<String, Object>> getMinioClusterInfo() {
        Map<String, Object> response = new HashMap<>();
        response.put("service", "MinIO Cluster");
        response.put("timestamp", Instant.now().toString());
        
        if (minioService == null || !minioService.isAvailable()) {
            response.put("status", "disabled");
            response.put("message", "MinIO service is not configured or available");
            return ResponseEntity.ok(response);
        }
        
        try {
            // Get cluster information from MinIO service
            Map<String, Object> clusterInfo = minioService.getClusterInfo();
            
            if (clusterInfo.containsKey("available") && !(Boolean) clusterInfo.get("available")) {
                response.put("status", "unhealthy");
                response.put("message", "MinIO cluster is not available");
                if (clusterInfo.containsKey("error")) {
                    response.put("error", clusterInfo.get("error"));
                }
            } else {
                response.put("status", "healthy");
                response.put("message", "MinIO cluster is operational");
                response.put("info", clusterInfo);
                response.put("note", "Detailed cluster metrics require MinIO Admin API credentials");
            }
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("Error getting MinIO cluster info: {}", e.getMessage());
            response.put("status", "error");
            response.put("message", "Internal error: " + e.getMessage());
            return ResponseEntity.ok(response);
        }
    }
    
    /**
     * Overall system health check
     * Aggregates health status from all services
     * 
     * @return Overall health status
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getSystemStatus() {
        Map<String, Object> response = new HashMap<>();
        response.put("timestamp", Instant.now().toString());
        response.put("application", "Delta Sharing Gateway");
        
        // Get all service statuses
        Map<String, Object> services = new HashMap<>();
        
        ResponseEntity<Map<String, Object>> minioHealth = checkMinioHealth();
        services.put("minio", minioHealth.getBody());
        
        ResponseEntity<Map<String, Object>> dbHealth = checkDatabaseHealth();
        services.put("database", dbHealth.getBody());
        
        ResponseEntity<Map<String, Object>> jvmHealth = getJvmInfo();
        services.put("jvm", jvmHealth.getBody());
        
        ResponseEntity<Map<String, Object>> minioCluster = getMinioClusterInfo();
        services.put("minioCluster", minioCluster.getBody());
        
        response.put("services", services);
        
        // Determine overall status
        String overallStatus = "healthy";
        for (Map.Entry<String, Object> entry : services.entrySet()) {
            @SuppressWarnings("unchecked")
            Map<String, Object> service = (Map<String, Object>) entry.getValue();
            String status = (String) service.get("status");
            
            if ("unhealthy".equals(status) || "error".equals(status) || "critical".equals(status)) {
                overallStatus = "degraded";
                break;
            } else if ("warning".equals(status) && !"degraded".equals(overallStatus)) {
                overallStatus = "warning";
            }
        }
        
        response.put("status", overallStatus);
        
        return ResponseEntity.ok(response);
    }
}

