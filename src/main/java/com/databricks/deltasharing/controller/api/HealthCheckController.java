package com.databricks.deltasharing.controller.api;

import com.databricks.deltasharing.service.storage.MinIOFileStorageService;
import io.minio.MinioClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * REST controller for system health checks
 * Provides endpoints to monitor the status of external services like MinIO
 */
@RestController
@RequestMapping("/api/health")
@Slf4j
public class HealthCheckController {
    
    @Autowired(required = false)
    private MinIOFileStorageService minioService;
    
    /**
     * Check MinIO service health
     * Result is cached for 60 seconds to avoid performance impact
     * 
     * @return Health check response with status and details
     */
    @GetMapping("/minio")
    @Cacheable(value = "minioHealthCheck", unless = "#result == null")
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
        
        // Try to perform a simple operation to check connectivity
        try {
            // Access the MinIO client via reflection (since it's private)
            java.lang.reflect.Field minioClientField = minioService.getClass().getDeclaredField("minioClient");
            minioClientField.setAccessible(true);
            MinioClient minioClient = (MinioClient) minioClientField.get(minioService);
            
            if (minioClient == null) {
                response.put("status", "error");
                response.put("message", "MinIO client is not initialized");
                return ResponseEntity.ok(response);
            }
            
            // Try to list buckets as a health check
            // This will fail if MinIO is not reachable
            minioClient.listBuckets();
            
            log.debug("MinIO health check: OK");
            response.put("status", "healthy");
            response.put("message", "MinIO service is operational");
            return ResponseEntity.ok(response);
            
        } catch (NoSuchFieldException e) {
            log.error("Failed to access MinIO client via reflection", e);
            response.put("status", "error");
            response.put("message", "Internal error accessing MinIO client");
            return ResponseEntity.ok(response);
        } catch (IllegalAccessException e) {
            log.error("Failed to access MinIO client field", e);
            response.put("status", "error");
            response.put("message", "Internal error accessing MinIO client");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.warn("MinIO health check failed: {}", e.getMessage());
            response.put("status", "unhealthy");
            response.put("message", "MinIO service is not responding: " + e.getMessage());
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
        
        // Get MinIO status
        ResponseEntity<Map<String, Object>> minioHealth = checkMinioHealth();
        Map<String, Object> minioStatus = minioHealth.getBody();
        
        response.put("services", Map.of("minio", minioStatus));
        
        // Determine overall status
        String overallStatus = "healthy";
        if ("unhealthy".equals(minioStatus.get("status")) || "error".equals(minioStatus.get("status"))) {
            overallStatus = "degraded";
        }
        
        response.put("status", overallStatus);
        
        return ResponseEntity.ok(response);
    }
}

