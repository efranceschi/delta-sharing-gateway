package com.databricks.deltasharing.controller.api;

import com.databricks.deltasharing.service.TableCrawlerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/crawler")
@RequiredArgsConstructor
@Slf4j
public class CrawlerApiController {
    
    @Autowired(required = false)
    private TableCrawlerService crawlerService;
    
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getCrawlerStatus() {
        Map<String, Object> response = new HashMap<>();
        
        if (crawlerService == null) {
            response.put("enabled", false);
            response.put("running", false);
            response.put("message", "Crawler is not enabled");
            return ResponseEntity.ok(response);
        }
        
        response.put("enabled", true);
        response.put("running", crawlerService.isRunning());
        
        LocalDateTime lastFinish = crawlerService.getLastExecutionFinishedAt();
        if (lastFinish != null) {
            response.put("lastExecutionFinishedAt", lastFinish.toString());
        }
        
        return ResponseEntity.ok(response);
    }
    
    @PostMapping("/trigger")
    public ResponseEntity<Map<String, Object>> triggerCrawler() {
        log.info("Manual crawler trigger requested via API");
        
        Map<String, Object> response = new HashMap<>();
        
        if (crawlerService == null) {
            response.put("success", false);
            response.put("message", "Crawler is not enabled. Enable it in application.yml");
            return ResponseEntity.ok(response);
        }
        
        try {
            // Trigger crawler asynchronously
            boolean started = crawlerService.triggerManualCrawl();
            
            if (started) {
                response.put("success", true);
                response.put("message", "Crawler started successfully. Check logs for progress.");
                return ResponseEntity.ok(response);
            } else {
                response.put("success", false);
                response.put("message", "Crawler is already running. Please wait for the current execution to finish.");
                return ResponseEntity.ok(response);
            }
            
        } catch (Exception e) {
            log.error("Error triggering crawler", e);
            response.put("success", false);
            response.put("message", "Error: " + e.getMessage());
            return ResponseEntity.status(500).body(response);
        }
    }
}

