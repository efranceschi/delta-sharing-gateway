package com.databricks.deltasharing.controller.api;

import com.databricks.deltasharing.service.TableCrawlerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/crawler")
@RequiredArgsConstructor
@Slf4j
public class CrawlerApiController {
    
    @Autowired(required = false)
    private TableCrawlerService crawlerService;
    
    @PostMapping("/trigger")
    public ResponseEntity<Map<String, Object>> triggerCrawler() {
        log.info("Manual crawler trigger requested");
        
        Map<String, Object> response = new HashMap<>();
        
        if (crawlerService == null) {
            response.put("success", false);
            response.put("message", "Crawler is not enabled. Enable it in application.yml");
            return ResponseEntity.ok(response);
        }
        
        try {
            // Trigger crawler asynchronously
            crawlerService.triggerManualCrawl();
            
            response.put("success", true);
            response.put("message", "Crawler started successfully. Check logs for progress.");
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("Error triggering crawler", e);
            response.put("success", false);
            response.put("message", "Error: " + e.getMessage());
            return ResponseEntity.status(500).body(response);
        }
    }
}

