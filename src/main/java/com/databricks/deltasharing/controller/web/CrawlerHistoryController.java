package com.databricks.deltasharing.controller.web;

import com.databricks.deltasharing.config.TableCrawlerProperties;
import com.databricks.deltasharing.repository.CrawlerExecutionRepository;
import com.databricks.deltasharing.repository.DeltaTableRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("/admin/crawler")
@RequiredArgsConstructor
@Slf4j
public class CrawlerHistoryController {
    
    private final CrawlerExecutionRepository executionRepository;
    private final DeltaTableRepository tableRepository;
    
    @Autowired(required = false)
    private TableCrawlerProperties crawlerProperties;
    
    @GetMapping("/history")
    public String history(Model model) {
        log.info("Accessing crawler history");
        
        // Get all executions ordered by most recent first
        var executions = executionRepository.findAll()
                .stream()
                .sorted((a, b) -> b.getStartedAt().compareTo(a.getStartedAt()))
                .toList();
        
        // Get auto-discovered tables
        var autoDiscoveredTables = tableRepository.findAll()
                .stream()
                .filter(table -> table.getDiscoveredAt() != null && "crawler".equals(table.getDiscoveredBy()))
                .sorted((a, b) -> b.getDiscoveredAt().compareTo(a.getDiscoveredAt()))
                .toList();
        
        model.addAttribute("executions", executions);
        model.addAttribute("autoDiscoveredTables", autoDiscoveredTables);
        
        // Add crawler interval
        if (crawlerProperties != null) {
            model.addAttribute("crawlerIntervalMinutes", crawlerProperties.getIntervalMinutes());
        } else {
            model.addAttribute("crawlerIntervalMinutes", 5); // Default value
        }
        
        return "admin/crawler/history";
    }
}

