package com.databricks.deltasharing.controller.web;

import com.databricks.deltasharing.config.TableCrawlerProperties;
import com.databricks.deltasharing.model.CrawlerExecution;
import com.databricks.deltasharing.repository.DeltaTableRepository;
import com.databricks.deltasharing.service.CrawlerStatusService;
import com.databricks.deltasharing.service.DeltaShareManagementService;
import com.databricks.deltasharing.service.DeltaSchemaManagementService;
import com.databricks.deltasharing.service.DeltaTableManagementService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

import java.util.Optional;

@Controller
@RequiredArgsConstructor
@Slf4j
public class DashboardController {
    
    private final DeltaShareManagementService shareService;
    private final DeltaSchemaManagementService schemaService;
    private final DeltaTableManagementService tableService;
    private final DeltaTableRepository tableRepository;
    
    @Autowired(required = false)
    private CrawlerStatusService crawlerStatusService;
    
    @Autowired(required = false)
    private TableCrawlerProperties crawlerProperties;
    
    @GetMapping("/")
    @Transactional(readOnly = true)
    public String dashboard(Model model) {
        log.info("Accessing dashboard");
        
        long sharesCount = shareService.getAllShares().size();
        long schemasCount = schemaService.getAllSchemas().size();
        long tablesCount = tableService.getAllTables().size();
        
        model.addAttribute("sharesCount", sharesCount);
        model.addAttribute("schemasCount", schemasCount);
        model.addAttribute("tablesCount", tablesCount);
        model.addAttribute("tables", tableRepository.findAll());
        model.addAttribute("appName", "Delta Sharing Gateway");
        model.addAttribute("version", "1.0.0");
        
        // Add crawler information if available
        if (crawlerStatusService != null) {
            Optional<CrawlerExecution> lastExecution = crawlerStatusService.getLastExecution();
            model.addAttribute("crawlerLastExecution", lastExecution.orElse(null));
            model.addAttribute("crawlerEnabled", lastExecution.isPresent());
            model.addAttribute("autoDiscoveredTablesCount", crawlerStatusService.countAutoDiscoveredTables());
            model.addAttribute("manualTablesCount", crawlerStatusService.countManualTables());
            
            // Add crawler interval if properties are available
            if (crawlerProperties != null) {
                model.addAttribute("crawlerIntervalMinutes", crawlerProperties.getIntervalMinutes());
            } else {
                model.addAttribute("crawlerIntervalMinutes", 5); // Default value
            }
        } else {
            model.addAttribute("crawlerEnabled", false);
            model.addAttribute("crawlerLastExecution", null);
            model.addAttribute("autoDiscoveredTablesCount", 0L);
            model.addAttribute("manualTablesCount", tablesCount);
            model.addAttribute("crawlerIntervalMinutes", 5); // Default value
        }
        
        return "dashboard";
    }
}
