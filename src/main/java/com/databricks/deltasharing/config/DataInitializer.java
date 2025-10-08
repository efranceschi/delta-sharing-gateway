package com.databricks.deltasharing.config;

import com.databricks.deltasharing.model.DeltaSchema;
import com.databricks.deltasharing.model.DeltaShare;
import com.databricks.deltasharing.model.DeltaTable;
import com.databricks.deltasharing.repository.DeltaSchemaRepository;
import com.databricks.deltasharing.repository.DeltaShareRepository;
import com.databricks.deltasharing.repository.DeltaTableRepository;
import lombok.RequiredArgsConstructor;
import net.datafaker.Faker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Data initializer for development profile.
 * Populates the database with dynamically generated sample data using Faker library.
 * 
 * This class is only active when the 'dev' profile is enabled.
 * To enable: run with -Dspring.profiles.active=dev or set SPRING_PROFILES_ACTIVE=dev
 * 
 * Configuration:
 * - 10 Shares
 * - 3-7 Schemas per Share
 * - 10-50 Tables per Schema
 */
@Configuration
@Profile("dev")
@RequiredArgsConstructor
public class DataInitializer {

    private static final Logger log = LoggerFactory.getLogger(DataInitializer.class);

    private final DeltaShareRepository shareRepository;
    private final DeltaSchemaRepository schemaRepository;
    private final DeltaTableRepository tableRepository;
    
    private static final int TOTAL_SHARES = 5;
    private static final int MIN_SCHEMAS_PER_SHARE = 1;
    private static final int MAX_SCHEMAS_PER_SHARE = 3;
    private static final int MIN_TABLES_PER_SCHEMA = 3;
    private static final int MAX_TABLES_PER_SCHEMA = 10;
    
    private final Faker faker = new Faker();
    private final Random random = new Random();

    @Bean
    public CommandLineRunner initializeData() {
        return args -> {
            log.info("╔════════════════════════════════════════════════════════════════╗");
            log.info("║  Initializing Dynamic Sample Data (dev profile)             ║");
            log.info("╠════════════════════════════════════════════════════════════════╣");
            log.info("║  Configuration:                                              ║");
            log.info("║  - Shares: {}                                                ║", TOTAL_SHARES);
            log.info("║  - Schemas per Share: {}-{}                                  ║", MIN_SCHEMAS_PER_SHARE, MAX_SCHEMAS_PER_SHARE);
            log.info("║  - Tables per Schema: {}-{}                                 ║", MIN_TABLES_PER_SCHEMA, MAX_TABLES_PER_SCHEMA);
            log.info("╚════════════════════════════════════════════════════════════════╝");
            
            // Check if data already exists
            if (shareRepository.count() > 0) {
                log.info("Database already contains data. Skipping initialization.");
                return;
            }
            
            long startTime = System.currentTimeMillis();
            
            // Generate shares
            List<DeltaShare> shares = generateShares();
            log.info("✅ Created {} shares", shares.size());
            
            // Generate schemas for each share
            int totalSchemas = 0;
            for (DeltaShare share : shares) {
                int schemaCount = random.nextInt(MAX_SCHEMAS_PER_SHARE - MIN_SCHEMAS_PER_SHARE + 1) + MIN_SCHEMAS_PER_SHARE;
                List<DeltaSchema> schemas = generateSchemas(share, schemaCount);
                totalSchemas += schemas.size();
                
                // Generate tables for each schema
                for (DeltaSchema schema : schemas) {
                    int tableCount = random.nextInt(MAX_TABLES_PER_SCHEMA - MIN_TABLES_PER_SCHEMA + 1) + MIN_TABLES_PER_SCHEMA;
                    generateTables(schema, tableCount);
                }
            }
            
            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            
            log.info("✅ Created {} schemas", totalSchemas);
            log.info("✅ Created {} tables", tableRepository.count());
            log.info("⏱️  Initialization completed in {} ms", duration);
            
            logDataSummary();
        };
    }
    
    /**
     * Generate fake shares
     */
    private List<DeltaShare> generateShares() {
        List<DeltaShare> shares = new ArrayList<>();
        
        for (int i = 0; i < TOTAL_SHARES; i++) {
            String name = generateShareName(i);
            String description = faker.company().catchPhrase() + " - " + faker.company().bs();
            boolean active = random.nextDouble() > 0.1; // 90% active
            
            DeltaShare share = createShare(name, description, active);
            shares.add(share);
        }
        
        return shares;
    }
    
    /**
     * Generate a unique share name
     */
    private String generateShareName(int index) {
        String[] prefixes = {
            "analytics", "sales", "marketing", "finance", "operations",
            "customer", "product", "supply-chain", "hr", "engineering"
        };
        
        if (index < prefixes.length) {
            return prefixes[index] + "-share";
        }
        
        // For additional shares, use faker
        return faker.company().industry().toLowerCase().replaceAll(" ", "-") + "-share";
    }
    
    /**
     * Generate fake schemas for a share
     */
    private List<DeltaSchema> generateSchemas(DeltaShare share, int count) {
        List<DeltaSchema> schemas = new ArrayList<>();
        
        for (int i = 0; i < count; i++) {
            String name = generateSchemaName(i);
            String description = faker.lorem().sentence(8);
            
            DeltaSchema schema = createSchema(name, description, share);
            schemas.add(schema);
        }
        
        return schemas;
    }
    
    /**
     * Generate a unique schema name
     */
    private String generateSchemaName(int index) {
        String[] names = {
            "default", "staging", "production", "analytics", "warehouse",
            "raw", "processed", "curated", "archive", "sandbox"
        };
        
        if (index < names.length) {
            return names[index];
        }
        
        // For additional schemas
        return faker.app().name().toLowerCase().replaceAll(" ", "_");
    }
    
    /**
     * Generate fake tables for a schema
     */
    private void generateTables(DeltaSchema schema, int count) {
        List<String> usedNames = new ArrayList<>();
        
        for (int i = 0; i < count; i++) {
            String name = generateUniqueTableName(usedNames);
            usedNames.add(name);
            
            String description = faker.commerce().productName() + " - " + faker.lorem().sentence(5);
            boolean shareAsView = random.nextDouble() > 0.8; // 20% as views
            String location = generateLocation(schema, name);
            String format = generateFormat();
            
            createTable(name, description, schema, shareAsView, location, format);
        }
    }
    
    /**
     * Generate a realistic table name
     */
    private String generateTableName() {
        String[] prefixes = {"fact", "dim", "ref", "staging", "raw", "proc", "agg"};
        String[] entities = {
            "customers", "orders", "products", "transactions", "sales",
            "users", "inventory", "shipments", "returns", "payments",
            "events", "sessions", "clicks", "conversions", "campaigns",
            "employees", "departments", "projects", "tasks", "issues",
            "invoices", "receipts", "contracts", "subscriptions", "renewals"
        };
        
        String prefix = prefixes[random.nextInt(prefixes.length)];
        String entity = entities[random.nextInt(entities.length)];
        
        // Sometimes add a suffix
        if (random.nextDouble() > 0.7) {
            String[] suffixes = {"daily", "weekly", "monthly", "yearly", "snapshot", "delta", "hist"};
            return prefix + "_" + entity + "_" + suffixes[random.nextInt(suffixes.length)];
        }
        
        return prefix + "_" + entity;
    }
    
    /**
     * Generate a unique table name (within a schema)
     */
    private String generateUniqueTableName(List<String> usedNames) {
        String name;
        int attempts = 0;
        int maxAttempts = 100;
        
        do {
            name = generateTableName();
            attempts++;
            
            // If we've tried too many times, append a unique suffix
            if (attempts >= maxAttempts) {
                name = name + "_" + System.currentTimeMillis() % 10000;
                break;
            }
        } while (usedNames.contains(name));
        
        return name;
    }
    
    /**
     * Generate a realistic data location path
     */
    private String generateLocation(DeltaSchema schema, String tableName) {
        String[] basePaths = {"/data", "/lake", "/warehouse", "/storage", "/datalake"};
        String basePath = basePaths[random.nextInt(basePaths.length)];
        
        return String.format("%s/%s/%s/%s",
            basePath,
            schema.getShare().getName().replace("-share", ""),
            schema.getName(),
            tableName);
    }
    
    /**
     * Generate a data format (delta, parquet)
     * According to Delta Sharing protocol specification, only delta and parquet are supported
     */
    private String generateFormat() {
        String[] formats = {"delta", "parquet", "parquet", "parquet"}; // parquet more common
        return formats[random.nextInt(formats.length)];
    }
    
    /**
     * Create and save a share
     */
    private DeltaShare createShare(String name, String description, boolean active) {
        DeltaShare share = new DeltaShare();
        share.setName(name);
        share.setDescription(description);
        share.setActive(active);
        share.setCreatedAt(LocalDateTime.now());
        share.setUpdatedAt(LocalDateTime.now());
        
        DeltaShare saved = shareRepository.save(share);
        log.debug("Created share: {} (ID: {})", saved.getName(), saved.getId());
        return saved;
    }
    
    /**
     * Create and save a schema
     */
    private DeltaSchema createSchema(String name, String description, DeltaShare share) {
        DeltaSchema schema = new DeltaSchema();
        schema.setName(name);
        schema.setDescription(description);
        schema.setShare(share);
        schema.setCreatedAt(LocalDateTime.now());
        schema.setUpdatedAt(LocalDateTime.now());
        
        DeltaSchema saved = schemaRepository.save(schema);
        log.debug("Created schema: {} in share: {} (ID: {})", saved.getName(), share.getName(), saved.getId());
        return saved;
    }
    
    /**
     * Create and save a table
     */
    private DeltaTable createTable(String name, String description, DeltaSchema schema, 
                                   boolean shareAsView, String location, String format) {
        DeltaTable table = new DeltaTable();
        table.setName(name);
        table.setDescription(description);
        table.setSchema(schema);
        table.setShareAsView(shareAsView);
        table.setLocation(location);
        table.setFormat(format);
        table.setCreatedAt(LocalDateTime.now());
        table.setUpdatedAt(LocalDateTime.now());
        
        DeltaTable saved = tableRepository.save(table);
        log.trace("Created table: {} in schema: {} (ID: {})", saved.getName(), schema.getName(), saved.getId());
        return saved;
    }
    
    /**
     * Log a summary of generated data
     */
    @Transactional(readOnly = true)
    private void logDataSummary() {
        log.info("╔════════════════════════════════════════════════════════════════╗");
        log.info("║           Delta Sharing OnPrem - Data Summary                ║");
        log.info("╠════════════════════════════════════════════════════════════════╣");
        log.info("║  Total Shares:  {:<44} ║", shareRepository.count());
        log.info("║  Total Schemas: {:<44} ║", schemaRepository.count());
        log.info("║  Total Tables:  {:<44} ║", tableRepository.count());
        log.info("╚════════════════════════════════════════════════════════════════╝");
        
        // Log sample of created shares
        log.info("Sample of generated data:");
        
        // Load shares with schemas eagerly
        List<DeltaShare> shares = shareRepository.findAll();
        shares.stream()
            .limit(3)
            .forEach(share -> {
                // Count schemas for this share using repository query
                long schemaCount = schemaRepository.count();
                log.info("  📦 Share: {} ({} schemas in total)", share.getName(), schemaCount);
                
                // Get schemas for this share
                List<DeltaSchema> schemas = schemaRepository.findAll().stream()
                    .filter(s -> s.getShare().getId().equals(share.getId()))
                    .limit(2)
                    .toList();
                
                for (DeltaSchema schema : schemas) {
                    // Count tables for this schema
                    long tableCount = tableRepository.findAll().stream()
                        .filter(t -> t.getSchema().getId().equals(schema.getId()))
                        .count();
                    
                    log.info("    📁 Schema: {} ({} tables)", schema.getName(), tableCount);
                    
                    // Get sample tables
                    List<DeltaTable> tables = tableRepository.findAll().stream()
                        .filter(t -> t.getSchema().getId().equals(schema.getId()))
                        .limit(3)
                        .toList();
                    
                    for (DeltaTable table : tables) {
                        log.info("      📋 Table: {} [{}]", table.getName(), table.getFormat());
                    }
                    
                    if (tableCount > 3) {
                        log.info("      ... and {} more tables", tableCount - 3);
                    }
                }
                
                if (schemaCount > 2) {
                    log.info("    ... and {} more schemas", schemaCount - 2);
                }
            });
        
        if (shareRepository.count() > 3) {
            log.info("  ... and {} more shares", shareRepository.count() - 3);
        }
        
        log.info("✅ Sample data generation completed successfully!");
    }
}
