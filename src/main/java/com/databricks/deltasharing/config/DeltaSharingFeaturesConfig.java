package com.databricks.deltasharing.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration for Delta Sharing advanced features
 * 
 * This configuration controls which advanced Delta Lake features are enabled
 * in the Delta Sharing server. Features can be enabled/disabled via application.yml
 * or environment variables.
 */
@Configuration
@ConfigurationProperties(prefix = "delta.sharing.features")
@Data
public class DeltaSharingFeaturesConfig {
    
    /**
     * Deletion Vectors configuration
     */
    private DeletionVectors deletionVectors = new DeletionVectors();
    
    @Data
    public static class DeletionVectors {
        /**
         * Enable Deletion Vectors support
         * 
         * When enabled: Server will respect readerfeatures=deletionvectors from clients
         *               and return Delta format responses with DeletionVector information
         * 
         * When disabled: Server will ignore deletionvectors requests and always return
         *                standard format without DeletionVector information
         * 
         * Default: false (disabled for broader compatibility)
         */
        private boolean enabled = false;
    }
    
    /**
     * Check if Deletion Vectors feature is enabled
     * 
     * @return true if deletion vectors are enabled, false otherwise
     */
    public boolean isDeletionVectorsEnabled() {
        return deletionVectors.isEnabled();
    }
}

