package com.databricks.deltasharing.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Configuration properties for file storage
 */
@Component
@ConfigurationProperties(prefix = "delta.sharing.storage")
@Data
public class StorageConfigProperties {
    private String type = "fake"; // default to fake storage
    private MinIOConfig minio = new MinIOConfig();
    private HttpConfig http = new HttpConfig();
    private FilesystemConfig filesystem = new FilesystemConfig();
    
    @Data
    public static class MinIOConfig {
        private String endpoint;
        private String accessKey;
        private String secretKey;
        private String bucket;
        private int urlExpirationMinutes = 60;
    }
    
    @Data
    public static class HttpConfig {
        private String basePath;
        private String baseUrl;
        private boolean useDeltaLog = true;
    }
    
    @Data
    public static class FilesystemConfig {
        private String basePath;
        private boolean useDeltaLog = true;
    }
    
    public MinIOConfig getMinio() {
        return minio;
    }
    
    public HttpConfig getHttp() {
        return http;
    }
    
    public FilesystemConfig getFilesystem() {
        return filesystem;
    }
}

