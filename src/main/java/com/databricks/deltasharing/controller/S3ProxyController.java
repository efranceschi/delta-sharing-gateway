package com.databricks.deltasharing.controller;

import com.databricks.deltasharing.service.storage.FakeFileStorageService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * S3 Proxy Controller - Emulates minimal S3 API for Delta Kernel compatibility
 * 
 * This controller exposes Parquet files through an S3-compatible interface,
 * allowing Delta Kernel Rust to access files using s3:// URLs.
 * 
 * Only active when FakeFileStorageService is enabled.
 */
@RestController
@RequestMapping("/s3-proxy")
@RequiredArgsConstructor
@Slf4j
@ConditionalOnBean(FakeFileStorageService.class)
public class S3ProxyController {

    private final FakeFileStorageService fakeFileStorageService;
    
    private static final String BUCKET_NAME = "delta-sharing-fake";

    /**
     * GET /{bucket}/{key}
     * Emulates: AWS S3 GetObject operation
     * 
     * This is the core operation that Delta Kernel uses to download Parquet files.
     */
    @GetMapping("/{bucket}/{key:.+}")
    public ResponseEntity<Resource> getObject(
            @PathVariable String bucket,
            @PathVariable String key,
            @RequestHeader(value = "Range", required = false) String range) {
        
        log.debug("S3 Proxy: GET /{}/{} (Range: {})", bucket, key, range);
        
        // Validate bucket
        if (!BUCKET_NAME.equals(bucket)) {
            log.warn("Invalid bucket requested: {}", bucket);
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .header("x-amz-request-id", generateRequestId())
                    .body(null);
        }
        
        // Validate key to prevent path traversal
        if (key.contains("..") || key.contains("//")) {
            log.warn("Invalid key requested: {}", key);
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .header("x-amz-request-id", generateRequestId())
                    .body(null);
        }
        
        // Get file from FakeFileStorageService
        File file = fakeFileStorageService.getGeneratedFile(key);
        
        if (file == null || !file.exists()) {
            log.warn("File not found: {}", key);
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .header("x-amz-request-id", generateRequestId())
                    .header("x-amz-id-2", "fake-id-2")
                    .body(null);
        }
        
        try {
            // Prepare response with S3-compatible headers
            Resource resource = new FileSystemResource(file);
            
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);
            headers.setContentLength(file.length());
            
            // S3-specific headers
            headers.add("x-amz-request-id", generateRequestId());
            headers.add("x-amz-id-2", "fake-id-2");
            headers.add("ETag", "\"" + generateETag(file) + "\"");
            headers.add("Last-Modified", formatS3Date(file.lastModified()));
            headers.add("Accept-Ranges", "bytes");
            headers.add("Server", "DeltaSharingS3Proxy");
            
            // Handle range requests (important for Delta Kernel)
            if (range != null && range.startsWith("bytes=")) {
                log.debug("Range request not fully implemented, returning full file");
                // For now, return full file. Delta Kernel should handle this.
            }
            
            log.info("S3 Proxy: Serving {}/{} ({} bytes)", bucket, key, file.length());
            
            return ResponseEntity.ok()
                    .headers(headers)
                    .body(resource);
                    
        } catch (Exception e) {
            log.error("Error serving file: {}", key, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .header("x-amz-request-id", generateRequestId())
                    .body(null);
        }
    }
    
    /**
     * HEAD /{bucket}/{key}
     * Emulates: AWS S3 HeadObject operation
     * 
     * Delta Kernel may use this to check if file exists and get metadata.
     */
    @RequestMapping(value = "/{bucket}/{key:.+}", method = RequestMethod.HEAD)
    public ResponseEntity<Void> headObject(
            @PathVariable String bucket,
            @PathVariable String key) {
        
        log.debug("S3 Proxy: HEAD /{}/{}", bucket, key);
        
        if (!BUCKET_NAME.equals(bucket)) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .header("x-amz-request-id", generateRequestId())
                    .build();
        }
        
        File file = fakeFileStorageService.getGeneratedFile(key);
        
        if (file == null || !file.exists()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .header("x-amz-request-id", generateRequestId())
                    .build();
        }
        
        HttpHeaders headers = new HttpHeaders();
        headers.setContentLength(file.length());
        headers.add("x-amz-request-id", generateRequestId());
        headers.add("x-amz-id-2", "fake-id-2");
        headers.add("ETag", "\"" + generateETag(file) + "\"");
        headers.add("Last-Modified", formatS3Date(file.lastModified()));
        headers.add("Accept-Ranges", "bytes");
        headers.add("Content-Type", "application/octet-stream");
        
        return ResponseEntity.ok().headers(headers).build();
    }
    
    /**
     * GET /
     * S3 Proxy health check
     */
    @GetMapping("/")
    public ResponseEntity<String> healthCheck() {
        return ResponseEntity.ok("S3 Proxy Active - Bucket: " + BUCKET_NAME);
    }
    
    /**
     * Generate a fake AWS request ID
     */
    private String generateRequestId() {
        return String.format("FAKE%016X", System.nanoTime());
    }
    
    /**
     * Generate a simple ETag (MD5-like)
     */
    private String generateETag(File file) {
        // Simple ETag based on file size and modification time
        return String.format("%x-%x", file.length(), file.lastModified());
    }
    
    /**
     * Format date in S3 format (RFC 2822)
     */
    private String formatS3Date(long timestamp) {
        return ZonedDateTime.ofInstant(
            java.time.Instant.ofEpochMilli(timestamp),
            java.time.ZoneId.of("GMT")
        ).format(DateTimeFormatter.RFC_1123_DATE_TIME);
    }
}

