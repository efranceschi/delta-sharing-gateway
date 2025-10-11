package com.databricks.deltasharing.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

/**
 * Service for handling pagination in Delta Sharing API responses
 * Generates and validates page tokens for result pagination
 */
@Service
@Slf4j
public class PaginationService {
    
    private static final int DEFAULT_MAX_RESULTS = 500;
    private static final int MAX_ALLOWED_RESULTS = 1000;
    
    /**
     * Paginate a list of items
     * 
     * @param items Full list of items
     * @param maxResults Maximum number of results to return
     * @param pageToken Token indicating the starting point
     * @return Paginated result with items and next page token
     */
    public <T> PaginatedResult<T> paginate(List<T> items, Integer maxResults, String pageToken) {
        if (items == null || items.isEmpty()) {
            return new PaginatedResult<>(items, null);
        }
        
        int limit = getEffectiveLimit(maxResults);
        int offset = decodePageToken(pageToken);
        
        // Validate offset
        if (offset < 0 || offset >= items.size()) {
            offset = 0;
        }
        
        // Calculate end index
        int endIndex = Math.min(offset + limit, items.size());
        
        // Get paginated items
        List<T> paginatedItems = items.subList(offset, endIndex);
        
        // Generate next page token if there are more items
        String nextPageToken = null;
        if (endIndex < items.size()) {
            nextPageToken = encodePageToken(endIndex);
        }
        
        log.debug("Paginated {} items: offset={}, limit={}, total={}, hasMore={}", 
                  items.size(), offset, limit, paginatedItems.size(), nextPageToken != null);
        
        return new PaginatedResult<>(paginatedItems, nextPageToken);
    }
    
    /**
     * Get effective limit considering default and maximum values
     */
    private int getEffectiveLimit(Integer maxResults) {
        if (maxResults == null || maxResults <= 0) {
            return DEFAULT_MAX_RESULTS;
        }
        return Math.min(maxResults, MAX_ALLOWED_RESULTS);
    }
    
    /**
     * Encode offset as page token (Base64)
     */
    private String encodePageToken(int offset) {
        String token = String.valueOf(offset);
        return Base64.getEncoder().encodeToString(token.getBytes(StandardCharsets.UTF_8));
    }
    
    /**
     * Decode page token to offset
     * Returns 0 if token is invalid
     */
    private int decodePageToken(String pageToken) {
        if (pageToken == null || pageToken.isEmpty()) {
            return 0;
        }
        
        try {
            byte[] decoded = Base64.getDecoder().decode(pageToken);
            String offsetStr = new String(decoded, StandardCharsets.UTF_8);
            return Integer.parseInt(offsetStr);
        } catch (Exception e) {
            log.warn("Invalid page token: {}", pageToken, e);
            return 0;
        }
    }
    
    /**
     * Result class containing paginated items and next page token
     */
    public static class PaginatedResult<T> {
        private final List<T> items;
        private final String nextPageToken;
        
        public PaginatedResult(List<T> items, String nextPageToken) {
            this.items = items;
            this.nextPageToken = nextPageToken;
        }
        
        public List<T> getItems() {
            return items;
        }
        
        public String getNextPageToken() {
            return nextPageToken;
        }
    }
}

