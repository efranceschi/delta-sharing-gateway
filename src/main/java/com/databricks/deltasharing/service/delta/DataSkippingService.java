package com.databricks.deltasharing.service.delta;

import com.databricks.deltasharing.dto.delta.AddAction;
import com.databricks.deltasharing.dto.delta.FileStatistics;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Service for data skipping using predicate pushdown
 * Filters files based on partition values and statistics (min/max values)
 */
@Slf4j
@Service
public class DataSkippingService {
    
    /**
     * Apply data skipping to filter files based on predicate hints
     * 
     * @param allFiles All files from Delta log
     * @param predicateHints SQL-like predicates (e.g., "year = 2024", "price > 100")
     * @return Filtered list of files that may contain matching data
     */
    public List<AddAction> applyDataSkipping(List<AddAction> allFiles, List<String> predicateHints) {
        if (predicateHints == null || predicateHints.isEmpty()) {
            log.debug("No predicate hints provided, returning all {} files", allFiles.size());
            return allFiles;
        }
        
        // Parse predicate hints into structured predicates
        List<Predicate> predicates = parsePredicates(predicateHints);
        
        if (predicates.isEmpty()) {
            log.warn("Failed to parse any predicates from hints: {}", predicateHints);
            return allFiles;
        }
        
        log.debug("Applying data skipping with {} predicates to {} files", 
                 predicates.size(), allFiles.size());
        
        // Filter files using predicates
        List<AddAction> filteredFiles = new ArrayList<>();
        int skippedCount = 0;
        
        for (AddAction file : allFiles) {
            if (fileMatchesPredicates(file, predicates)) {
                filteredFiles.add(file);
            } else {
                skippedCount++;
                log.trace("Skipped file: {}", file.getPath());
            }
        }
        
        log.info("Data skipping: {} -> {} files (skipped {} files, {:.1f}% reduction)",
                 allFiles.size(), 
                 filteredFiles.size(), 
                 skippedCount,
                 skippedCount * 100.0 / allFiles.size());
        
        return filteredFiles;
    }
    
    /**
     * Check if a file matches all predicates
     * Returns true if file MAY contain matching data (conservative)
     */
    private boolean fileMatchesPredicates(AddAction file, List<Predicate> predicates) {
        for (Predicate predicate : predicates) {
            if (!evaluatePredicate(file, predicate)) {
                // File definitely doesn't match - SKIP
                return false;
            }
        }
        // File may contain matching data - INCLUDE
        return true;
    }
    
    /**
     * Evaluate a single predicate against a file
     * Returns false only if we're certain the file doesn't match
     */
    private boolean evaluatePredicate(AddAction file, Predicate predicate) {
        String column = predicate.getColumn();
        
        // 1. Try partition pruning first (fastest)
        Map<String, String> partitionValues = file.getPartitionValues();
        if (partitionValues != null && partitionValues.containsKey(column)) {
            return evaluatePartitionPredicate(
                    partitionValues.get(column), 
                    predicate.getOperator(), 
                    predicate.getValue()
            );
        }
        
        // 2. Try min/max filtering using statistics
        FileStatistics stats = file.getParsedStats();
        if (stats != null && stats.getMinValues() != null && stats.getMaxValues() != null) {
            if (stats.getMinValues().containsKey(column) && 
                stats.getMaxValues().containsKey(column)) {
                return evaluateStatsPredicate(stats, column, predicate.getOperator(), predicate.getValue());
            }
        }
        
        // 3. No information available - conservatively include file
        log.trace("No pruning info for column '{}', including file: {}", column, file.getPath());
        return true;
    }
    
    /**
     * Evaluate predicate using partition values
     * Example: year = "2024", month IN ("01", "02")
     */
    private boolean evaluatePartitionPredicate(String partValue, String operator, Object value) {
        switch (operator.toUpperCase()) {
            case "=":
            case "==":
                return partValue.equals(value.toString());
            
            case "!=":
            case "<>":
                return !partValue.equals(value.toString());
            
            case ">":
                return compareStrings(partValue, ">", value.toString());
            
            case ">=":
                return compareStrings(partValue, ">=", value.toString());
            
            case "<":
                return compareStrings(partValue, "<", value.toString());
            
            case "<=":
                return compareStrings(partValue, "<=", value.toString());
            
            case "IN":
                if (value instanceof List) {
                    List<?> values = (List<?>) value;
                    return values.stream().anyMatch(v -> v.toString().equals(partValue));
                }
                return false;
            
            case "NOT IN":
                if (value instanceof List) {
                    List<?> values = (List<?>) value;
                    return values.stream().noneMatch(v -> v.toString().equals(partValue));
                }
                return true;
            
            default:
                log.warn("Unknown operator: {}", operator);
                return true; // Conservative: include file
        }
    }
    
    /**
     * Evaluate predicate using min/max statistics
     * Example: price > 100
     * - If maxValue(price) <= 100: SKIP file (no matching records)
     * - If minValue(price) > 100: INCLUDE file (all records match)
     * - Otherwise: INCLUDE file (may have matching records)
     */
    private boolean evaluateStatsPredicate(FileStatistics stats, String column, 
                                           String operator, Object value) {
        Object minValue = stats.getMinValues().get(column);
        Object maxValue = stats.getMaxValues().get(column);
        
        if (minValue == null || maxValue == null) {
            return true; // No stats, include file
        }
        
        switch (operator.toUpperCase()) {
            case "=":
            case "==":
                // If value < minValue OR value > maxValue: SKIP
                return compareValues(value, ">=", minValue) && compareValues(value, "<=", maxValue);
            
            case "!=":
            case "<>":
                // Hard to optimize, conservatively include
                return true;
            
            case ">":
                // If maxValue <= value: SKIP
                return compareValues(maxValue, ">", value);
            
            case ">=":
                // If maxValue < value: SKIP
                return compareValues(maxValue, ">=", value);
            
            case "<":
                // If minValue >= value: SKIP
                return compareValues(minValue, "<", value);
            
            case "<=":
                // If minValue > value: SKIP
                return compareValues(minValue, "<=", value);
            
            case "IN":
                // Complex, conservatively include
                return true;
            
            case "NOT IN":
                // Complex, conservatively include
                return true;
            
            default:
                return true;
        }
    }
    
    /**
     * Compare two values based on operator
     * Handles numeric, string, and boolean comparisons
     */
    private boolean compareValues(Object left, String operator, Object right) {
        try {
            // Try numeric comparison first
            if (left instanceof Number && right instanceof Number) {
                double l = ((Number) left).doubleValue();
                double r = ((Number) right).doubleValue();
                return compareDoubles(l, operator, r);
            }
            
            // Try parsing as numbers
            try {
                double l = Double.parseDouble(left.toString());
                double r = Double.parseDouble(right.toString());
                return compareDoubles(l, operator, r);
            } catch (NumberFormatException e) {
                // Not numbers, fall through to string comparison
            }
            
            // String comparison
            return compareStrings(left.toString(), operator, right.toString());
            
        } catch (Exception e) {
            log.warn("Failed to compare values: {} {} {}", left, operator, right, e);
            return true; // Conservative
        }
    }
    
    /**
     * Compare doubles
     */
    private boolean compareDoubles(double left, String operator, double right) {
        switch (operator) {
            case "=":
            case "==":
                return Math.abs(left - right) < 0.0001;
            case "!=":
            case "<>":
                return Math.abs(left - right) >= 0.0001;
            case ">":
                return left > right;
            case ">=":
                return left >= right;
            case "<":
                return left < right;
            case "<=":
                return left <= right;
            default:
                return true;
        }
    }
    
    /**
     * Compare strings
     */
    private boolean compareStrings(String left, String operator, String right) {
        int comparison = left.compareTo(right);
        
        switch (operator) {
            case "=":
            case "==":
                return comparison == 0;
            case "!=":
            case "<>":
                return comparison != 0;
            case ">":
                return comparison > 0;
            case ">=":
                return comparison >= 0;
            case "<":
                return comparison < 0;
            case "<=":
                return comparison <= 0;
            default:
                return true;
        }
    }
    
    /**
     * Parse predicate hints into structured Predicate objects
     * 
     * Supported formats:
     * - "column = value"
     * - "column > 100"
     * - "column IN ('A', 'B', 'C')"
     * - "column BETWEEN 10 AND 20" (converted to: column >= 10 AND column <= 20)
     */
    private List<Predicate> parsePredicates(List<String> predicateHints) {
        List<Predicate> predicates = new ArrayList<>();
        
        for (String hint : predicateHints) {
            hint = hint.trim();
            
            try {
                // Pattern for: column operator value
                // Examples: "year = 2024", "price > 100", "status != 'active'"
                Pattern pattern = Pattern.compile(
                        "(\\w+)\\s*(=|==|!=|<>|>|>=|<|<=|IN|NOT\\s+IN)\\s*(.+)",
                        Pattern.CASE_INSENSITIVE
                );
                Matcher matcher = pattern.matcher(hint);
                
                if (matcher.matches()) {
                    String column = matcher.group(1).trim();
                    String operator = matcher.group(2).trim();
                    String valueStr = matcher.group(3).trim();
                    
                    Object value = parseValue(valueStr, operator);
                    
                    predicates.add(new Predicate(column, operator, value));
                    log.debug("Parsed predicate: {} {} {}", column, operator, value);
                } else {
                    log.warn("Failed to parse predicate hint: {}", hint);
                }
                
            } catch (Exception e) {
                log.error("Error parsing predicate hint: {}", hint, e);
            }
        }
        
        return predicates;
    }
    
    /**
     * Parse value from string
     * Handles: numbers, strings (quoted), lists (for IN operator)
     */
    private Object parseValue(String valueStr, String operator) {
        valueStr = valueStr.trim();
        
        // Handle IN operator: parse list of values
        if (operator.toUpperCase().contains("IN")) {
            // Remove parentheses and split by comma
            valueStr = valueStr.replaceAll("[()]", "").trim();
            List<String> values = Arrays.stream(valueStr.split(","))
                    .map(String::trim)
                    .map(v -> v.replaceAll("^['\"]|['\"]$", "")) // Remove quotes
                    .collect(Collectors.toList());
            return values;
        }
        
        // Remove quotes for string values
        if ((valueStr.startsWith("'") && valueStr.endsWith("'")) ||
            (valueStr.startsWith("\"") && valueStr.endsWith("\""))) {
            return valueStr.substring(1, valueStr.length() - 1);
        }
        
        // Try parsing as number
        try {
            if (valueStr.contains(".")) {
                return Double.parseDouble(valueStr);
            } else {
                return Long.parseLong(valueStr);
            }
        } catch (NumberFormatException e) {
            // Not a number, return as string
            return valueStr;
        }
    }
    
    /**
     * Internal class to represent a parsed predicate
     */
    @Data
    @AllArgsConstructor
    private static class Predicate {
        private String column;
        private String operator;
        private Object value;
    }
}

