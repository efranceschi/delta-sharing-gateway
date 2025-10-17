package com.databricks.deltasharing.dto.protocol;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Descriptor for a deletion vector in Delta Sharing Protocol
 * Based on Delta Sharing Protocol specification
 * https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md
 * 
 * A deletion vector is a data structure that indicates which rows have been deleted from a file.
 * Instead of rewriting the entire file when rows are deleted, Delta Lake uses deletion vectors
 * to track deleted rows, improving performance.
 * 
 * Format:
 * {
 *   "storageType": "u",
 *   "pathOrInlineDv": "vBn[lx{q8@P<9BNH/isA",
 *   "offset": 1,
 *   "sizeInBytes": 36,
 *   "cardinality": 2
 * }
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DeletionVectorDescriptor {
    
    /**
     * Storage type of the deletion vector
     * 
     * - "u": UUID-based deletion vector (stored in a separate file)
     * - "i": Inline deletion vector (embedded in the Delta log)
     * - "p": Path-based deletion vector (stored at a specific path)
     */
    private String storageType;
    
    /**
     * Path or inline deletion vector data
     * 
     * Depending on storageType:
     * - For "u": A UUID string identifying the deletion vector file
     * - For "i": Base64-encoded deletion vector data
     * - For "p": A relative path to the deletion vector file
     */
    private String pathOrInlineDv;
    
    /**
     * Offset in bytes within the deletion vector file (optional)
     * Used when multiple deletion vectors are stored in the same file
     */
    private Integer offset;
    
    /**
     * Size of the deletion vector in bytes
     * This is the serialized size of the deletion vector data
     */
    private Integer sizeInBytes;
    
    /**
     * Cardinality of the deletion vector
     * The number of rows marked as deleted by this deletion vector
     */
    private Long cardinality;
}

