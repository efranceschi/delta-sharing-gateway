package com.databricks.deltasharing.repository;

import com.databricks.deltasharing.model.DeltaSchema;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface DeltaSchemaRepository extends JpaRepository<DeltaSchema, Long> {
    
    @Query("SELECT s FROM DeltaSchema s WHERE s.name = :schemaName AND s.share.name = :shareName")
    Optional<DeltaSchema> findByNameAndShareName(@Param("schemaName") String schemaName, 
                                                   @Param("shareName") String shareName);
    
    @Query("SELECT s FROM DeltaSchema s WHERE s.share.name = :shareName")
    List<DeltaSchema> findByShareName(@Param("shareName") String shareName);
    
    List<DeltaSchema> findByShareId(Long shareId);
}
