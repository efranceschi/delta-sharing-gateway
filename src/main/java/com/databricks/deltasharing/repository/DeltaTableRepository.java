package com.databricks.deltasharing.repository;

import com.databricks.deltasharing.model.DeltaTable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface DeltaTableRepository extends JpaRepository<DeltaTable, Long> {
    
    @Query("SELECT t FROM DeltaTable t " +
           "JOIN FETCH t.schema s " +
           "JOIN FETCH s.share " +
           "WHERE t.id = :id")
    Optional<DeltaTable> findByIdWithSchemaAndShare(@Param("id") Long id);
    
    @Query("SELECT t FROM DeltaTable t WHERE t.name = :tableName " +
           "AND t.schema.name = :schemaName AND t.schema.share.name = :shareName")
    Optional<DeltaTable> findByNameAndSchemaNameAndShareName(
            @Param("tableName") String tableName,
            @Param("schemaName") String schemaName,
            @Param("shareName") String shareName);
    
    @Query("SELECT t FROM DeltaTable t " +
           "JOIN FETCH t.schema s " +
           "JOIN FETCH s.share " +
           "WHERE s.name = :schemaName AND s.share.name = :shareName")
    List<DeltaTable> findBySchemaNameAndShareName(
            @Param("schemaName") String schemaName,
            @Param("shareName") String shareName);
    
    @Query("SELECT t FROM DeltaTable t " +
           "JOIN FETCH t.schema s " +
           "JOIN FETCH s.share sh " +
           "WHERE sh.name = :shareName")
    List<DeltaTable> findByShareName(@Param("shareName") String shareName);
    
    List<DeltaTable> findBySchemaId(Long schemaId);
}
