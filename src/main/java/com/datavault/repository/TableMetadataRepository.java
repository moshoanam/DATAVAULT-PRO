package com.datavault.repository;

import com.datavault.entity.*;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface TableMetadataRepository extends JpaRepository<TableMetadata, Long> {
    List<TableMetadata> findByDatabase(Database database);
    Page<TableMetadata> findByDatabaseId(Long databaseId, Pageable pageable);
    Optional<TableMetadata> findByDatabaseAndTableName(Database database, String tableName);

    @Query("SELECT t FROM TableMetadata t WHERE t.database.id = :databaseId " +
           "AND t.schema = :schema AND t.tableName = :tableName")
    Optional<TableMetadata> findByDatabaseAndSchemaAndTable(
        @Param("databaseId") Long databaseId,
        @Param("schema") String schema,
        @Param("tableName") String tableName
    );

    @Query("SELECT t FROM TableMetadata t WHERE t.dataQualityScore < :threshold")
    List<TableMetadata> findLowQualityTables(@Param("threshold") Double threshold);

    @Query("SELECT AVG(t.dataQualityScore) FROM TableMetadata t WHERE t.dataQualityScore IS NOT NULL")
    Double getAverageQualityScore();
}
