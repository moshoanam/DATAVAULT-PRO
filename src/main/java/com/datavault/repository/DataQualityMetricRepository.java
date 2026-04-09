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
public interface DataQualityMetricRepository extends JpaRepository<DataQualityMetric, Long> {
    List<DataQualityMetric> findByTable(TableMetadata table);
    List<DataQualityMetric> findByField(FieldMetadata field);
    
    @Query("SELECT m FROM DataQualityMetric m WHERE m.table.id = :tableId " +
           "AND m.metricName = :metricName ORDER BY m.measuredAt DESC")
    List<DataQualityMetric> findLatestMetrics(
        @Param("tableId") Long tableId,
        @Param("metricName") String metricName,
        Pageable pageable
    );
}
