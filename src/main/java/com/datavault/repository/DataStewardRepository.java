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
public interface DataStewardRepository extends JpaRepository<DataSteward, Long> {
    Optional<DataSteward> findByEmail(String email);
    List<DataSteward> findByTeam(String team);
    List<DataSteward> findByIsActive(Boolean isActive);

    @Query("SELECT s FROM DataSteward s JOIN s.databases d WHERE d.id = :databaseId")
    List<DataSteward> findByDatabaseId(@Param("databaseId") Long databaseId);
}
