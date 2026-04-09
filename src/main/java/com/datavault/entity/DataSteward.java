package com.datavault.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "data_stewards")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DataSteward {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String name;

    @Column(nullable = false, unique = true)
    private String email;

    private String team;

    private String department;

    private String role;

    @Column(name = "phone_number")
    private String phoneNumber;

    @Column(name = "is_active")
    private Boolean isActive = true;

    @ElementCollection
    @CollectionTable(
        name = "steward_responsibilities",
        joinColumns = @JoinColumn(name = "steward_id")
    )
    @Column(name = "responsibility")
    @Builder.Default
    private List<String> responsibilities = new ArrayList<>();

    @ManyToMany
    @JoinTable(
        name = "steward_databases",
        joinColumns = @JoinColumn(name = "steward_id"),
        inverseJoinColumns = @JoinColumn(name = "database_id")
    )
    @Builder.Default
    private List<Database> databases = new ArrayList<>();

    @CreationTimestamp
    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at")
    private LocalDateTime updatedAt;
}
