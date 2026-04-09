package com.datavault.entity;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;
import java.time.LocalDateTime;
import java.util.*;

// ========== Enums ==========
public enum SensitivityLevel {
    PII, CONFIDENTIAL, INTERNAL, PUBLIC
}
