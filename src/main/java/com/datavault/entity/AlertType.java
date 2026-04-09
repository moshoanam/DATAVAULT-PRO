package com.datavault.entity;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;
import java.time.LocalDateTime;
import java.util.*;

public enum AlertType {
    PII_MISSING_ENCRYPTION,
    PII_DETECTED,
    QUALITY_BELOW_THRESHOLD,
    MISSING_OWNER,
    MISSING_DESCRIPTION,
    DEPRECATED_FIELD_IN_USE,
    BREAKING_CHANGE_DETECTED,
    SYNC_FAILED
}
