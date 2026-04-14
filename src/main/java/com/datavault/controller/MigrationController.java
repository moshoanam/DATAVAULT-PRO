package com.datavault.controller;

import com.datavault.service.MigrationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api/v1/migration")
@RequiredArgsConstructor
@Slf4j
public class MigrationController {

    private final MigrationService migrationService;

    /**
     * POST /api/v1/migration/analyze
     * Body: { connStr, username, password, schema, tables[] }
     * Returns type-mapping preview for each table.
     */
    @PostMapping("/analyze")
    public ResponseEntity<Map<String, Object>> analyze(@RequestBody Map<String, Object> req) {
        try {
            String connStr    = str(req, "connectionString");
            String username   = str(req, "username");
            String password   = str(req, "password");
            String schema     = str(req, "schema", "dbo");
            @SuppressWarnings("unchecked")
            List<String> tables = (List<String>) req.getOrDefault("tables", Collections.emptyList());

            Map<String, Object> plan = migrationService.analyzeMigration(connStr, username, password, schema, tables);
            plan.put("success", true);
            return ResponseEntity.ok(plan);
        } catch (Exception e) {
            log.error("Migration analyze error: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(Map.of("success", false, "error", e.getMessage()));
        }
    }

    /**
     * POST /api/v1/migration/generate-ddl
     * Returns the PostgreSQL DDL as plain text for download / preview.
     */
    @PostMapping("/generate-ddl")
    public ResponseEntity<String> generateDdl(@RequestBody Map<String, Object> req) {
        try {
            String ddl = migrationService.generateDdl(
                str(req, "connectionString"),
                str(req, "username"),
                str(req, "password"),
                str(req, "sourceSchema", "dbo"),
                str(req, "targetSchema", "public"),
                tables(req),
                bool(req, "includeIndexes", true),
                bool(req, "includeComments", true),
                bool(req, "dropIfExists", false)
            );
            return ResponseEntity.ok()
                    .contentType(MediaType.TEXT_PLAIN)
                    .body(ddl);
        } catch (Exception e) {
            log.error("Migration DDL error: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("-- Error: " + e.getMessage());
        }
    }

    /**
     * POST /api/v1/migration/download-ddl
     * Same as generate-ddl but triggers a browser file download.
     */
    @PostMapping("/download-ddl")
    public ResponseEntity<byte[]> downloadDdl(@RequestBody Map<String, Object> req) {
        try {
            String ddl = migrationService.generateDdl(
                str(req, "connectionString"),
                str(req, "username"),
                str(req, "password"),
                str(req, "sourceSchema", "dbo"),
                str(req, "targetSchema", "public"),
                tables(req),
                bool(req, "includeIndexes", true),
                bool(req, "includeComments", true),
                bool(req, "dropIfExists", false)
            );
            String filename = "migration_" + str(req, "sourceSchema", "dbo") + "_" +
                    java.time.LocalDate.now() + ".sql";
            return ResponseEntity.ok()
                    .header("Content-Disposition", "attachment; filename=\"" + filename + "\"")
                    .contentType(MediaType.APPLICATION_OCTET_STREAM)
                    .body(ddl.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        } catch (Exception e) {
            log.error("Migration download error: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(("-- Error: " + e.getMessage()).getBytes());
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────
    private String str(Map<String, Object> m, String key) { return str(m, key, ""); }
    private String str(Map<String, Object> m, String key, String def) {
        Object v = m.get(key); return v != null ? v.toString() : def;
    }
    private boolean bool(Map<String, Object> m, String key, boolean def) {
        Object v = m.get(key);
        if (v instanceof Boolean b) return b;
        if (v != null) return Boolean.parseBoolean(v.toString());
        return def;
    }
    @SuppressWarnings("unchecked")
    private List<String> tables(Map<String, Object> m) {
        Object v = m.get("tables");
        return (v instanceof List<?> l) ? (List<String>) l : Collections.emptyList();
    }
}
