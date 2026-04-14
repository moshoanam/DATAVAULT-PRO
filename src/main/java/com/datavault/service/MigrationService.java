package com.datavault.service;

import com.datavault.adapter.MSSQLAdapter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.*;

@Service
@RequiredArgsConstructor
@Slf4j
public class MigrationService {

    private final MSSQLAdapter mssqlAdapter;

    // ── Type mapping: SQL Server → PostgreSQL ────────────────────────────────
    private static final Map<String, String> TYPE_MAP = new LinkedHashMap<>();
    static {
        // Exact matches first
        TYPE_MAP.put("bigint",            "bigint");
        TYPE_MAP.put("int",               "integer");
        TYPE_MAP.put("integer",           "integer");
        TYPE_MAP.put("smallint",          "smallint");
        TYPE_MAP.put("tinyint",           "smallint");
        TYPE_MAP.put("bit",               "boolean");
        TYPE_MAP.put("decimal",           "numeric");
        TYPE_MAP.put("numeric",           "numeric");
        TYPE_MAP.put("money",             "decimal(19,4)");
        TYPE_MAP.put("smallmoney",        "decimal(10,4)");
        TYPE_MAP.put("float",             "double precision");
        TYPE_MAP.put("real",              "real");
        TYPE_MAP.put("date",              "date");
        TYPE_MAP.put("time",              "time");
        TYPE_MAP.put("datetime",          "timestamp");
        TYPE_MAP.put("datetime2",         "timestamp");
        TYPE_MAP.put("smalldatetime",     "timestamp");
        TYPE_MAP.put("datetimeoffset",    "timestamptz");
        TYPE_MAP.put("char",              "char");
        TYPE_MAP.put("nchar",             "char");
        TYPE_MAP.put("varchar",           "varchar");
        TYPE_MAP.put("nvarchar",          "varchar");
        TYPE_MAP.put("text",              "text");
        TYPE_MAP.put("ntext",             "text");
        TYPE_MAP.put("binary",            "bytea");
        TYPE_MAP.put("varbinary",         "bytea");
        TYPE_MAP.put("image",             "bytea");
        TYPE_MAP.put("uniqueidentifier",  "uuid");
        TYPE_MAP.put("xml",               "xml");
        TYPE_MAP.put("rowversion",        "bytea");
        TYPE_MAP.put("timestamp",         "bytea");   // SQL Server timestamp ≠ SQL timestamp
        TYPE_MAP.put("sql_variant",       "text");
        TYPE_MAP.put("hierarchyid",       "text");
        TYPE_MAP.put("geography",         "text");
        TYPE_MAP.put("geometry",          "text");
    }

    // ────────────────────────────────────────────────────────────────────────
    // Public API
    // ────────────────────────────────────────────────────────────────────────

    /**
     * Analyse the MSSQL source and return a migration plan (type-mapped DDL preview).
     */
    public Map<String, Object> analyzeMigration(String connStr, String username, String password,
                                                 String schemaName, List<String> selectedTables) throws Exception {
        Map<String, Object> plan = new LinkedHashMap<>();
        List<Map<String, Object>> tableReports = new ArrayList<>();
        List<String> warnings = new ArrayList<>();
        int totalColumns = 0;

        try (Connection conn = mssqlAdapter.getConnection(connStr, username, password)) {
            DatabaseMetaData meta = conn.getMetaData();

            // Resolve tables
            List<String> tables = selectedTables != null && !selectedTables.isEmpty()
                ? selectedTables
                : listTables(conn, schemaName);

            for (String tableName : tables) {
                Map<String, Object> report = analyzeTable(conn, meta, schemaName, tableName, warnings);
                tableReports.add(report);
                totalColumns += (int) report.getOrDefault("columnCount", 0);
            }
        }

        plan.put("tableCount",   tableReports.size());
        plan.put("totalColumns", totalColumns);
        plan.put("tables",       tableReports);
        plan.put("warnings",     warnings);
        return plan;
    }

    /**
     * Generate a complete PostgreSQL DDL migration script.
     */
    public String generateDdl(String connStr, String username, String password,
                               String sourceSchema, String targetSchema,
                               List<String> selectedTables,
                               boolean includeIndexes, boolean includeComments,
                               boolean dropIfExists) throws Exception {

        StringBuilder sb = new StringBuilder();
        sb.append("-- ============================================================\n");
        sb.append("-- DataVault Pro — MS SQL Server → PostgreSQL Migration Script\n");
        sb.append("-- Generated: ").append(java.time.LocalDateTime.now()).append("\n");
        sb.append("-- Source schema: ").append(sourceSchema).append("\n");
        sb.append("-- Target schema: ").append(targetSchema).append("\n");
        sb.append("-- ============================================================\n\n");

        sb.append("SET client_encoding = 'UTF8';\n");
        sb.append("SET standard_conforming_strings = on;\n\n");

        if (targetSchema != null && !targetSchema.equalsIgnoreCase("public")) {
            sb.append("CREATE SCHEMA IF NOT EXISTS ").append(quoteIdent(targetSchema)).append(";\n\n");
        }

        try (Connection conn = mssqlAdapter.getConnection(connStr, username, password)) {
            DatabaseMetaData meta = conn.getMetaData();
            List<String> tables = selectedTables != null && !selectedTables.isEmpty()
                ? selectedTables
                : listTables(conn, sourceSchema);

            // Pass 1: CREATE TABLE statements
            for (String tableName : tables) {
                sb.append(generateCreateTable(conn, meta, sourceSchema, targetSchema,
                                              tableName, dropIfExists, includeComments));
                sb.append("\n");
            }

            // Pass 2: Foreign keys (after all tables exist)
            for (String tableName : tables) {
                String fkDdl = generateForeignKeys(meta, sourceSchema, targetSchema, tableName);
                if (!fkDdl.isEmpty()) sb.append(fkDdl).append("\n");
            }

            // Pass 3: Indexes
            if (includeIndexes) {
                for (String tableName : tables) {
                    sb.append(generateIndexes(meta, sourceSchema, targetSchema, tableName));
                }
            }
        }

        sb.append("\n-- Migration script complete.\n");
        return sb.toString();
    }

    // ────────────────────────────────────────────────────────────────────────
    // Internal helpers
    // ────────────────────────────────────────────────────────────────────────

    private List<String> listTables(Connection conn, String schema) throws SQLException {
        List<String> tables = new ArrayList<>();
        try (ResultSet rs = conn.getMetaData().getTables(null, schema, "%", new String[]{"TABLE"})) {
            while (rs.next()) tables.add(rs.getString("TABLE_NAME"));
        }
        return tables;
    }

    private Map<String, Object> analyzeTable(Connection conn, DatabaseMetaData meta,
                                              String schema, String tableName,
                                              List<String> warnings) throws SQLException {
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("tableName", tableName);

        List<Map<String, Object>> columns = new ArrayList<>();
        try (ResultSet rs = meta.getColumns(null, schema, tableName, "%")) {
            while (rs.next()) {
                String srcType = rs.getString("TYPE_NAME").toLowerCase();
                int    size    = rs.getInt("COLUMN_SIZE");
                int    scale   = rs.getInt("DECIMAL_DIGITS");

                String pgType  = mapType(srcType, size, scale);
                boolean needsReview = pgType.startsWith("text") && !srcType.contains("text")
                                   && !srcType.contains("varchar");
                if (needsReview) warnings.add("Column " + tableName + "." + rs.getString("COLUMN_NAME") + " mapped from [" + srcType + "] to [" + pgType + "] — review recommended");

                Map<String, Object> col = new LinkedHashMap<>();
                col.put("name",      rs.getString("COLUMN_NAME"));
                col.put("mssqlType", srcType + (size > 0 && srcType.contains("var") ? "(" + (size == Integer.MAX_VALUE || size > 8000 ? "MAX" : size) + ")" : ""));
                col.put("pgType",    pgType);
                col.put("nullable",  "YES".equals(rs.getString("IS_NULLABLE")));
                col.put("hasDefault", rs.getString("COLUMN_DEF") != null);
                col.put("needsReview", needsReview);
                columns.add(col);
            }
        }
        report.put("columnCount", columns.size());
        report.put("columns", columns);
        return report;
    }

    private String generateCreateTable(Connection conn, DatabaseMetaData meta,
                                        String srcSchema, String tgtSchema,
                                        String tableName, boolean drop, boolean comments) throws SQLException {
        StringBuilder sb = new StringBuilder();
        String fqn = quoteIdent(tgtSchema) + "." + quoteIdent(tableName);

        if (drop) sb.append("DROP TABLE IF EXISTS ").append(fqn).append(" CASCADE;\n");
        sb.append("CREATE TABLE ").append(fqn).append(" (\n");

        List<String> pks = new ArrayList<>();
        try (ResultSet rs = meta.getPrimaryKeys(null, srcSchema, tableName)) {
            while (rs.next()) pks.add(rs.getString("COLUMN_NAME"));
        }

        List<String> colDefs = new ArrayList<>();
        try (ResultSet rs = meta.getColumns(null, srcSchema, tableName, "%")) {
            while (rs.next()) {
                String col     = rs.getString("COLUMN_NAME");
                String srcType = rs.getString("TYPE_NAME").toLowerCase();
                int    size    = rs.getInt("COLUMN_SIZE");
                int    scale   = rs.getInt("DECIMAL_DIGITS");
                String defVal  = rs.getString("COLUMN_DEF");
                boolean nullable = "YES".equals(rs.getString("IS_NULLABLE"));
                boolean isPk   = pks.contains(col);

                // Use SERIAL/BIGSERIAL for identity PK columns
                boolean isIdentity = isIdentityColumn(conn, srcSchema, tableName, col);
                String pgType;
                if (isPk && isIdentity) {
                    pgType = srcType.equals("bigint") ? "bigserial" : "serial";
                } else {
                    pgType = mapType(srcType, size, scale);
                }

                StringBuilder def = new StringBuilder("    ")
                    .append(quoteIdent(col)).append(" ").append(pgType);
                if (!nullable || isPk) def.append(" NOT NULL");
                if (defVal != null && !isIdentity) {
                    String pgDefault = convertDefault(defVal, srcType);
                    if (pgDefault != null) def.append(" DEFAULT ").append(pgDefault);
                }
                colDefs.add(def.toString());
            }
        }
        // Primary key constraint
        if (!pks.isEmpty()) {
            String pkCols = String.join(", ", pks.stream().map(this::quoteIdent).toList());
            colDefs.add("    CONSTRAINT pk_" + tableName.toLowerCase() + " PRIMARY KEY (" + pkCols + ")");
        }

        sb.append(String.join(",\n", colDefs)).append("\n);\n");

        if (comments) {
            sb.append("COMMENT ON TABLE ").append(fqn)
              .append(" IS 'Migrated from MS SQL Server schema [").append(srcSchema).append("]';\n");
        }
        return sb.toString();
    }

    private String generateForeignKeys(DatabaseMetaData meta, String srcSchema, String tgtSchema,
                                        String tableName) throws SQLException {
        StringBuilder sb = new StringBuilder();
        String fqn = quoteIdent(tgtSchema) + "." + quoteIdent(tableName);
        try (ResultSet rs = meta.getImportedKeys(null, srcSchema, tableName)) {
            Map<String, List<String[]>> fkMap = new LinkedHashMap<>();
            while (rs.next()) {
                String fkName = rs.getString("FK_NAME");
                fkMap.computeIfAbsent(fkName, k -> new ArrayList<>())
                     .add(new String[]{rs.getString("FKCOLUMN_NAME"),
                                       rs.getString("PKTABLE_NAME"),
                                       rs.getString("PKCOLUMN_NAME")});
            }
            for (Map.Entry<String, List<String[]>> e : fkMap.entrySet()) {
                String fkCols = e.getValue().stream().map(r -> quoteIdent(r[0])).reduce((a,b)->a+","+b).orElse("");
                String pkTable = e.getValue().get(0)[1];
                String pkCols  = e.getValue().stream().map(r -> quoteIdent(r[2])).reduce((a,b)->a+","+b).orElse("");
                sb.append("ALTER TABLE ").append(fqn)
                  .append(" ADD CONSTRAINT ").append(quoteIdent(e.getKey()))
                  .append(" FOREIGN KEY (").append(fkCols).append(")")
                  .append(" REFERENCES ").append(quoteIdent(tgtSchema)).append(".").append(quoteIdent(pkTable))
                  .append(" (").append(pkCols).append(");\n");
            }
        }
        return sb.toString();
    }

    private String generateIndexes(DatabaseMetaData meta, String srcSchema, String tgtSchema,
                                    String tableName) throws SQLException {
        StringBuilder sb = new StringBuilder();
        String fqn = quoteIdent(tgtSchema) + "." + quoteIdent(tableName);
        try (ResultSet rs = meta.getIndexInfo(null, srcSchema, tableName, false, false)) {
            Map<String, List<String[]>> idxMap = new LinkedHashMap<>();
            while (rs.next()) {
                String idxName = rs.getString("INDEX_NAME");
                if (idxName == null) continue;
                boolean unique = !rs.getBoolean("NON_UNIQUE");
                String col = rs.getString("COLUMN_NAME");
                idxMap.computeIfAbsent(idxName, k -> new ArrayList<>())
                      .add(new String[]{col, unique ? "UNIQUE" : ""});
            }
            for (Map.Entry<String, List<String[]>> e : idxMap.entrySet()) {
                boolean unique = "UNIQUE".equals(e.getValue().get(0)[1]);
                String cols = e.getValue().stream().map(r -> quoteIdent(r[0])).reduce((a,b)->a+","+b).orElse("");
                String name = "idx_" + tableName.toLowerCase() + "_" + e.getKey().toLowerCase().replaceAll("[^a-z0-9]","_");
                sb.append("CREATE ").append(unique ? "UNIQUE " : "").append("INDEX IF NOT EXISTS ")
                  .append(name).append(" ON ").append(fqn).append(" (").append(cols).append(");\n");
            }
        }
        return sb.toString();
    }

    private boolean isIdentityColumn(Connection conn, String schema, String table, String column) {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT is_identity FROM sys.columns c " +
                "JOIN sys.tables t ON c.object_id = t.object_id " +
                "JOIN sys.schemas s ON t.schema_id = s.schema_id " +
                "WHERE s.name = ? AND t.name = ? AND c.name = ?")) {
            ps.setString(1, schema); ps.setString(2, table); ps.setString(3, column);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getBoolean(1);
            }
        } catch (Exception e) { return false; }
    }

    private String mapType(String mssql, int size, int scale) {
        String base = mssql.toLowerCase().replaceAll("\\(.*\\)", "").trim();
        // Handle sized types
        if (base.equals("varchar") || base.equals("nvarchar") || base.equals("char") || base.equals("nchar")) {
            if (size <= 0 || size >= 8000) return "text";
            return (base.startsWith("n") ? "varchar" : base) + "(" + size + ")";
        }
        if ((base.equals("decimal") || base.equals("numeric")) && size > 0) {
            return "numeric(" + size + (scale > 0 ? "," + scale : "") + ")";
        }
        return TYPE_MAP.getOrDefault(base, "text");
    }

    private String convertDefault(String def, String srcType) {
        if (def == null) return null;
        String d = def.trim().replaceAll("^\\(+|\\)+$", "").trim();
        if (d.equalsIgnoreCase("getdate()") || d.equalsIgnoreCase("getutcdate()")) return "CURRENT_TIMESTAMP";
        if (d.equalsIgnoreCase("newid()")) return "gen_random_uuid()";
        if (d.equals("1") && srcType.equals("bit")) return "true";
        if (d.equals("0") && srcType.equals("bit")) return "false";
        if (d.startsWith("N'") && d.endsWith("'")) return "'" + d.substring(2, d.length() - 1) + "'";
        if (d.startsWith("'") && d.endsWith("'")) return d;
        try { Double.parseDouble(d); return d; } catch (NumberFormatException ignore) {}
        return null; // skip unrecognised defaults
    }

    private String quoteIdent(String s) {
        if (s == null) return "\"\"";
        return "\"" + s.replace("\"", "\"\"") + "\"";
    }
}
