package com.datavault.adapter;

import com.datavault.dto.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.*;

import java.sql.Connection;
import java.util.*;

/**
 * AWS Glue Data Catalog adapter.
 *
 * Connection string format : glue://&lt;aws-region&gt;   e.g. glue://us-east-1
 * Username                 : AWS Access Key ID     (leave blank for default credentials chain)
 * Password                 : AWS Secret Access Key (leave blank for default credentials chain)
 *
 * When username/password are blank the SDK falls back to the standard
 * DefaultCredentialsProvider chain: env vars → system props → ~/.aws/credentials → instance profile.
 */
@Component
@Slf4j
public class GlueAdapter implements DatabaseAdapter {

    @Override
    public String getAdapterType() { return "AWSGlue"; }

    // ── Connection test ────────────────────────────────────────────────────────

    @Override
    public boolean testConnection(String connectionString, String username, String password) {
        try (GlueClient client = buildClient(connectionString, username, password)) {
            client.getDatabases(GetDatabasesRequest.builder().maxResults(1).build());
            log.info("Glue connection test succeeded for {}", connectionString);
            return true;
        } catch (Exception e) {
            log.warn("Glue connection test failed: {}", e.getMessage());
            return false;
        }
    }

    // ── Glue-specific extraction methods ──────────────────────────────────────

    /** List all Glue database names in the catalog. */
    public List<String> extractGlueDatabases(String connectionString, String username, String password) {
        List<String> names = new ArrayList<>();
        try (GlueClient client = buildClient(connectionString, username, password)) {
            String nextToken = null;
            do {
                GetDatabasesRequest.Builder req = GetDatabasesRequest.builder().maxResults(100);
                if (nextToken != null) req.nextToken(nextToken);
                GetDatabasesResponse resp = client.getDatabases(req.build());
                resp.databaseList().forEach(db -> names.add(db.name()));
                nextToken = resp.nextToken();
            } while (nextToken != null);
        } catch (Exception e) {
            log.error("Error listing Glue databases: {}", e.getMessage(), e);
        }
        log.info("Found {} Glue databases", names.size());
        return names;
    }

    /** List all tables in a Glue database, including row-count / size hints. */
    public List<TableMetadataDTO> extractGlueTables(String connectionString, String username,
                                                     String password, String glueDatabaseName) {
        List<TableMetadataDTO> tables = new ArrayList<>();
        try (GlueClient client = buildClient(connectionString, username, password)) {
            String nextToken = null;
            do {
                GetTablesRequest.Builder req = GetTablesRequest.builder()
                        .databaseName(glueDatabaseName).maxResults(100);
                if (nextToken != null) req.nextToken(nextToken);
                GetTablesResponse resp = client.getTables(req.build());
                for (Table t : resp.tableList()) {
                    Map<String, String> params = t.parameters() != null ? t.parameters() : Map.of();
                    tables.add(TableMetadataDTO.builder()
                            .tableName(t.name())
                            .schema(glueDatabaseName)
                            .description(t.description())
                            .rowCount(parseLong(params.get("numRows")))
                            .sizeBytes(parseLong(params.get("totalSize")))
                            .build());
                }
                nextToken = resp.nextToken();
            } while (nextToken != null);
        } catch (Exception e) {
            log.error("Error listing Glue tables in database '{}': {}", glueDatabaseName, e.getMessage(), e);
        }
        return tables;
    }

    /** Extract columns (storage descriptor + partition keys) for a single Glue table. */
    public List<FieldMetadataDTO> extractGlueFields(String connectionString, String username,
                                                      String password,
                                                      String glueDatabaseName, String tableName) {
        List<FieldMetadataDTO> fields = new ArrayList<>();
        try (GlueClient client = buildClient(connectionString, username, password)) {
            GetTableResponse resp = client.getTable(GetTableRequest.builder()
                    .databaseName(glueDatabaseName).name(tableName).build());
            Table t = resp.table();

            List<Column> columns = new ArrayList<>();
            if (t.storageDescriptor() != null && t.storageDescriptor().columns() != null) {
                columns.addAll(t.storageDescriptor().columns());
            }
            // Partition keys are also queryable columns
            if (t.partitionKeys() != null) {
                columns.addAll(t.partitionKeys());
            }

            for (int i = 0; i < columns.size(); i++) {
                Column col = columns.get(i);
                boolean pii = isPII(col.name(), col.type());
                fields.add(FieldMetadataDTO.builder()
                        .fieldName(col.name())
                        .businessName(col.name())          // default to physical name
                        .dataType(normaliseType(col.type()))
                        .description(col.comment())
                        .isNullable(true)
                        .isPrimaryKey(false)
                        .isForeignKey(false)
                        .sensitivityLevel(pii ? "PII" : "INTERNAL")
                        .build());
            }
        } catch (Exception e) {
            log.error("Error extracting fields for {}.{}: {}", glueDatabaseName, tableName, e.getMessage(), e);
        }
        return fields;
    }

    // ── PII detection ─────────────────────────────────────────────────────────

    @Override
    public boolean isPII(String fieldName, String dataType) {
        if (fieldName == null) return false;
        String lower = fieldName.toLowerCase();
        return lower.contains("email")       || lower.contains("phone")      ||
               lower.contains("ssn")         || lower.contains("passport")   ||
               lower.contains("credit_card") || lower.contains("dob")        ||
               lower.contains("date_of_birth") || lower.contains("national_id") ||
               lower.contains("first_name")  || lower.contains("last_name")  ||
               lower.contains("full_name")   || lower.contains("address")    ||
               lower.contains("ip_address")  || lower.contains("user_id");
    }

    // ── Client factory ────────────────────────────────────────────────────────

    /**
     * Build a {@link GlueClient} from the connection string and optional static credentials.
     * connectionString must be in the form {@code glue://&lt;region&gt;} or just {@code &lt;region&gt;}.
     */
    public GlueClient buildClient(String connectionString, String accessKeyId, String secretKey) {
        String region = connectionString.replaceFirst("(?i)^glue://", "").trim();

        AwsCredentialsProvider creds;
        if (accessKeyId != null && !accessKeyId.isBlank()
                && secretKey != null && !secretKey.isBlank()) {
            creds = StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKeyId.trim(), secretKey.trim()));
        } else {
            // Env vars / system props / ~/.aws/credentials / EC2 instance profile
            creds = DefaultCredentialsProvider.create();
        }

        return GlueClient.builder()
                .region(Region.of(region))
                .credentialsProvider(creds)
                .httpClient(UrlConnectionHttpClient.builder().build())
                .build();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /** Shorten verbose Hive composite types for display (e.g. array<struct<...>> → array<struct>). */
    private String normaliseType(String glueType) {
        if (glueType == null) return "unknown";
        if (glueType.length() <= 64) return glueType;
        // Truncate complex nested types at the outer bracket
        int idx = glueType.indexOf('<');
        return idx > 0 ? glueType.substring(0, idx) + "<...>" : glueType.substring(0, 64) + "…";
    }

    private Long parseLong(String s) {
        try { return s != null ? Long.parseLong(s.trim()) : null; }
        catch (NumberFormatException e) { return null; }
    }

    // ── Unsupported SQL-based interface methods ───────────────────────────────

    @Override
    public Connection getConnection(String cs, String u, String p) {
        throw new UnsupportedOperationException("AWS Glue does not use JDBC connections");
    }

    @Override public List<String>               extractSchemas(Connection c)                           { return List.of(); }
    @Override public List<TableMetadataDTO>     extractTables(Connection c, String schema)             { return List.of(); }
    @Override public List<FieldMetadataDTO>     extractFields(Connection c, String schema, String t)   { return List.of(); }
    @Override public List<RelationshipDTO>      extractRelationships(Connection c, String schema)      { return List.of(); }
    @Override public Map<String, Object>        extractTableStatistics(Connection c, String s, String t){ return Map.of(); }
    @Override public List<LineageRelationshipDTO> buildLineage(Connection c, String schema)            { return List.of(); }

    @Override
    public DataQualityDTO calculateQuality(Connection c, String schema, String table) {
        // Quality cannot be calculated without direct data access; return neutral score
        return DataQualityDTO.builder().overallScore(null).build();
    }
}
