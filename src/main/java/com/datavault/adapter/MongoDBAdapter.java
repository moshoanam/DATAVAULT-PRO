package com.datavault.adapter;

import com.datavault.dto.*;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.*;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.util.*;

@Slf4j
@Component("mongodbAdapter")
public class MongoDBAdapter implements DatabaseAdapter {
    
    private static final Set<String> PII_KEYWORDS = Set.of(
        "email", "phone", "ssn", "social", "passport", "license",
        "credit", "card", "account", "password", "birth", "dob",
        "name", "address", "zip", "postal"
    );

    @Override
    public String getAdapterType() {
        return "MongoDB";
    }

    @Override
    public boolean testConnection(String connectionString, String username, String password) {
        try {
            MongoClient mongoClient = createMongoClient(connectionString);
            mongoClient.listDatabaseNames().first();
            mongoClient.close();
            return true;
        } catch (Exception e) {
            log.error("MongoDB connection test failed: {}", e.getMessage());
            return false;
        }
    }

    @Override
    public Connection getConnection(String connectionString, String username, String password) throws Exception {
        throw new UnsupportedOperationException("MongoDB does not use JDBC connections");
    }

    @Override
    public List<String> extractSchemas(Connection connection) {
        throw new UnsupportedOperationException("Use extractDatabases() for MongoDB");
    }
    
    public List<String> extractDatabases(String connectionString) {
        List<String> databases = new ArrayList<>();
        try (MongoClient mongoClient = createMongoClient(connectionString)) {
            for (String dbName : mongoClient.listDatabaseNames()) {
                if (!dbName.equals("admin") && !dbName.equals("config") && !dbName.equals("local")) {
                    databases.add(dbName);
                }
            }
        } catch (Exception e) {
            log.error("Error extracting MongoDB databases: {}", e.getMessage());
        }
        return databases;
    }

    @Override
    public List<TableMetadataDTO> extractTables(Connection connection, String schema) {
        throw new UnsupportedOperationException("Use extractCollections() for MongoDB");
    }
    
    public List<TableMetadataDTO> extractCollections(String connectionString, String databaseName, List<String> collectionsToExtract) {
        List<TableMetadataDTO> collections = new ArrayList<>();
        try (MongoClient mongoClient = createMongoClient(connectionString)) {
            MongoDatabase database = mongoClient.getDatabase(databaseName);
            
            // Get list of collections to extract
            List<String> collectionNames;
            if (collectionsToExtract != null && !collectionsToExtract.isEmpty()) {
                collectionNames = collectionsToExtract;
            } else {
                collectionNames = new ArrayList<>();
                for (String name : database.listCollectionNames()) {
                    collectionNames.add(name);
                }
            }
            
            for (String collectionName : collectionNames) {
                try {
                    TableMetadataDTO collection = TableMetadataDTO.builder()
                        .schema(databaseName)
                        .tableName(collectionName)
                        .description("MongoDB Collection")
                        .build();
                    
                    // Get document count
                    MongoCollection<Document> coll = database.getCollection(collectionName);
                    long count = coll.countDocuments();
                    collection.setRowCount(count);
                    
                    // Estimate size
                    Document stats = database.runCommand(new Document("collStats", collectionName));
                    long size = stats.get("size", 0L);
                    collection.setSizeBytes(size);
                    
                    collections.add(collection);
                } catch (Exception e) {
                    log.error("Error extracting collection {}: {}", collectionName, e.getMessage());
                }
            }
        } catch (Exception e) {
            log.error("Error extracting MongoDB collections: {}", e.getMessage());
        }
        return collections;
    }
    
    public List<TableMetadataDTO> extractCollections(String connectionString, String databaseName) {
        return extractCollections(connectionString, databaseName, null);
    }

    @Override
    public List<FieldMetadataDTO> extractFields(Connection connection, String schema, String tableName) {
        throw new UnsupportedOperationException("Use extractFields(connectionString, database, collection) for MongoDB");
    }
    
    public List<FieldMetadataDTO> extractFields(String connectionString, String databaseName, String collectionName) {
        List<FieldMetadataDTO> fields = new ArrayList<>();
        Set<String> fieldNames = new HashSet<>();
        
        try (MongoClient mongoClient = createMongoClient(connectionString)) {
            MongoDatabase database = mongoClient.getDatabase(databaseName);
            MongoCollection<Document> collection = database.getCollection(collectionName);
            
            // Sample documents to infer schema
            int sampleSize = 100;
            try (MongoCursor<Document> cursor = collection.find().limit(sampleSize).iterator()) {
                while (cursor.hasNext()) {
                    Document doc = cursor.next();
                    extractFieldsFromDocument(doc, "", fieldNames, fields);
                }
            }
            
        } catch (Exception e) {
            log.error("Error extracting MongoDB fields: {}", e.getMessage());
        }
        
        return fields;
    }
    
    private void extractFieldsFromDocument(Document doc, String prefix, Set<String> fieldNames, List<FieldMetadataDTO> fields) {
        for (String key : doc.keySet()) {
            String fullKey = prefix.isEmpty() ? key : prefix + "." + key;
            
            if (!fieldNames.contains(fullKey)) {
                fieldNames.add(fullKey);
                
                Object value = doc.get(key);
                String dataType = inferMongoDBType(value);
                
                FieldMetadataDTO field = FieldMetadataDTO.builder()
                    .fieldName(fullKey)
                    .businessName(formatBusinessName(fullKey))
                    .dataType(dataType)
                    .isPrimaryKey("_id".equals(key))
                    .isForeignKey(false)
                    .isNullable(true)
                    .sensitivityLevel(isPII(fullKey, dataType) ? "PII" : "INTERNAL")
                    .build();
                
                fields.add(field);
                
                // Recursively extract nested documents
                if (value instanceof Document) {
                    extractFieldsFromDocument((Document) value, fullKey, fieldNames, fields);
                }
            }
        }
    }
    
    private String inferMongoDBType(Object value) {
        if (value == null) return "null";
        if (value instanceof String) return "string";
        if (value instanceof Integer) return "int";
        if (value instanceof Long) return "long";
        if (value instanceof Double) return "double";
        if (value instanceof Boolean) return "boolean";
        if (value instanceof Date) return "date";
        if (value instanceof Document) return "document";
        if (value instanceof List) return "array";
        return value.getClass().getSimpleName();
    }

    @Override
    public List<RelationshipDTO> extractRelationships(Connection connection, String schema) {
        // MongoDB doesn't have built-in foreign keys
        // Would need to analyze references in documents
        return new ArrayList<>();
    }

    @Override
    public Map<String, Object> extractTableStatistics(Connection connection, String schema, String tableName) {
        throw new UnsupportedOperationException("Use extractCollectionStatistics() for MongoDB");
    }
    
    public Map<String, Object> extractCollectionStatistics(String connectionString, String databaseName, String collectionName) {
        Map<String, Object> stats = new HashMap<>();
        
        try (MongoClient mongoClient = createMongoClient(connectionString)) {
            MongoDatabase database = mongoClient.getDatabase(databaseName);
            MongoCollection<Document> collection = database.getCollection(collectionName);
            
            stats.put("rowCount", collection.countDocuments());
            
            Document collStats = database.runCommand(new Document("collStats", collectionName));
            stats.put("sizeBytes", collStats.get("size", 0L));
            stats.put("avgObjSize", collStats.get("avgObjSize", 0L));
            stats.put("storageSize", collStats.get("storageSize", 0L));
            
        } catch (Exception e) {
            log.error("Error extracting MongoDB statistics: {}", e.getMessage());
            stats.put("rowCount", 0L);
            stats.put("sizeBytes", 0L);
        }
        
        return stats;
    }

    @Override
    public boolean isPII(String fieldName, String dataType) {
        String lowerFieldName = fieldName.toLowerCase();
        return PII_KEYWORDS.stream().anyMatch(lowerFieldName::contains);
    }

    @Override
    public List<LineageRelationshipDTO> buildLineage(Connection connection, String schema) {
        // MongoDB lineage would require analyzing document references
        return new ArrayList<>();
    }

    @Override
    public DataQualityDTO calculateQuality(Connection connection, String schema, String tableName) {
        throw new UnsupportedOperationException("Use calculateQuality(connectionString, database, collection) for MongoDB");
    }
    
    public DataQualityDTO calculateQuality(String connectionString, String databaseName, String collectionName) {
        DataQualityDTO quality = DataQualityDTO.builder()
            .tableName(collectionName)
            .build();
        
        try {
            List<FieldMetadataDTO> fields = extractFields(connectionString, databaseName, collectionName);
            
            // Completeness based on field presence
            double completeness = fields.isEmpty() ? 0 : 95.0; // Estimated
            quality.setCompleteness(completeness);
            
            // Validity based on data types
            double validity = 90.0; // Estimated
            quality.setValidity(validity);
            
            double overall = (completeness + validity) / 2;
            quality.setOverallScore(overall);
            
        } catch (Exception e) {
            log.error("Error calculating MongoDB quality: {}", e.getMessage());
        }
        
        return quality;
    }
    
    private MongoClient createMongoClient(String connectionString) {
        ConnectionString connString = new ConnectionString(connectionString);
        MongoClientSettings settings = MongoClientSettings.builder()
            .applyConnectionString(connString)
            .build();
        return MongoClients.create(settings);
    }

    private String formatBusinessName(String fieldName) {
        return Arrays.stream(fieldName.split("[._]"))
            .map(word -> word.substring(0, 1).toUpperCase() + word.substring(1).toLowerCase())
            .reduce((a, b) -> a + " " + b)
            .orElse(fieldName);
    }
}
