package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Main class that orchestrates the NGSI-LD to HBase pipeline.
 * Fetches entities from Orion-LD, parses them, and inserts them into HBase.
 */
public class OrionToHBase {
    private static final Logger LOGGER = Logger.getLogger(OrionToHBase.class.getName());
    
    // Known entity types from our dataset
    private static final String[] ENTITY_TYPES = {
        "BrightnessSensor",
        "HumiditySensor",
        "TemperatureSensor",
        "ThermostatTemperatureSensor",
        "SetpointHistorySensor",
        "VirtualOutdoorTemperatureSensor",
        "OutdoorTemperatureSensor",
        "Sensor" // Generic sensor type
    };
    
    private final OrionFetcher orionFetcher;
    private final HBaseInserter hbaseInserter;
    private final Connection hbaseConnection;
    
    /**
     * Constructor initializes the pipeline components.
     * 
     * @param orionHost Orion-LD host name
     * @param orionPort Orion-LD port
     * @param zookeeperQuorum ZooKeeper quorum for HBase connection
     * @throws Exception If initialization fails
     */
    public OrionToHBase(String orionHost, int orionPort, String zookeeperQuorum) throws Exception {
        // Initialize Orion-LD fetcher
        LOGGER.info("Initializing OrionFetcher with host: " + orionHost + ", port: " + orionPort);
        this.orionFetcher = new OrionFetcher(orionHost, orionPort);
        
        // Test Orion-LD connection
        if (!orionFetcher.testConnection()) {
            throw new Exception("Failed to connect to Orion-LD");
        }
        LOGGER.info("Successfully connected to Orion-LD");
        
        // Initialize HBase connection
        LOGGER.info("Initializing HBase connection with ZooKeeper quorum: " + zookeeperQuorum);
        Configuration hbaseConfig = new Configuration();
        hbaseConfig.set("hbase.zookeeper.quorum", zookeeperQuorum);
        
        this.hbaseConnection = ConnectionFactory.createConnection(hbaseConfig);
        if (this.hbaseConnection == null || this.hbaseConnection.isClosed()) {
            throw new Exception("Failed to connect to HBase");
        }
        LOGGER.info("Successfully connected to HBase");
        
        // Initialize HBase inserter
        this.hbaseInserter = new HBaseInserter();
        LOGGER.info("Initialized HBaseInserter");
    }
    
    /**
     * Process a single entity type, fetching entities and inserting them into HBase.
     * This method now handles pagination for large entity sets.
     * 
     * @param entityType The type of entities to process
     * @param batchSize Number of entities to process in a batch
     * @return Number of entities successfully processed
     */
    public int processEntityType(String entityType, int batchSize) {
        LOGGER.info("Processing entity type: " + entityType + " with batch size: " + batchSize);
        int processedCount = 0;
        int offset = 0;
        int batchNum = 0;
        boolean hasMoreEntities = true;
        
        try {
            while (hasMoreEntities) {
                batchNum++;
                LOGGER.info(String.format("[%s] Fetching batch #%d (offset: %d, limit: %d)", 
                        entityType, batchNum, offset, batchSize));
                
                // Fetch entities from Orion-LD with pagination
                List<JSONObject> entities = orionFetcher.fetchEntitiesWithPagination(entityType, batchSize, offset);
                
                // If we got fewer entities than the batch size, this is the last batch
                if (entities.size() < batchSize) {
                    hasMoreEntities = false;
                    LOGGER.info(String.format("[%s] Reached final batch with %d entities", entityType, entities.size()));
                }
                
                if (entities.isEmpty()) {
                    LOGGER.info(String.format("[%s] No more entities to fetch", entityType));
                    break;
                }
                
                LOGGER.info(String.format("[%s] Fetched %d entities in batch #%d", 
                        entityType, entities.size(), batchNum));
                
                // Parse and insert each entity
                List<SensorEntity> sensorEntities = new ArrayList<>();
                for (JSONObject entity : entities) {
                    SensorEntity sensorEntity = SensorEntityParser.parse(entity);
                    if (sensorEntity != null) {
                        sensorEntities.add(sensorEntity);
                    }
                }
                
                LOGGER.info(String.format("[%s] Parsed %d valid entities in batch #%d", 
                        entityType, sensorEntities.size(), batchNum));
                
                // Insert valid entities into HBase
                int insertedCount = hbaseInserter.batchInsert(sensorEntities, hbaseConnection);
                LOGGER.info(String.format("[%s] Inserted %d entities into HBase in batch #%d", 
                        entityType, insertedCount, batchNum));
                
                processedCount += insertedCount;
                
                // Update offset for next batch
                offset += batchSize;
                
                // Log running total
                LOGGER.info(String.format("[%s] Running total: %d entities processed", entityType, processedCount));
            }
            
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error processing entity type: " + entityType + " at offset " + offset, e);
        }
        
        return processedCount;
    }
    
    /**
     * Execute a complete pipeline run for all known entity types.
     * 
     * @param batchSize Number of entities to process in a batch
     * @return Total number of entities processed
     */
    public int executePipeline(int batchSize) {
        LOGGER.info("Executing complete pipeline for all entity types with batch size: " + batchSize);
        int totalProcessed = 0;
        
        for (String entityType : ENTITY_TYPES) {
            LOGGER.info("=== Processing entity type: " + entityType + " ===");
            int processed = processEntityType(entityType, batchSize);
            totalProcessed += processed;
            LOGGER.info("=== Completed processing " + processed + " entities of type " + entityType + " ===");
        }
        
        LOGGER.info("Pipeline execution complete. Total entities processed: " + totalProcessed);
        return totalProcessed;
    }
    
    /**
     * Close all connections.
     */
    public void close() {
        try {
            if (hbaseConnection != null && !hbaseConnection.isClosed()) {
                hbaseConnection.close();
                LOGGER.info("HBase connection closed");
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error closing HBase connection", e);
        }
    }
    
    /**
     * Main method to run the pipeline.
     * 
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        LOGGER.info("OrionToHBase application starting...");
        
        String orionHost = System.getenv("ORION_HOST");
        if (orionHost == null || orionHost.isEmpty()) {
            orionHost = "fiware-orion"; // Default in Docker environment
        }
        
        String zookeeperQuorum = System.getenv("HBASE_ZOOKEEPER_QUORUM");
        if (zookeeperQuorum == null || zookeeperQuorum.isEmpty()) {
            zookeeperQuorum = "hbase"; // Default in Docker environment
        }
        
        int orionPort = 1026; // Default NGSI-LD port
        int batchSize = 500; // Increased batch size for efficient full migration
        
        OrionToHBase pipeline = null;
        try {
            // Initialize the pipeline
            pipeline = new OrionToHBase(orionHost, orionPort, zookeeperQuorum);
            LOGGER.info("Pipeline initialized successfully");
            
            // Execute the pipeline
            int processedCount = pipeline.executePipeline(batchSize);
            LOGGER.info("Pipeline execution completed. Total entities processed: " + processedCount);
            
            // After successful execution, keep the application running for continuous operation
            LOGGER.info("Pipeline is now in idle state. Container will remain running.");
            while (true) {
                Thread.sleep(60000); // Sleep for 1 minute
                // Additional periodic pipeline runs could be triggered here
            }
            
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Pipeline execution failed", e);
        } finally {
            // Clean up resources
            if (pipeline != null) {
                pipeline.close();
            }
        }
    }
}
