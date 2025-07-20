package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Creates the SensorData table in HBase with appropriate column families.
 * 
 * This class is responsible for setting up the HBase table structure required
 * for storing sensor data from Orion-LD. It creates column families for each room
 * to allow efficient querying by room.
 */
public class CreateSensorTable {
    private static final Logger logger = LoggerFactory.getLogger(CreateSensorTable.class);
    
    // Table name constant
    public static final String TABLE_NAME = "SensorData";
    
    // Column families based on room names
    public static final String[] COLUMN_FAMILIES = {
        "bathroom",
        "kitchen",
        "room1",
        "room2",
        "room3",
        "toilet"
    };
    
    /**
     * Creates the SensorData table if it doesn't already exist.
     * 
     * @return true if table was created or already exists, false if an error occurred
     */
    public static boolean createTable() {
        logger.info("Starting HBase table creation process for: {}", TABLE_NAME);
        
        // Create HBase configuration
        Configuration config = configureHBaseConnection();
        
        // Set up retry parameters
        int maxRetries = 20; // Increased from 10 to 20 for better resilience
        int currentRetry = 0;
        long initialBackoffMs = 10000; // 10 seconds initial backoff (increased from 5s)
        
        while (currentRetry <= maxRetries) {
            try {
                boolean result = attemptTableCreation(config);
                if (result) {
                    logger.info("Table {} successfully created or already exists after {} attempts", TABLE_NAME, currentRetry + 1);
                    return true;
                }
            } catch (Exception e) {
                // Check specifically for PleaseHoldException which means master is still initializing
                if (e.toString().contains("PleaseHoldException") || e.getMessage().contains("Master is initializing")) {
                    logger.warn("HBase master is not fully initialized yet: callTimeout={}, callDuration={}: {}", 
                        config.getInt("hbase.client.operation.timeout", 60000),
                        currentRetry * initialBackoffMs, 
                        e.getMessage());
                } else {
                    logger.warn("Attempt {} failed to create table: {}", currentRetry + 1, e.getMessage());
                }
                
                // Check if this is the last retry
                if (currentRetry >= maxRetries) {
                    logger.error("Failed to create HBase table after {} attempts. Last error: {}", maxRetries + 1, e.getMessage());
                    return false;
                }
                
                // Calculate backoff time with exponential backoff
                long backoffMs = initialBackoffMs * (long)Math.pow(2, currentRetry);
                logger.info("Waiting {} ms before retry attempt {}", backoffMs, currentRetry + 2);
                
                try {
                    Thread.sleep(backoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    logger.error("Thread interrupted during retry backoff", ie);
                    return false;
                }
            }
            
            currentRetry++;
        }
        
        logger.error("Failed to create HBase table after {} attempts", maxRetries + 1);
        return false;
    }
    
    /**
     * Attempts to create the table with the current configuration
     * 
     * @param attempt the current attempt number
     * @throws IOException if there was an error communicating with HBase
     * @throws InterruptedException if the thread was interrupted during the attempt
     */
    private static boolean attemptTableCreation(Configuration config) throws IOException, InterruptedException {
        logger.info("Attempting to create table {}", TABLE_NAME);

        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            TableName tableName = TableName.valueOf(TABLE_NAME);

            // Check if table exists first
            if (admin.tableExists(tableName)) {
                logger.info("Table {} already exists - success!", TABLE_NAME);
                return true;
            }

            // Wait for HBase master to be available with detailed logging
            logger.info("Checking if HBase master is available...");
            try {
                // Verify cluster status
                try {
                    logger.info("Checking HBase cluster status...");
                    TableName[] tableNames = admin.listTableNames();
                    logger.info("Successfully connected to HBase - Found {} existing tables", tableNames.length);
                    if (tableNames.length > 0) {
                        StringBuilder tableList = new StringBuilder();
                        for (int i = 0; i < tableNames.length; i++) {
                            if (i > 0) tableList.append(", ");
                            tableList.append(tableNames[i].getNameAsString());
                        }
                        logger.info("Existing tables: {}", tableList.toString());
                    }
                } catch (Exception tableListException) {
                    logger.warn("Could not list tables: {} - {}", tableListException.getClass().getName(), tableListException.getMessage());
                }
                
                // Verify namespaces
                try {
                    logger.info("Checking HBase namespaces...");
                    NamespaceDescriptor[] nsDescriptors = admin.listNamespaceDescriptors();
                    List<String> namespaceNames = new ArrayList<>();
                    for (NamespaceDescriptor ns : nsDescriptors) {
                        namespaceNames.add(ns.getName());
                    }
                    logger.info("Successfully listed namespaces: {}", namespaceNames);
                } catch (Exception nsException) {
                    logger.warn("Could not list namespaces: {} - {}", nsException.getClass().getName(), nsException.getMessage());
                    if (nsException.toString().contains("PleaseHoldException")) {
                        throw nsException; // Will trigger retry if it's a PleaseHoldException
                    }
                }

                logger.info("HBase master is running and responding to API calls");
            } catch (Exception e) {
                if (e.toString().contains("PleaseHoldException") ||
                    e.toString().contains("MasterNotRunningException") ||
                    e.toString().contains("ConnectionException") ||
                    e.toString().contains("ConnectException")) {
                    logger.warn("HBase master is not ready: {} - {}", e.getClass().getName(), e.getMessage());
                    throw e; // Will trigger retry
                }
                logger.warn("Unexpected error checking master status: {} - {}", e.getClass().getName(), e.getMessage());
                e.printStackTrace(); // Print full stack trace for debugging
            }

            // Verify HBase services for detailed debugging
            logger.info("HBase service verification before table creation:");
            logger.info("  - Admin interface available: {}", admin != null);
            logger.info("  - Connection status: {}", connection.isClosed() ? "CLOSED" : "OPEN");

            // Create table descriptor
            logger.info("Creating table descriptor for {}", TABLE_NAME);
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);

            // Add column families
            logger.info("Adding column families: {}", String.join(", ", COLUMN_FAMILIES));
            for (String columnFamily : COLUMN_FAMILIES) {
                ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = 
                    ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily));
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
                logger.info("Added column family: {}", columnFamily);
            }

            // Create table
            logger.info("Creating table {} with descriptor", TABLE_NAME);
            TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
            
            // Execute table creation with additional error handling
            try {
                logger.info("Executing admin.createTable() call...");
                admin.createTable(tableDescriptor);
                logger.info("admin.createTable() call completed");
            } catch (Exception createEx) {
                logger.error("Failed to create table: {} - {}", createEx.getClass().getName(), createEx.getMessage());
                createEx.printStackTrace();
                throw createEx;
            }
            
            // Double check table creation with retries
            logger.info("Verifying table creation...");
            boolean tableExists = false;
            for (int i = 0; i < 5; i++) {
                try {
                    tableExists = admin.tableExists(tableName);
                    if (tableExists) {
                        break;
                    }
                    logger.info("Table not found yet, waiting 2 seconds... (attempt {})", i+1);
                    Thread.sleep(2000);
                } catch (Exception checkEx) {
                    logger.warn("Error checking if table exists: {}", checkEx.getMessage());
                    Thread.sleep(2000);
                }
            }
            
            if (tableExists) {
                logger.info("VERIFIED: Table {} exists after creation", TABLE_NAME);
                return true;
            } else {
                logger.error("FAILED: Table {} does not exist after creation call", TABLE_NAME);
                return false;
            }
        } catch (Exception e) {
            logger.error("Error during table creation process: {} - {}", e.getClass().getName(), e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }
    
    /**
     * Configures the HBase connection with appropriate settings for Docker environment.
     * Uses localhost for all connections to prevent hostname resolution issues.
     * 
     * @return A properly configured HBase Configuration object
     */
    public static Configuration configureHBaseConnection() {
        // Get HBase host from environment or use default for Docker
        String hbaseHost = System.getenv("HBASE_ZOOKEEPER_QUORUM") != null ? 
                          System.getenv("HBASE_ZOOKEEPER_QUORUM") : "hbase";
        String zookeeperPort = System.getenv("HBASE_ZOOKEEPER_PORT") != null ? 
                             System.getenv("HBASE_ZOOKEEPER_PORT") : "2181";
                             
        logger.info("Configuring HBase connection to use host: {} and port: {}", hbaseHost, zookeeperPort);
        
        try {
            // Try to resolve HBase hostname to verify connectivity
            try {
                java.net.InetAddress addr = java.net.InetAddress.getByName(hbaseHost);
                logger.info("Successfully resolved HBase host: {} -> {}", hbaseHost, addr.getHostAddress());
            } catch (Exception e) {
                logger.warn("Could not resolve HBase host {}: {}", hbaseHost, e.getMessage());
            }
        } catch (Exception ex) {
            logger.warn("Failed during hostname resolution testing: {}", ex.getMessage());
        }
        
        Configuration config = HBaseConfiguration.create();
        
        // Use Docker service names for container networking
        config.set("hbase.zookeeper.quorum", hbaseHost);
        config.set("hbase.zookeeper.property.clientPort", zookeeperPort);
        config.set("hbase.master", hbaseHost + ":16000");
        
        // Let Docker DNS handle hostname resolution
        config.set("hbase.regionserver.hostname.disable.master.reversedns", "true");
        // No need to set regionserver hostname, let HBase use the service discovery
        
        // More aggressive settings for hostname resolution and timeouts
        config.set("hbase.client.pause", "2000"); // 2 second pause between retries (reduced from 5s for more responsive retries)
        config.setInt("hbase.client.retries.number", 50);
        config.setInt("hbase.client.operation.timeout", 300000);    // 5 minutes
        config.setInt("hbase.client.meta.operation.timeout", 600000); // 10 minutes
        config.setInt("hbase.rpc.timeout", 120000);                  // 2 minutes
        config.setInt("zookeeper.session.timeout", 180000);          // 3 minutes
        config.setInt("zookeeper.recovery.retry", 10);
        config.setInt("hbase.client.scanner.timeout.period", 180000); // 3 minutes
        
        // Enable connection pooling for better performance and connection management
        config.setBoolean("hbase.client.ipc.pool.type", true);
        config.set("hbase.ipc.client.tcpnodelay", "true"); // Improve TCP performance
        
        // Disable hostname reverse DNS lookups which can cause timeouts
        config.setBoolean("hbase.regionserver.hostname.disable.master.reversedns", true);
        
        // Set more lenient timeouts for ZooKeeper operations
        config.set("hbase.zookeeper.recoverable.waittime", "60000");
        
        // Increase write buffer for better batch performance later
        config.setLong("hbase.client.write.buffer", 8 * 1024 * 1024); // 8MB write buffer
        
        logger.info("HBase configuration complete with robust connection settings and retry logic");
        
        // Validate that ZooKeeper is actually reachable with a direct socket connection test
        try {
            logger.info("Attempting to validate ZooKeeper connection at {}:{}", hbaseHost, zookeeperPort);
            java.net.Socket socket = new java.net.Socket();
            socket.connect(new java.net.InetSocketAddress(hbaseHost, Integer.parseInt(zookeeperPort)), 5000);
            if (socket.isConnected()) {
                logger.info("✅ ZooKeeper port {} is reachable!", zookeeperPort);
                socket.close();
            }
        } catch (Exception e) {
            logger.warn("⚠️ Could not connect to ZooKeeper port {}: {}", zookeeperPort, e.getMessage());
            logger.debug("ZooKeeper connectivity error details", e);
        }
        
        // Also check if HBase Master port is reachable
        try {
            int hbaseMasterPort = 16000;
            logger.info("Checking if HBase Master is reachable at {}:{}", hbaseHost, hbaseMasterPort);
            java.net.Socket socket = new java.net.Socket();
            socket.connect(new java.net.InetSocketAddress(hbaseHost, hbaseMasterPort), 5000);
            if (socket.isConnected()) {
                logger.info("✅ HBase Master port {} is reachable!", hbaseMasterPort);
                socket.close();
            }
        } catch (Exception e) {
            logger.warn("⚠️ Could not connect to HBase Master port: {}", e.getMessage());
        }
        
        logger.debug("HBase connection configured with aggressive hostname mappings");
        return config;
    }
    
    /**
     * Main method for standalone execution.
     * 
     * @param args Command line arguments (not used)
     */
    public static void main(String[] args) {
        boolean success = createTable();
        
        if (success) {
            System.out.println("✅ SensorData table created or already exists in HBase.");
        } else {
            System.out.println("❌ Failed to create SensorData table in HBase.");
            System.exit(1);
        }
    }
}
