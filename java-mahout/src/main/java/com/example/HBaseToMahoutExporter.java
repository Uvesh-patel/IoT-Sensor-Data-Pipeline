package com.example;

import org.apache.commons.cli.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


/**
 * HBaseToMahoutExporter - Exports sensor data from HBase to CSV format for machine learning analysis.
 * 
 * This application connects to an HBase cluster and extracts IoT sensor data from the SensorData table,
 * converting it to a clean CSV format suitable for Apache Mahout and other ML frameworks.
 * 
 * The exported data includes:
 * - Row keys (NGSI-LD sensor identifiers)
 * - Sensor values (numerical readings)
 * - Timestamps (ISO 8601 format)
 * - Room locations (extracted from entity IDs)
 * - Sensor types (brightness, humidity, temperature, etc.)
 * 
 * Usage:
 * - Test connection: java HBaseToMahoutExporter --test
 * - Export all data: java HBaseToMahoutExporter --export output.csv
 * - Filter by sensor: java HBaseToMahoutExporter --export output.csv --sensor-type brightness
 * - Filter by room: java HBaseToMahoutExporter --export output.csv --room kitchen
 * - Limit rows: java HBaseToMahoutExporter --export output.csv --limit 1000
 * 
 * @author IoT Data Processing Pipeline
 * @version 1.0
 * @since 2024
 */
public class HBaseToMahoutExporter {
    private static final Logger logger = LoggerFactory.getLogger(HBaseToMahoutExporter.class);
    
    // HBase configuration constants
    private static final String TABLE_NAME = "SensorData";  // HBase table containing sensor data
    private static final String ROW_KEY_PREFIX = "urn:ngsi-ld:Sensor:";  // NGSI-LD entity prefix
    
    // CSV output format configuration
    private static final String[] CSV_HEADERS = {"rowKey", "value", "timestamp", "room", "sensorType"};
    
    // HBase connection configuration
    private Connection connection;
    private Configuration config;
    
    /**
     * Constructor - Initialize HBase configuration for connection.
     * 
     * Sets up HBase client configuration with ZooKeeper connection parameters.
     * Uses environment variables for flexibility in different deployment environments.
     */
    public HBaseToMahoutExporter() {
        // Initialize HBase configuration object
        config = HBaseConfiguration.create();
        
        // Get HBase ZooKeeper quorum from environment (Docker service name or IP)
        String zkQuorum = System.getenv("HBASE_ZOOKEEPER_QUORUM");
        if (zkQuorum == null || zkQuorum.isEmpty()) {
            zkQuorum = "hbase";  // Default to Docker Compose service name
        }
        
        logger.info("Configuring HBase connection with ZooKeeper quorum: {}", zkQuorum);
        
        // Configure HBase client connection parameters
        config.set("hbase.zookeeper.quorum", zkQuorum);  // ZooKeeper ensemble
        config.set("hbase.zookeeper.property.clientPort", "2181");  // ZooKeeper port
    }
    
    /**
     * Connect to HBase
     * @return true if connection successful, false otherwise
     */
    public boolean connect() {
        try {
            connection = ConnectionFactory.createConnection(config);
            logger.info("Successfully connected to HBase");
            return true;
        } catch (IOException e) {
            logger.error("Failed to connect to HBase", e);
            return false;
        }
    }
    
    /**
     * Close HBase connection
     */
    public void close() {
        if (connection != null) {
            try {
                connection.close();
                logger.info("HBase connection closed");
            } catch (IOException e) {
                logger.error("Error closing HBase connection", e);
            }
        }
    }
    
    /**
     * Check if the HBase SensorData table exists
     * @return true if table exists, false otherwise
     */
    public boolean tableExists() {
        try {
            Admin admin = connection.getAdmin();
            boolean exists = admin.tableExists(TableName.valueOf(TABLE_NAME));
            admin.close();
            return exists;
        } catch (IOException e) {
            logger.error("Error checking if table exists", e);
            return false;
        }
    }
    
    /**
     * Export sensor data from HBase to CSV format for machine learning analysis.
     * 
     * This method scans the HBase SensorData table and exports matching records to a CSV file.
     * It supports filtering by sensor type and room, and can limit the number of exported rows.
     * 
     * The export process:
     * 1. Creates output directory if it doesn't exist
     * 2. Sets up HBase table scanner with optional filters
     * 3. Iterates through matching rows, parsing NGSI-LD entity data
     * 4. Extracts room and sensor type from entity IDs
     * 5. Writes formatted data to CSV with proper headers
     * 
     * @param outputPath Path where CSV file will be written (creates parent directories if needed)
     * @param sensorType Optional sensor type filter (e.g., "brightness", "humidity", "temperature")
     * @param roomFilter Optional room filter (e.g., "kitchen", "bathroom", "room1")
     * @param limit Maximum number of rows to export (0 for unlimited, useful for testing)
     * @return Number of rows successfully exported to CSV
     * @throws IOException if file operations fail
     * @throws RuntimeException if HBase operations fail
     */
    public int exportToCsv(String outputPath, String sensorType, String roomFilter, int limit) {
        int rowCount = 0;
        
        try {
            // Create directory if it doesn't exist
            Path directory = Paths.get(outputPath).getParent();
            if (!Files.exists(directory)) {
                Files.createDirectories(directory);
            }
            
            // Create CSV printer
            FileWriter fileWriter = new FileWriter(outputPath);
            CSVPrinter csvPrinter = new CSVPrinter(fileWriter, CSVFormat.DEFAULT.withHeader(CSV_HEADERS));
            
            // Get reference to HBase table containing sensor data
            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
            
            // Create scanner to iterate through table rows
            Scan scan = new Scan();
            
            // Configure row limit for export (useful for testing or partial exports)
            if (limit > 0) {
                logger.debug("Setting scan limit to {} rows for controlled export", limit);
                scan.setLimit(limit);
            } else {
                logger.debug("No scan limit specified - processing entire table");
            }
            
            // Add row prefix filter if sensor type and room filter are specified
            StringBuilder rowPrefixBuilder = new StringBuilder(ROW_KEY_PREFIX);
            boolean hasFilter = false;
            
            if (roomFilter != null && !roomFilter.isEmpty()) {
                rowPrefixBuilder.append(roomFilter).append("_");
                hasFilter = true;
                logger.info("Filtering by room: {}", roomFilter);
            }
            
            if (sensorType != null && !sensorType.isEmpty()) {
                if (hasFilter) {
                    rowPrefixBuilder.append(sensorType.toLowerCase()).append("_");
                }
                logger.info("Filtering by sensor type: {}", sensorType);
            }
            
            if (hasFilter) {
                scan.setRowPrefixFilter(Bytes.toBytes(rowPrefixBuilder.toString()));
            }
            
            // Execute scan
            ResultScanner scanner = table.getScanner(scan);
            logger.info("Scan started");
            
            // Iterate through all matching rows in the HBase table
            for (Result result : scanner) {
                // Extract the row key (NGSI-LD entity identifier)
                String rowKey = Bytes.toString(result.getRow());
                
                // Periodic debug logging to track progress (every 5000 rows)
                if (rowCount % 5000 == 0 && rowCount > 0) {
                    logger.debug("Processing row: {}", rowKey);
                }
                
                // Parse the row key to extract components (format: urn:ngsi-ld:Sensor:room_sensortype_number)
                if (!rowKey.startsWith(ROW_KEY_PREFIX)) {
                    // Validate row key format (must be NGSI-LD sensor entity)
                    continue;  // Skip non-sensor entities
                }
                
                // Parse NGSI-LD entity ID to extract room and sensor type
                // Format: urn:ngsi-ld:Sensor:{room}_{sensorType}_{id}
                String entityId = rowKey.substring(ROW_KEY_PREFIX.length());
                String[] parts = entityId.split("_", 3);  // Split into room, type, id
                if (parts.length < 2) {
                    continue; // Skip malformed entity IDs that don't follow expected pattern
                }
                
                String room = parts[0];        // Extract room location (e.g., "kitchen", "bathroom")
                String sensorTypeFromId = parts[1];  // Extract sensor type (e.g., "brightness", "humidity")
                
                // Apply optional filters to reduce exported data
                if (roomFilter != null && !roomFilter.isEmpty() && !room.equals(roomFilter)) {
                    continue;  // Skip if room doesn't match filter
                }
                
                if (sensorType != null && !sensorType.isEmpty() && 
                    !sensorTypeFromId.toLowerCase().contains(sensorType.toLowerCase())) {
                    continue;  // Skip if sensor type doesn't match filter
                }
                
                // Process all cells in this row (HBase stores data in column families)
                for (Cell cell : result.rawCells()) {
                    // Extract cell value which contains sensor reading and timestamp
                    // Format stored in HBase: "sensorValue|timestamp"
                    String cellValue = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    
                    // Parse the pipe-separated value and timestamp
                    String[] valueParts = cellValue.split("\\|");
                    if (valueParts.length >= 2) {
                        String value = valueParts[0];      // Numerical sensor reading
                        String timestamp = valueParts[1];  // ISO 8601 timestamp
                        
                        // Write complete record to CSV file
                        csvPrinter.printRecord(rowKey, value, timestamp, room, sensorTypeFromId);
                        rowCount++;
                        
                        // Log progress every 1000 rows
                        if (rowCount % 1000 == 0) {
                            logger.info("Exported {} rows", rowCount);
                        }
                        
                        break; // Process only first cell per row (sufficient for our data model)
                    }
                }
            }
            
            // Close CSV writer resources
            csvPrinter.close();
            fileWriter.close();
            
            // Log completion status with export statistics
            logger.info("CSV export completed successfully. Total rows exported: {}", rowCount);
            
        } catch (IOException e) {
            logger.error("Error exporting data to CSV", e);
        }
        
        return rowCount;
    }
    
    /**
     * Test HBase connectivity
     * @return true if connection test passes, false otherwise
     */
    public boolean testConnection() {
        if (!connect()) {
            return false;
        }
        
        boolean success = tableExists();
        if (success) {
            logger.info("Successfully connected to HBase and verified '{}' table exists", TABLE_NAME);
        } else {
            logger.error("Connection successful but '{}' table does not exist", TABLE_NAME);
        }
        
        close();
        return success;
    }
    
    /**
     * Main method
     * @param args command line arguments
     */
    public static void main(String[] args) {
        Options options = new Options();
        
        Option testOpt = new Option("t", "test", false, "Test HBase connectivity");
        Option exportOpt = Option.builder("e")
                .longOpt("export")
                .hasArg()
                .argName("OUTPUT_PATH")
                .desc("Export data to CSV (path required)")
                .build();
        Option typeOpt = Option.builder("s")
                .longOpt("sensor-type")
                .hasArg()
                .argName("TYPE")
                .desc("Filter by sensor type (e.g., Brightness, Humidity)")
                .build();
        Option roomOpt = Option.builder("r")
                .longOpt("room")
                .hasArg()
                .argName("ROOM_ID")
                .desc("Filter by room ID")
                .build();
        Option limitOpt = Option.builder("l")
                .longOpt("limit")
                .hasArg()
                .argName("COUNT")
                .desc("Limit number of rows exported")
                .type(Integer.class)
                .build();
        
        options.addOption(testOpt);
        options.addOption(exportOpt);
        options.addOption(typeOpt);
        options.addOption(roomOpt);
        options.addOption(limitOpt);
        
        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        
        try {
            CommandLine cmd = parser.parse(options, args);
            HBaseToMahoutExporter exporter = new HBaseToMahoutExporter();
            
            if (cmd.hasOption("test")) {
                // Test HBase connectivity
                boolean success = exporter.testConnection();
                System.exit(success ? 0 : 1);
            } else if (cmd.hasOption("export")) {
                // Export to CSV
                String outputPath = cmd.getOptionValue("export");
                String sensorType = cmd.getOptionValue("sensor-type", null);
                String roomFilter = cmd.getOptionValue("room", null);
                int limit = Integer.parseInt(cmd.getOptionValue("limit", "0"));
                
                exporter.connect();
                if (!exporter.tableExists()) {
                    logger.error("SensorData table does not exist. Export aborted.");
                    exporter.close();
                    System.exit(1);
                }
                
                int rowsExported = exporter.exportToCsv(outputPath, sensorType, roomFilter, limit);
                exporter.close();
                
                if (rowsExported > 0) {
                    logger.info("Export successful: {} rows written to {}", rowsExported, outputPath);
                    System.exit(0);
                } else {
                    logger.error("Export failed: No data was exported");
                    System.exit(1);
                }
            } else {
                // Show help if no valid option provided
                formatter.printHelp("HBaseToMahoutExporter", options);
                System.exit(1);
            }
            
        } catch (ParseException e) {
            logger.error("Error parsing command line options", e);
            formatter.printHelp("HBaseToMahoutExporter", options);
            System.exit(1);
        } catch (NumberFormatException e) {
            logger.error("Invalid number format for limit option", e);
            System.exit(1);
        }
    }
}
