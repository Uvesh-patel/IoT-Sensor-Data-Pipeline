package com.example;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Class responsible for inserting sensor entities into HBase.
 */
public class HBaseInserter {
    private static final Logger LOGGER = Logger.getLogger(HBaseInserter.class.getName());
    
    private static final String TABLE_NAME = "SensorData";
    
    /**
     * Insert a SensorEntity into HBase.
     * 
     * Row key: entity ID
     * Column family: room
     * Column qualifier: sensor name (e.g., temperature)
     * Value: value|timestamp
     * 
     * @param entity The SensorEntity to insert
     * @param hbaseConn The HBase connection
     * @throws IOException If an error occurs during insertion
     */
    public void insert(SensorEntity entity, Connection hbaseConn) throws IOException {
        if (entity == null) {
            LOGGER.warning("Cannot insert null entity");
            return;
        }
        
        if (hbaseConn == null || hbaseConn.isClosed()) {
            LOGGER.severe("HBase connection is null or closed");
            throw new IOException("Invalid HBase connection");
        }
        
        // Validate required entity fields
        if (entity.getId() == null || entity.getRoom() == null || 
            entity.getSensorName() == null || entity.getTimestamp() == null) {
            LOGGER.warning("Entity has null required fields: " + entity);
            return;
        }
        
        LOGGER.info("Inserting entity into HBase: " + entity);
        
        try (Table table = hbaseConn.getTable(TableName.valueOf(TABLE_NAME))) {
            // Create row key from entity ID
            byte[] rowKey = Bytes.toBytes(entity.getId());
            
            // Create column family from room
            byte[] columnFamily = Bytes.toBytes(entity.getRoom());
            
            // Create column qualifier from sensor name
            byte[] columnQualifier = Bytes.toBytes(entity.getSensorName());
            
            // Create value as "value|timestamp" format
            String valueWithTimestamp = entity.getValue() + "|" + entity.getTimestamp();
            byte[] value = Bytes.toBytes(valueWithTimestamp);
            
            // Create and execute put operation
            Put put = new Put(rowKey);
            put.addColumn(columnFamily, columnQualifier, value);
            table.put(put);
            
            LOGGER.info("Successfully inserted entity: " + entity.getId() + 
                       ", room: " + entity.getRoom() + 
                       ", sensor: " + entity.getSensorName() + 
                       ", value: " + valueWithTimestamp);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Error inserting entity into HBase: " + entity.getId(), e);
            throw e;
        }
    }
    
    /**
     * Batch insert multiple SensorEntities into HBase.
     * 
     * @param entities List of SensorEntity objects to insert
     * @param hbaseConn The HBase connection
     * @return Number of successfully inserted entities
     */
    public int batchInsert(Iterable<SensorEntity> entities, Connection hbaseConn) {
        int successCount = 0;
        int failureCount = 0;
        
        for (SensorEntity entity : entities) {
            try {
                insert(entity, hbaseConn);
                successCount++;
            } catch (Exception e) {
                failureCount++;
                LOGGER.log(Level.WARNING, "Failed to insert entity: " + 
                          (entity != null ? entity.getId() : "null"), e);
            }
        }
        
        LOGGER.info("Batch insert completed. Success: " + successCount + ", Failures: " + failureCount);
        return successCount;
    }
}
