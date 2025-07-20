package com.example;

import org.json.JSONObject;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Utility class for parsing NGSI-LD entities into SensorEntity POJOs.
 * Processes NGSI-LD entities with attributes that match sensorName.
 */
public class SensorEntityParser {
    private static final Logger LOGGER = Logger.getLogger(SensorEntityParser.class.getName());
    
    // Regular expression to extract room from entity ID
    private static final Pattern ROOM_PATTERN = Pattern.compile("urn:ngsi-ld:.*:([^:]+):");
    
    /**
     * Parse a JSON entity into a SensorEntity POJO.
     * 
     * @param entity The JSON entity from Orion-LD
     * @return A SensorEntity POJO
     */
    public static SensorEntity parse(JSONObject entity) {
        try {
            SensorEntity sensorEntity = new SensorEntity();
            
            // Extract basic entity metadata
            sensorEntity.setId(entity.getString("id"));
            sensorEntity.setType(entity.getString("type"));
            
            // Extract room from ID pattern or fallback
            sensorEntity.setRoom(extractRoomFromId(sensorEntity.getId()));
            
            // Extract timestamp from dateObserved or timestamp property
            boolean foundTimestamp = false;
            
            // Try timestamp property first
            if (entity.has("timestamp")) {
                JSONObject timestampProp = entity.getJSONObject("timestamp");
                if (timestampProp.has("value")) {
                    sensorEntity.setTimestamp(timestampProp.getString("value"));
                    foundTimestamp = true;
                    LOGGER.info("Found timestamp from 'timestamp' property: " + sensorEntity.getTimestamp());
                }
            }
            
            // Fallback to dateObserved if timestamp not found
            if (!foundTimestamp && entity.has("dateObserved")) {
                JSONObject dateObservedProp = entity.getJSONObject("dateObserved");
                if (dateObservedProp.has("value")) {
                    sensorEntity.setTimestamp(dateObservedProp.getString("value"));
                    foundTimestamp = true;
                    LOGGER.info("Found timestamp from 'dateObserved' property: " + sensorEntity.getTimestamp());
                }
            }
            
            // If still no timestamp found, use current time
            if (!foundTimestamp) {
                String currentTimestamp = ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                LOGGER.warning("⚠️ No timestamp found for entity " + sensorEntity.getId() + ", using current time: " + currentTimestamp);
                sensorEntity.setTimestamp(currentTimestamp);
            }
            
            // Try to extract room directly if available
            if (entity.has("room")) {
                JSONObject roomProp = entity.getJSONObject("room");
                if (roomProp.has("value")) {
                    String roomValue = roomProp.getString("value").toLowerCase();
                    sensorEntity.setRoom(roomValue);
                    LOGGER.info("Found room directly from entity: " + roomValue);
                }
            } else {
                // Otherwise extract room from ID pattern or fallback (already set above)
                LOGGER.info("Using room from ID pattern: " + sensorEntity.getRoom());
            }
            
            // Extract sensor name from entity type
            String sensorName = extractSensorNameFromType(sensorEntity.getType());
            sensorEntity.setSensorName(sensorName);
            LOGGER.info("Extracted sensor name: " + sensorName);
            
            // Direct attribute lookup using sensorName
            if (entity.has(sensorName)) {
                JSONObject valueProp = entity.getJSONObject(sensorName);
                if (valueProp.has("value") && valueProp.get("value") instanceof Number) {
                    sensorEntity.setValue(valueProp.getDouble("value"));
                    LOGGER.info("Found value for " + sensorName + ": " + sensorEntity.getValue());
                } else {
                    LOGGER.warning("⚠️ Property '" + sensorName + "' exists but has no numeric value");
                }
            } else {
                LOGGER.warning("⚠️ Missing value property for sensorName: " + sensorName);
                
                // Fallback: Try to find any property with a value
                boolean valueFound = false;
                for (String key : entity.keySet()) {
                    if (!key.equals("id") && !key.equals("type") && !key.equals("dateObserved") && !key.equals("timestamp") && !key.equals("room")) {
                        Object obj = entity.get(key);
                        if (obj instanceof JSONObject) {
                            JSONObject prop = (JSONObject) obj;
                            if (prop.has("value") && prop.get("value") instanceof Number) {
                                sensorEntity.setValue(prop.getDouble("value"));
                                LOGGER.info("Fallback: Found value from '" + key + "' property: " + sensorEntity.getValue());
                                valueFound = true;
                                break;
                            }
                        }
                    }
                }
                
                if (!valueFound) {
                    LOGGER.severe("❌ No suitable value found for entity " + sensorEntity.getId() + " with type " + sensorEntity.getType());
                }
            }
            
            // Log successful parsing
            LOGGER.info("Successfully parsed entity: " + sensorEntity);
            return sensorEntity;
            
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error parsing entity: " + entity, e);
            return null;
        }
    }
    
    /**
     * Extract room information from entity ID.
     * 
     * @param id The entity ID
     * @return The extracted room name or a default value
     */
    private static String extractRoomFromId(String id) {
        try {
            Matcher matcher = ROOM_PATTERN.matcher(id);
            if (matcher.find()) {
                String room = matcher.group(1).toLowerCase();
                
                // Map room names to expected column family names
                switch (room) {
                    case "bathroom":
                    case "kitchen":
                    case "room1":
                    case "room2":
                    case "room3":
                    case "toilet":
                        return room;
                    default:
                        // If room doesn't match expected column families, default to room1
                        LOGGER.warning("Unknown room found in ID: " + room + ", defaulting to room1");
                        return "room1";
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to extract room from ID: " + id, e);
        }
        
        // Default to room1 if extraction fails
        return "room1";
    }
    
    /**
     * Extract sensor name from entity type.
     * 
     * @param type The entity type
     * @return The extracted sensor name
     */
    private static String extractSensorNameFromType(String type) {
        // Handle different sensor types
        switch (type) {
            case "BrightnessSensor":
                return "brightness";
            case "HumiditySensor":
                return "humidity";
            case "TemperatureSensor":
                return "temperature";
            case "ThermostatTemperatureSensor":
                return "thermostatTemperature";
            case "SetpointHistorySensor":
                return "setpointHistory";
            case "VirtualOutdoorTemperatureSensor":
                return "virtualOutdoorTemperature";
            case "OutdoorTemperatureSensor":
                return "outdoorTemperature";
            default:
                // For unknown types, use lowercase of type name
                return type.toLowerCase();
        }
    }
}
