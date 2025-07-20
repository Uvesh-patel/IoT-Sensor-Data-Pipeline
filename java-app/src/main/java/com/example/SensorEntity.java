package com.example;

/**
 * POJO representing a sensor entity from Orion-LD.
 * Contains extracted data from the NGSI-LD JSON entities.
 */
public class SensorEntity {
    private String id;
    private String type;
    private String room;
    private String timestamp;
    private String sensorName;
    private double value;
    
    /**
     * Default constructor
     */
    public SensorEntity() {
    }
    
    /**
     * Full constructor
     */
    public SensorEntity(String id, String type, String room, String timestamp, String sensorName, double value) {
        this.id = id;
        this.type = type;
        this.room = room;
        this.timestamp = timestamp;
        this.sensorName = sensorName;
        this.value = value;
    }
    
    // Getters and setters
    public String getId() {
        return id;
    }
    
    public void setId(String id) {
        this.id = id;
    }
    
    public String getType() {
        return type;
    }
    
    public void setType(String type) {
        this.type = type;
    }
    
    public String getRoom() {
        return room;
    }
    
    public void setRoom(String room) {
        this.room = room;
    }
    
    public String getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }
    
    public String getSensorName() {
        return sensorName;
    }
    
    public void setSensorName(String sensorName) {
        this.sensorName = sensorName;
    }
    
    public double getValue() {
        return value;
    }
    
    public void setValue(double value) {
        this.value = value;
    }
    
    @Override
    public String toString() {
        return "SensorEntity{" +
                "id='" + id + '\'' +
                ", type='" + type + '\'' +
                ", room='" + room + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", sensorName='" + sensorName + '\'' +
                ", value=" + value +
                '}';
    }
}
