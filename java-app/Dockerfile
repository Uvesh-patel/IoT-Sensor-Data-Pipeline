FROM maven:3.9-eclipse-temurin-17 AS build

# Set working directory
WORKDIR /app

# Copy pom.xml and download dependencies
COPY pom.xml .
RUN mvn dependency:go-offline

# Copy source code
COPY src ./src

# Build application
RUN mvn clean package -DskipTests

# Create runtime image - Using JDK instead of JRE to include jar command
FROM eclipse-temurin:17-jammy

# Install necessary tools
RUN apt-get update && \
    apt-get install -y curl iputils-ping netcat && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy JAR from build stage
COPY --from=build /app/target/ngsi-ld-to-hbase-1.0-SNAPSHOT-jar-with-dependencies.jar app.jar
COPY --from=build /app/target/classes /app/classes

# Environment variables for configuration
ENV ORION_HOST=orion-ld
ENV ORION_PORT=1026
ENV HBASE_ZOOKEEPER_QUORUM=hbase
ENV HBASE_ZOOKEEPER_PORT=2181
ENV HBASE_TABLE=SensorData
ENV CREATE_SENSOR_TABLE=false

# Copy entry script
COPY docker-entrypoint.sh /app/
RUN chmod +x /app/docker-entrypoint.sh

# Run the entry script
ENTRYPOINT ["/app/docker-entrypoint.sh"]
