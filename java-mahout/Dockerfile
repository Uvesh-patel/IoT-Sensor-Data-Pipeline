FROM maven:3.8-openjdk-11 as builder

# Set working directory
WORKDIR /app

# Copy pom.xml and download dependencies
COPY pom.xml .
RUN mvn dependency:go-offline -B

# Copy source code and resources
COPY src ./src

# Create resources directory if it doesn't exist
RUN mkdir -p src/main/resources

# Build the application with debugging enabled
RUN mvn package -DskipTests -X

# Runtime stage
FROM openjdk:11-jre-slim

# Install wget for downloading Mahout (if needed later)
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy the JAR file
COPY --from=builder /app/target/hbase-to-mahout-1.0-SNAPSHOT-jar-with-dependencies.jar app.jar

# Create output directory for Mahout data
RUN mkdir -p /tmp/mahout/input /tmp/mahout/output

# Script to run the application
COPY docker-entrypoint.sh /
RUN chmod +x /docker-entrypoint.sh

ENTRYPOINT ["/docker-entrypoint.sh"]
