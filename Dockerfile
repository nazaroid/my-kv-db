# Build stage
FROM openjdk:25-oraclelinux8 AS builder

# Install SBT
RUN yum install -y curl && \
    curl -fL https://github.com/coursier/launchers/raw/master/cs-x86_64-pc-linux.gz | gzip -d > cs && \
    chmod +x cs && \
    mv cs /usr/local/bin/cs

# Set working directory
WORKDIR /app

# Copy project files
COPY . .

# Build the application
RUN cs setup && \
    cs launch --jvm 25 sbt:1.9.6 assembly

# Runtime stage
FROM openjdk:25-oraclelinux8

# Install curl for health check
RUN yum install -y curl && \
    yum clean all

# Create app user
RUN groupadd -r kvdb && useradd -r -g kvdb kvdb

# Set working directory
WORKDIR /app

# Copy the built JAR from builder stage
COPY --from=builder /app/codebase/service/target/scala-2.13/service-assembly-*.jar app.jar

# Create data directory
RUN mkdir -p /catalog && \
    chown -R kvdb:kvdb /app /catalog

# Switch to non-root user
USER kvdb

# Expose ports
EXPOSE 9000 9091

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
  CMD curl -f http://localhost:9000/health || exit 1

# Run the application
ENTRYPOINT ["java", "-jar", "app.jar"]
