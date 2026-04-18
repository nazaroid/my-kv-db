# Build stage
FROM amazoncorretto:17-al2023 AS builder

RUN yum update -y && \
    yum install -y gzip tar ca-certificates && \
    yum clean all

RUN ARCH=$(uname -m) && \
    case "$ARCH" in \
      x86_64)  ASSET="cs-x86_64-pc-linux" ;; \
      aarch64) ASSET="cs-aarch64-pc-linux" ;; \
      *) echo "Unsupported arch: $ARCH"; exit 1 ;; \
    esac && \
    echo "github.com/coursier/launchers/raw/refs/heads/master/${ASSET}.gz" && \
    curl -fL "github.com/coursier/launchers/raw/refs/heads/master/${ASSET}.gz" | gzip -d > cs && \
    chmod +x cs && \
    mv cs /usr/local/bin/cs

WORKDIR /app
COPY . .

RUN ls .

RUN cs launch sbt:1.9.6 -- "service/assembly"

FROM amazoncorretto:17-al2023

RUN yum install -y shadow-utils curl-minimal && \
    yum clean all

RUN groupadd -r kvdb && useradd -r -g kvdb kvdb

WORKDIR /app

COPY --from=builder /app/codebase/service/target/scala-3.4.2/service-assembly-25.2.1.0.jar app.jar

RUN mkdir -p /catalog && \
    chown -R kvdb:kvdb /app /catalog

USER kvdb

EXPOSE 9000 9091

HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
  CMD curl -f http://localhost:9000/health || exit 1

ENTRYPOINT ["java", "-jar", "app.jar"]
