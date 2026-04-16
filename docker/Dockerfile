# ─────────────────────────────────────────────────────────────────────────────
# Stage 1: build
# Build context must be the repo root.
# Used automatically by docker/docker-compose.yml (switch dockerfile there).
#
# Manual build from source:
#   docker build -f docker/Dockerfile -t graph-query-engine:latest .
# ─────────────────────────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jdk-alpine AS build
WORKDIR /build

# Cache Maven dependencies first (layer invalidated only when pom.xml changes)
COPY pom.xml ./
RUN apk add --no-cache maven && mvn dependency:go-offline -q

# Copy source and build (skip tests — run them separately in CI)
COPY src ./src
RUN mvn package -DskipTests -q

# ─────────────────────────────────────────────────────────────────────────────
# Stage 2: runtime
# Lean JRE image.  Only the JAR, default H2 data, mappings and entrypoint go in.
# Can be built from either the build stage or a pre-built local JAR:
#   docker build --target runtime .                   (uses build stage)
#   docker build --target runtime \
#     --build-arg JAR_SRC=target/graph-query-engine-0.1.0.jar .  (local JAR)
# ─────────────────────────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jre-alpine AS runtime

LABEL org.opencontainers.image.title="graph-query-engine"
LABEL org.opencontainers.image.description="Gremlin-to-SQL graph query engine (H2 + Iceberg/Trino)"
LABEL org.opencontainers.image.version="0.1.0"

# ── Application directories ───────────────────────────────────────────────────
RUN mkdir -p /app/data /app/mappings /app/wcoj-index-cache /app/docker

# Install curl — used by entrypoint.sh health-wait loop
RUN apk add --no-cache curl

WORKDIR /app

# ── Fat JAR ───────────────────────────────────────────────────────────────────
COPY --from=build /build/target/graph-query-engine-0.1.0.jar app.jar

# ── Entrypoint ────────────────────────────────────────────────────────────────
COPY docker/entrypoint.sh /app/docker/entrypoint.sh
RUN chmod +x /app/docker/entrypoint.sh

# ── Runtime tunables (all overridable via docker-compose environment:) ────────
ENV DB_URL="jdbc:h2:file:/app/data/graph;AUTO_SERVER=TRUE" \
    DB_USER="sa" \
    DB_PASSWORD="" \
    DB_DRIVER="" \
    GRAPH_PROVIDER="sql" \
    WCOJ_ENABLED="true" \
    WCOJ_MAX_EDGES="5000000" \
    WCOJ_DISK_QUOTA_MB="2048" \
    WCOJ_INDEX_TTL_SECONDS="30" \
    SERVER_PORT="7000" \
    JAVA_OPTS="-Xmx2g -Xms512m"

EXPOSE 7000

# Named volumes declared here so compose can attach them
VOLUME ["/app/data", "/app/wcoj-index-cache"]

ENTRYPOINT ["/app/docker/entrypoint.sh"]

