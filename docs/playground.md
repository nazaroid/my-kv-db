# KV Database Docker Compose Setup

[Return to Index](./index.md)

This setup provides a complete monitoring stack for the KV Database service with Prometheus and Grafana.

## Architecture

```
KV Database Service (Port 9000) + Metrics (Port 9091)
    |
    v
Prometheus (Port 9090) - Collects metrics
    |
    v
Grafana (Port 3000) - Visualizes metrics
```

## Services

### 1. KV Database Service
- **HTTP API**: `http://localhost:9000`
- **Health Check**: `http://localhost:9000/health`
- **Metrics**: `http://localhost:9091` (Prometheus HTTPServer)
- **Data API**: `http://localhost:9000/data/{db}/{table}/{key}`

### 2. Prometheus
- **Web UI**: `http://localhost:9090`
- **Metrics**: `http://localhost:9090/metrics`
- **Config**: `./monitoring/prometheus/prometheus.yml`

### 3. Grafana
- **Web UI**: `http://localhost:3000`
- **Login**: `admin` / `admin123`
- **Dashboard**: Pre-configured KV Database Dashboard

## Quick Start

### 1. Run Playground and show Environment Info

```bash
make demo
```

### 2. Show Playground Capabilities

```bash
make help
```

### 3. Access Web UIs

- **KV Database API**: http://localhost:9000
- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000 (admin/admin123)

## API Usage Examples

### Store and Retrieve Data
```bash
# Store value
curl -X POST http://localhost:9000/data/mydb/mytable/key1 \
  -H "Content-Type: text/plain" \
  -d "Hello World"

# Retrieve value
curl http://localhost:9000/data/mydb/mytable/key1
```

### Delete Data
```bash
# Delete value
curl -X DELETE http://localhost:9000/data/mydb/mytable/key1
```

### Statistics
```bash
# Show stat
curl -X DELETE http://localhost:9000/stats/catalog
```
