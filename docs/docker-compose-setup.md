# KV Database Docker Compose Setup

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

### 1. Build and Start Services

```bash
# Build the application
sbt assembly

# Start all services
docker-compose up -d
```

### 2. Verify Services

```bash
# Check KV Database health
curl http://localhost:9000/health

# Check metrics endpoint (Prometheus HTTPServer)
curl http://localhost:9091

# Check Prometheus targets
curl http://localhost:9090/api/v1/targets
```

### 3. Access Web UIs

- **KV Database API**: http://localhost:9000
- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000 (admin/admin123)

## API Usage Examples

### Create Database and Table
```bash
# Create database
curl -X POST http://localhost:9000/data/mydb

# Create table
curl -X POST http://localhost:9000/data/mydb/mytable
```

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
# Get catalog statistics
curl http://localhost:9000/stats/catalog

# Get database statistics
curl http://localhost:9000/stats/database/mydb

# Get table statistics
curl http://localhost:9000/stats/table/mydb/mytable
```

## Available Metrics

The service exposes the following Prometheus metrics:

### Catalog Level
- `bitcask_databases_total` - Total number of databases
- `bitcask_tables_total` - Total number of tables
- `bitcask_entries_total` - Total number of entries
- `bitcask_entries_active_total` - Active entries count
- `bitcask_entries_deleted_total` - Deleted entries count
- `bitcask_data_size_bytes` - Total data size in bytes
- `bitcask_segments_total` - Total segments count
- `bitcask_segments_active_total` - Active segments count

### Database Level
- `bitcask_database_entries_total{database="name"}` - Entries per database
- `bitcask_database_tables_total{database="name"}` - Tables per database
- `bitcask_database_data_size_bytes{database="name"}` - Data size per database

### Table Level
- `bitcask_table_entries_total{database="name",table="name"}` - Entries per table
- `bitcask_table_entries_active_total{database="name",table="name"}` - Active entries per table
- `bitcask_table_entries_deleted_total{database="name",table="name"}` - Deleted entries per table
- `bitcask_table_data_size_bytes{database="name",table="name"}` - Data size per table
- `bitcask_table_segments_total{database="name",table="name"}` - Segments per table

### Segment Level
- `bitcask_segment_size_bytes{database="name",table="name",segment="name"}` - Segment size
- `bitcask_segment_entries_total{database="name",table="name",segment="name"}` - Entries per segment
- `bitcask_segment_stale_data_ratio{database="name",table="name",segment="name"}` - Stale data ratio

## Grafana Dashboard

The pre-configured dashboard includes:

1. **Overview Panel**: Total databases, tables, entries, and data size
2. **Entry Status Pie Chart**: Active vs deleted entries
3. **Segments Overview**: Active vs inactive segments
4. **Database Performance Table**: Per-database statistics
5. **Time Series Graphs**: Historical data trends

## Configuration

### Environment Variables
- `config` - Configuration file name (default: `docker.conf`)

### Ports
- `9000` - KV Database HTTP API
- `9091` - KV Database Metrics
- `9090` - Prometheus
- `3000` - Grafana

### Volumes
- `kvdb_data` - Database storage
- `prometheus_data` - Prometheus storage
- `grafana_data` - Grafana storage

## Development

### Local Development

```bash
# Run only KV Database with local config
sbt "project service" run

# Run with custom config
CONFIG=docker.conf sbt "project service" run
```

### Monitoring Development

```bash
# Check Prometheus configuration
docker-compose exec prometheus promtool check config /etc/prometheus/prometheus.yml

# Reload Prometheus configuration
curl -X POST http://localhost:9090/-/reload
```

## Troubleshooting

### Common Issues

1. **Port conflicts**: Ensure ports 9000, 9090, 9091, 3000 are available
2. **Permission issues**: Check Docker volumes permissions
3. **Network issues**: Verify Docker network connectivity

### Logs

```bash
# View all logs
docker-compose logs

# View specific service logs
docker-compose logs kvdb
docker-compose logs prometheus
docker-compose logs grafana

# Follow logs
docker-compose logs -f kvdb
```

### Health Checks

```bash
# Check service health
curl http://localhost:9000/health

# Check Prometheus targets
curl http://localhost:9090/api/v1/targets

# Check Grafana API
curl http://localhost:3000/api/health
```

## Cleanup

```bash
# Stop and remove all services
docker-compose down

# Remove volumes (WARNING: This deletes all data)
docker-compose down -v

# Remove images
docker-compose down --rmi all
```

## Production Considerations

1. **Security**: Change default passwords and use HTTPS
2. **Persistence**: Configure backup strategies for data volumes
3. **Scaling**: Consider load balancing for high availability
4. **Monitoring**: Set up alerts in Grafana/Prometheus
5. **Resource limits**: Configure memory and CPU limits in Docker Compose
