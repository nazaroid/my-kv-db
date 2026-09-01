#!/bin/bash

echo "=== KV Database Statistics ==="

# Configuration
KVDB_API="http://localhost:9000"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_header() {
    echo -e "${BLUE}=== $1 ===${NC}"
}

# Check if KV Database is running
check_kvdb() {
    log_info "Checking KV Database health..."
    if curl -s "$KVDB_API/health" > /dev/null; then
        log_info "KV Database is running!"
        return 0
    else
        log_error "KV Database is not responding at $KVDB_API"
        log_error "Please start the services first: make run"
        exit 1
    fi
}

# Show catalog statistics
show_catalog_stats() {
    log_header "Catalog Statistics"
    curl -s "$KVDB_API/stats/catalog" | jq '.' 2>/dev/null || curl -s "$KVDB_API/stats/catalog"
}

# Show database statistics
show_database_stats() {
    local db_name=$1
    log_header "Database: $db_name"
    curl -s "$KVDB_API/stats/database/$db_name" | jq '.' 2>/dev/null || curl -s "$KVDB_API/stats/database/$db_name"
}

# Show table statistics
show_table_stats() {
    local db_name=$1
    local table_name=$2
    log_header "Table: $table_name (in $db_name)"
    curl -s "$KVDB_API/stats/table/$db_name/$table_name" | jq '.' 2>/dev/null || curl -s "$KVDB_API/stats/table/$db_name/$table_name"
}

# Show Prometheus metrics
show_prometheus_metrics() {
    log_header "Prometheus Metrics"
    curl -s "http://localhost:9091"
}

# Show system health
show_system_health() {
    log_header "System Health"
    
    echo "KV Database:"
    curl -s "$KVDB_API/health" || echo "  Failed to connect"
    echo ""
    
    echo "Prometheus:"
    curl -s "http://localhost:9090/-/healthy" || echo "  Failed to connect"
    echo ""
    
    echo "Grafana:"
    curl -s "http://localhost:3000/api/health" || echo "  Failed to connect"
}

# Show usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -c, --catalog       Show catalog statistics"
    echo "  -d, --database DB   Show statistics for specific database"
    echo "  -t, --table DB TB   Show statistics for specific table"
    echo "  -m, --metrics       Show Prometheus metrics"
    echo "  -h, --health        Show system health"
    echo "  -a, --all           Show all statistics (default)"
    echo "  --help              Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                  # Show all statistics"
    echo "  $0 -c              # Show catalog statistics"
    echo "  $0 -d testdb       # Show statistics for 'testdb' database"
    echo "  $0 -t testdb users # Show statistics for 'users' table in 'testdb'"
    echo "  $0 -m              # Show Prometheus metrics"
    echo "  $0 -h              # Show system health"
}

# Parse command line arguments
SHOW_CATALOG=false
SHOW_DATABASE=""
SHOW_TABLE=""
SHOW_METRICS=false
SHOW_HEALTH=false
SHOW_ALL=true

while [[ $# -gt 0 ]]; do
    case $1 in
        -c|--catalog)
            SHOW_CATALOG=true
            SHOW_ALL=false
            shift
            ;;
        -d|--database)
            SHOW_DATABASE="$2"
            SHOW_ALL=false
            shift 2
            ;;
        -t|--table)
            SHOW_DATABASE="$2"
            SHOW_TABLE="$3"
            SHOW_ALL=false
            shift 3
            ;;
        -m|--metrics)
            SHOW_METRICS=true
            SHOW_ALL=false
            shift
            ;;
        -h|--health)
            SHOW_HEALTH=true
            SHOW_ALL=false
            shift
            ;;
        -a|--all)
            SHOW_ALL=true
            shift
            ;;
        --help)
            usage
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Main execution
main() {
    check_kvdb
    
    if [ "$SHOW_HEALTH" = true ]; then
        show_system_health
        exit 0
    fi
    
    if [ "$SHOW_METRICS" = true ]; then
        show_prometheus_metrics
        exit 0
    fi
    
    if [ "$SHOW_CATALOG" = true ]; then
        show_catalog_stats
        exit 0
    fi
    
    if [ -n "$SHOW_DATABASE" ]; then
        if [ -n "$SHOW_TABLE" ]; then
            show_table_stats "$SHOW_DATABASE" "$SHOW_TABLE"
        else
            show_database_stats "$SHOW_DATABASE"
        fi
        exit 0
    fi
    
    if [ "$SHOW_ALL" = true ]; then
        show_system_health
        echo ""
        show_catalog_stats
        echo ""
        show_prometheus_metrics
    fi
}

# Run main function
main
