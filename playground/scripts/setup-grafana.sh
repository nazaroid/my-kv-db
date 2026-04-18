#!/bin/bash

echo "=== Template Grafana Dashboard Setup ==="

# Configuration
GRAFANA_URL="http://localhost:3000"
GRAFANA_USER="admin"
GRAFANA_PASS="admin123"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Wait for Grafana to start
log_info "Waiting for Grafana to be ready..."
until curl -s "$GRAFANA_URL/api/health" > /dev/null; do
    log_warn "Grafana not ready yet, waiting 5 seconds..."
    sleep 5
done

log_info "Grafana is ready!"

# Check if Prometheus datasource exists, create if not
log_info "Checking Prometheus datasource..."
datasource_check=$(curl -s -u "$GRAFANA_USER:$GRAFANA_PASS" \
    "$GRAFANA_URL/api/datasources/name/Prometheus" | jq -r '.message // "exists"')

if [ "$datasource_check" != "exists" ]; then
    log_warn "Prometheus datasource not found, creating..."
    response=$(curl -s -u "$GRAFANA_USER:$GRAFANA_PASS" -X POST \
        "$GRAFANA_URL/api/datasources" \
        -H "Content-Type: application/json" \
        -d '{
            "name": "Prometheus",
            "type": "prometheus",
            "url": "http://prometheus:9090",
            "access": "proxy",
            "isDefault": true
        }')
    
    if echo "$response" | jq -e '.datasource' > /dev/null 2>&1; then
        log_info "Prometheus datasource created successfully!"
    else
        log_error "Failed to create Prometheus datasource"
        log_error "Response: $response"
        exit 1
    fi
else
    log_info "Prometheus datasource already exists!"
fi

# Import template dashboard
log_info "Importing template dashboard..."
response=$(curl -s -u "$GRAFANA_USER:$GRAFANA_PASS" -X POST \
    "$GRAFANA_URL/api/dashboards/db" \
    -H "Content-Type: application/json" \
    -d @monitoring/grafana/dashboards/kvdb-dashboard.json)

if echo "$response" | jq -e '.status == "success"' > /dev/null 2>&1; then
    dashboard_url=$(echo "$response" | jq -r '.url')
    dashboard_id=$(echo "$response" | jq -r '.id')
    log_info "Template dashboard imported successfully!"
    log_info "Dashboard ID: $dashboard_id"
    log_info "Access at: $GRAFANA_URL$dashboard_url"
    log_info ""
    log_info "The dashboard uses DS_PROMETHEUS variable that automatically"
    log_info "resolves to the Prometheus datasource, so it will work"
    log_info "regardless of the datasource UID!"
else
    log_error "Failed to import template dashboard"
    log_error "Response: $response"
    exit 1
fi

log_info "Template dashboard setup completed!"
log_info "Access Grafana at: $GRAFANA_URL"
log_info "Login: $GRAFANA_USER / $GRAFANA_PASS"
