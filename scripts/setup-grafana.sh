#!/bin/bash

echo "=== Dynamic Grafana Dashboard Setup ==="

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

# Get Prometheus datasource UID dynamically
log_info "Getting Prometheus datasource UID..."
DATASOURCE_UID=$(curl -s -u "$GRAFANA_USER:$GRAFANA_PASS" \
    "$GRAFANA_URL/api/datasources/name/Prometheus" | jq -r '.uid' 2>/dev/null)

if [ -z "$DATASOURCE_UID" ] || [ "$DATASOURCE_UID" = "null" ]; then
    log_error "Could not get Prometheus datasource UID"
    log_error "Creating Prometheus datasource..."
    
    # Create Prometheus datasource
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
    
    DATASOURCE_UID=$(echo "$response" | jq -r '.uid' 2>/dev/null)
    
    if [ -z "$DATASOURCE_UID" ] || [ "$DATASOURCE_UID" = "null" ]; then
        log_error "Failed to create Prometheus datasource"
        exit 1
    fi
fi

log_info "Prometheus datasource UID: $DATASOURCE_UID"

# Create dashboard with dynamic UID
log_info "Creating dashboard with dynamic datasource UID..."

# Create dashboard JSON with dynamic UID
cat > /tmp/kvdb-dynamic-dashboard.json << EOF
{
  "dashboard": {
    "id": null,
    "title": "KV Database Dashboard",
    "tags": ["kvdb", "bitcask", "database"],
    "timezone": "browser",
    "panels": [
      {
        "datasource": {
          "type": "prometheus",
          "uid": "$DATASOURCE_UID"
        },
        "fieldConfig": {
          "defaults": {
            "color": {
              "mode": "thresholds"
            },
            "mappings": [],
            "thresholds": {
              "steps": [
                {
                  "color": "green",
                  "value": null
                }
              ]
            },
            "unit": "short"
          },
          "overrides": []
        },
        "gridPos": {
          "h": 8,
          "w": 6,
          "x": 0,
          "y": 0
        },
        "id": 1,
        "options": {
          "colorMode": "value",
          "graphMode": "area",
          "justifyMode": "auto",
          "orientation": "auto",
          "reduceOptions": {
            "values": false,
            "calcs": [
              "lastNotNull"
            ],
            "fields": ""
          },
          "text": {},
          "textMode": "auto"
        },
        "targets": [
          {
            "datasource": {
              "type": "prometheus",
              "uid": "$DATASOURCE_UID"
            },
            "expr": "bitcask_databases_total",
            "refId": "A"
          }
        ],
        "title": "Total Databases",
        "type": "stat"
      },
      {
        "datasource": {
          "type": "prometheus",
          "uid": "$DATASOURCE_UID"
        },
        "fieldConfig": {
          "defaults": {
            "color": {
              "mode": "thresholds"
            },
            "mappings": [],
            "thresholds": {
              "steps": [
                {
                  "color": "green",
                  "value": null
                }
              ]
            },
            "unit": "short"
          },
          "overrides": []
        },
        "gridPos": {
          "h": 8,
          "w": 6,
          "x": 6,
          "y": 0
        },
        "id": 2,
        "options": {
          "colorMode": "value",
          "graphMode": "area",
          "justifyMode": "auto",
          "orientation": "auto",
          "reduceOptions": {
            "values": false,
            "calcs": [
              "lastNotNull"
            ],
            "fields": ""
          },
          "text": {},
          "textMode": "auto"
        },
        "targets": [
          {
            "datasource": {
              "type": "prometheus",
              "uid": "$DATASOURCE_UID"
            },
            "expr": "bitcask_tables_total",
            "refId": "A"
          }
        ],
        "title": "Total Tables",
        "type": "stat"
      },
      {
        "datasource": {
          "type": "prometheus",
          "uid": "$DATASOURCE_UID"
        },
        "fieldConfig": {
          "defaults": {
            "color": {
              "mode": "thresholds"
            },
            "mappings": [],
            "thresholds": {
              "steps": [
                {
                  "color": "green",
                  "value": null
                }
              ]
            },
            "unit": "short"
          },
          "overrides": []
        },
        "gridPos": {
          "h": 8,
          "w": 6,
          "x": 12,
          "y": 0
        },
        "id": 3,
        "options": {
          "colorMode": "value",
          "graphMode": "area",
          "justifyMode": "auto",
          "orientation": "auto",
          "reduceOptions": {
            "values": false,
            "calcs": [
              "lastNotNull"
            ],
            "fields": ""
          },
          "text": {},
          "textMode": "auto"
        },
        "targets": [
          {
            "datasource": {
              "type": "prometheus",
              "uid": "$DATASOURCE_UID"
            },
            "expr": "bitcask_entries_total",
            "refId": "A"
          }
        ],
        "title": "Total Entries",
        "type": "stat"
      },
      {
        "datasource": {
          "type": "prometheus",
          "uid": "$DATASOURCE_UID"
        },
        "fieldConfig": {
          "defaults": {
            "color": {
              "mode": "thresholds"
            },
            "mappings": [],
            "thresholds": {
              "steps": [
                {
                  "color": "green",
                  "value": null
                }
              ]
            },
            "unit": "bytes"
          },
          "overrides": []
        },
        "gridPos": {
          "h": 8,
          "w": 6,
          "x": 18,
          "y": 0
        },
        "id": 4,
        "options": {
          "colorMode": "value",
          "graphMode": "area",
          "justifyMode": "auto",
          "orientation": "auto",
          "reduceOptions": {
            "values": false,
            "calcs": [
              "lastNotNull"
            ],
            "fields": ""
          },
          "text": {},
          "textMode": "auto"
        },
        "targets": [
          {
            "datasource": {
              "type": "prometheus",
              "uid": "$DATASOURCE_UID"
            },
            "expr": "bitcask_data_size_bytes",
            "refId": "A"
          }
        ],
        "title": "Data Size (Bytes)",
        "type": "stat"
      }
    ],
    "refresh": "10s",
    "schemaVersion": 36,
    "style": "dark",
    "time": {
      "from": "now-1h",
      "to": "now"
    },
    "timepicker": {},
    "timezone": "",
    "templating": {
      "list": []
    }
  },
  "overwrite": true
}
EOF

# Import dashboard
log_info "Importing dashboard..."
response=$(curl -s -u "$GRAFANA_USER:$GRAFANA_PASS" -X POST \
    "$GRAFANA_URL/api/dashboards/db" \
    -H "Content-Type: application/json" \
    -d @/tmp/kvdb-dynamic-dashboard.json)

if echo "$response" | jq -e '.status == "success"' > /dev/null 2>&1; then
    dashboard_url=$(echo "$response" | jq -r '.url')
    log_info "Dashboard created successfully!"
    log_info "Access at: $GRAFANA_URL$dashboard_url"
else
    log_error "Failed to create dashboard"
    log_error "Response: $response"
    exit 1
fi

# Clean up temporary file
rm -f /tmp/kvdb-dynamic-dashboard.json

log_info "Dynamic dashboard setup completed!"
log_info "Access Grafana at: $GRAFANA_URL"
log_info "Login: $GRAFANA_USER / $GRAFANA_PASS"
