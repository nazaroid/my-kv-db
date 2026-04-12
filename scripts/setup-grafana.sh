#!/bin/bash

echo "Setting up Grafana dashboards..."

# Wait for Grafana to start
echo "Waiting for Grafana to be ready..."
until curl -s http://localhost:3000/api/health > /dev/null; do
    echo "Grafana not ready yet, waiting 5 seconds..."
    sleep 5
done

echo "Grafana is ready! Setting up datasources and dashboards..."

# Check if Prometheus datasource exists
DATASOURCE_EXISTS=$(curl -s -u admin:admin123 \
    http://localhost:3000/api/datasources/name/prometheus \
    | jq -r '.message // "exists"')

if [ "$DATASOURCE_EXISTS" = "exists" ]; then
    echo "Prometheus datasource already exists"
else
    echo "Creating Prometheus datasource..."
    curl -s -u admin:admin123 -X POST \
        http://localhost:3000/api/datasources \
        -H "Content-Type: application/json" \
        -d '{
            "name": "Prometheus",
            "type": "prometheus",
            "url": "http://prometheus:9090",
            "access": "proxy",
            "isDefault": true
        }'
fi

# Import dashboard
echo "Importing KV Database dashboard..."
curl -s -u admin:admin123 -X POST \
    http://localhost:3000/api/dashboards/db \
    -H "Content-Type: application/json" \
    -d @monitoring/grafana/dashboards/kvdb-dashboard.json

echo "Dashboard setup completed!"
echo "Access Grafana at: http://localhost:3000"
echo "Login: admin / admin123"
