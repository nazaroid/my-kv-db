.PHONY: help build run stop clean logs status test grafana-setup populate-db show-stats
DOCKER_COMPOSE := docker-compose -f docker-compose.yml --project-directory ..

# Default target
help:
	@echo "KV Database Docker Compose Commands:"
	@echo ""
	@echo "  run       - Build and start all services"
	@echo "  stop      - Stop all services"
	@echo "  restart   - Restart all services"
	@echo "  clean     - Stop and remove all services and volumes"
	@echo "  logs      - Show logs from all services"
	@echo "  status    - Show status of all services"
	@echo "  test      - Run API tests"
	@echo "  health    - Check health of all services"
	@echo "  metrics   - Show available metrics"
	@echo "  grafana-setup - Setup Grafana dashboards (template variables)"
	@echo "  demo - Show playground capabilities
	@echo "  populate-db - Populate database with test data (100 users)"
	@echo "  populate-db-custom - Populate with custom number of records"
	@echo "  cleanup-test-data - Clean up test data"
	@echo "  show-stats - Show database statistics (formatted JSON)"
	@echo ""


# Build and start all services
run: build
	@echo "Starting all services..."
	$(DOCKER_COMPOSE) up -d
	@echo "Services started!"
	@echo "KV Database API: http://localhost:9000"
	@echo "Prometheus: http://localhost:9090"
	@echo "Grafana: http://localhost:3000 (admin/admin123)"

# Stop all services
stop:
	@echo "Stopping all services..."
	docker-compose down
	@echo "Services stopped!"

# Restart all services
restart: stop run

# Stop and remove all services and volumes
clean:
	@echo "Cleaning up all services and volumes..."
	docker-compose down -v
	docker system prune -f
	@echo "Cleanup completed!"

# Show logs from all services
logs:
	$(DOCKER_COMPOSE) logs -f

# Show logs from specific service
logs-kvdb:
	$(DOCKER_COMPOSE) logs -f kvdb

logs-prometheus:
	$(DOCKER_COMPOSE) logs -f prometheus

logs-grafana:
	$(DOCKER_COMPOSE) logs -f grafana

# Show status of all services
status:
	@echo "=== Service Status ==="
	$(DOCKER_COMPOSE) ps
	@echo ""
	@echo "=== Health Checks ==="
	@curl -s http://localhost:9000/health || echo "KV Database: DOWN"
	@curl -s http://localhost:9090/api/v1/targets | jq -r '.data.activeTargets[] | "\(.labels.job): \(.health)"' || echo "Prometheus: DOWN"
	@curl -s http://localhost:3000/api/health || echo "Grafana: DOWN"

# Check health of all services
health:
	@echo "=== KV Database Health ==="
	@curl -s http://localhost:9000/health || echo "KV Database: DOWN"
	@echo ""
	@echo "=== Prometheus Health ==="
	@curl -s http://localhost:9090/-/healthy | jq -r '.status' || echo "Prometheus: DOWN"
	@echo ""
	@echo "=== Grafana Health ==="
	@curl -s http://localhost:3000/api/health | jq -r '.database' || echo "Grafana: DOWN"

# Show available metrics
metrics:
	@echo "=== KV Database Metrics ==="
	@curl -s http://localhost:9091 | grep "bitcask_" | head -20
	@echo ""
	@echo "=== Prometheus Metrics ==="
	@curl -s http://localhost:9090/metrics | grep "prometheus_" | head -10

# Setup Grafana dashboards (template variables)
grafana-setup:
	@echo "Setting up Grafana dashboards with template variables..."
	./scripts/setup-grafana.sh

# Populate database with test data
populate-db:
	@echo "Populating database with test data..."
	./scripts/populate-db.sh

# Populate with custom number of records
populate-db-custom:
	@read -p "Enter number of records to create: " num; \
	./scripts/populate-db.sh -n $$num -v

# Clean up test data
cleanup-test-data:
	@echo "Cleaning up test data..."
	./scripts/populate-db.sh -c

# Show database statistics
show-stats:
	@echo "Showing database statistics..."
	./scripts/show-stats.sh

# Run API tests
test:
	@echo "=== Testing KV Database API ==="
	@echo "Creating database..."
	@curl -X POST http://localhost:9000/data/testdb
	@echo ""
	@echo "Creating table..."
	@curl -X POST http://localhost:9000/data/testdb/testtable
	@echo ""
	@echo "Storing value..."
	@curl -X POST http://localhost:9000/data/testdb/testtable/testkey \
		-H "Content-Type: text/plain" \
		-d "Hello Docker!"
	@echo ""
	@echo "Retrieving value..."
	@curl http://localhost:9000/data/testdb/testtable/testkey
	@echo ""
	@echo "Getting statistics..."
	@curl http://localhost:9000/stats/catalog
	@echo ""
	@echo "API tests completed!"

# Quick demo
demo: run
	@echo "Waiting for services to start..."
	@sleep 10
	@echo "Running demo..."
	@make test
	@echo ""
	@echo "Demo completed! Visit:"
	@echo "  - KV Database: http://localhost:9000"
	@echo "  - Prometheus: http://localhost:9090"
	@echo "  - Grafana: http://localhost:3000 (admin/admin123)"

# Monitoring commands
prometheus-reload:
	curl -X POST http://localhost:9090/-/reload

prometheus-config-check:
	docker-compose exec prometheus promtool check config /etc/prometheus/prometheus.yml

# Backup data volumes
backup:
	@echo "Backing up data volumes..."
	docker run --rm -v kvdb_kvdb_data:/data -v $(PWD):/backup alpine tar czf /backup/kvdb-data-$(shell date +%Y%m%d-%H%M%S).tar.gz -C /data .
	@echo "Backup completed!"

# Restore data volumes
restore:
	@echo "Available backups:"
	@ls -la kvdb-data-*.tar.gz 2>/dev/null || echo "No backups found"
	@read -p "Enter backup filename: " backup_file; \
	if [ -f "$$backup_file" ]; then \
		docker run --rm -v kvdb_kvdb_data:/data -v $(PWD):/backup alpine tar xzf /backup/"$$backup_file" -C /data; \
		echo "Restore completed!"; \
	else \
		echo "Backup file not found!"; \
	fi
