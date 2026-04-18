#!/bin/bash

echo "=== KV Database Test Data Population ==="

# Configuration
KVDB_API="http://localhost:9000"
TEST_DB="testdb"
TEST_TABLE="users"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
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

# Create test database
create_database() {
    log_info "Creating test database: $TEST_DB"
    response=$(curl -s -w "%{http_code}" -X POST "$KVDB_API/data/$TEST_DB")
    http_code="${response: -3}"
    
    if [ "$http_code" = "200" ]; then
        log_info "Database '$TEST_DB' created successfully!"
    else
        log_warn "Database might already exist or creation failed (HTTP: $http_code)"
    fi
}

# Create test table
create_table() {
    log_info "Creating test table: $TEST_TABLE"
    response=$(curl -s -w "%{http_code}" -X POST "$KVDB_API/data/$TEST_DB/$TEST_TABLE")
    http_code="${response: -3}"
    
    if [ "$http_code" = "200" ]; then
        log_info "Table '$TEST_TABLE' created successfully!"
    else
        log_warn "Table might already exist or creation failed (HTTP: $http_code)"
    fi
}

# Generate random user data
generate_user_data() {
    local user_id=$1
    local names=("Alice" "Bob" "Charlie" "Diana" "Eve" "Frank" "Grace" "Henry" "Iris" "Jack")
    local emails=("gmail.com" "yahoo.com" "outlook.com" "example.com" "test.com")
    local cities=("New York" "London" "Tokyo" "Paris" "Berlin" "Sydney" "Toronto" "Moscow" "Beijing" "Mumbai")
    
    local name="${names[$((user_id % 10))]}${user_id}"
    local email=$(echo "$name" | tr '[:upper:]' '[:lower:]')"@${emails[$((user_id % 5))]}"
    local city="${cities[$((user_id % 10))]}"
    local age=$((20 + user_id % 50))
    
    echo "{
        \"name\": \"$name\",
        \"email\": \"$email\",
        \"age\": $age,
        \"city\": \"$city\",
        \"created_at\": \"$(date -u +"%Y-%m-%dT%H:%M:%SZ")\"
    }"
}

# Populate with test data
populate_data() {
    local num_users=${1:-100}
    log_info "Populating table with $num_users test users..."
    
    for i in $(seq 1 $num_users); do
        user_data=$(generate_user_data $i)
        user_key="user_$i"
        
        response=$(curl -s -w "%{http_code}" -X POST "$KVDB_API/data/$TEST_DB/$TEST_TABLE/$user_key" \
            -H "Content-Type: application/json" \
            -d "$user_data")
        http_code="${response: -3}"
        
        if [ "$http_code" = "200" ]; then
            if [ $((i % 10)) -eq 0 ]; then
                log_info "Created $i users..."
            fi
        else
            log_error "Failed to create user $user_key (HTTP: $http_code)"
        fi
        
        # Small delay to avoid overwhelming the system
        sleep 0.01
    done
    
    log_info "Successfully populated $num_users users!"
}

# Read and verify some data
verify_data() {
    log_info "Verifying data..."
    
    # Read a few random users
    for i in 1 5 10 50 100; do
        if [ $i -le $num_users ]; then
            user_key="user_$i"
            response=$(curl -s "$KVDB_API/data/$TEST_DB/$TEST_TABLE/$user_key")
            
            if [ "$response" != "Value not found" ] && [ -n "$response" ]; then
                log_info "Verified: $user_key exists"
            else
                log_error "Failed to read: $user_key"
            fi
        fi
    done
}

# Show statistics
show_stats() {
    log_info "Getting database statistics..."
    
    # Catalog stats
    catalog_stats=$(curl -s "$KVDB_API/stats/catalog")
    log_info "Catalog Statistics:"
    echo "$catalog_stats" | jq -r '
        "  Total Databases: " + .totalDatabases,
        "  Total Tables: " + .totalTables,
        "  Total Entries: " + .totalEntries,
        "  Active Entries: " + .activeEntries,
        "  Data Size: " + (.totalDataSize | tostring) + " bytes"
    ' 2>/dev/null || echo "  Could not parse catalog stats"
    
    echo ""
    
    # Database stats
    db_stats=$(curl -s "$KVDB_API/stats/database/$TEST_DB")
    log_info "Database '$TEST_DB' Statistics:"
    echo "$db_stats" | jq -r '
        "  Total Tables: " + .totalTables,
        "  Total Entries: " + .totalEntries,
        "  Active Entries: " + .activeEntries,
        "  Data Size: " + (.totalDataSize | tostring) + " bytes"
    ' 2>/dev/null || echo "  Could not parse database stats"
    
    echo ""
    
    # Table stats
    table_stats=$(curl -s "$KVDB_API/stats/table/$TEST_DB/$TEST_TABLE")
    log_info "Table '$TEST_TABLE' Statistics:"
    echo "$table_stats" | jq -r '
        "  Total Entries: " + .totalEntries,
        "  Active Entries: " + .activeEntries,
        "  Deleted Entries: " + .deletedEntries,
        "  Data Size: " + (.totalDataSize | tostring) + " bytes",
        "  Segments: " + .segmentCount
    ' 2>/dev/null || echo "  Could not parse table stats"
}

# Clean up test data
cleanup() {
    log_warn "Cleaning up test data..."
    
    # Delete all users
    for i in $(seq 1 100); do
        user_key="user_$i"
        curl -s -X DELETE "$KVDB_API/data/$TEST_DB/$TEST_TABLE/$user_key" > /dev/null
    done
    
    # Delete table and database
    curl -s -X DELETE "$KVDB_API/data/$TEST_DB/$TEST_TABLE" > /dev/null
    curl -s -X DELETE "$KVDB_API/data/$TEST_DB" > /dev/null
    
    log_info "Test data cleaned up!"
}

# Show usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -n, --number NUM     Number of users to create (default: 100)"
    echo "  -c, --cleanup        Clean up existing test data"
    echo "  -v, --verify        Verify data after creation"
    echo "  -s, --stats         Show statistics only"
    echo "  -h, --help          Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                  # Create 100 test users"
    echo "  $0 -n 500           # Create 500 test users"
    echo "  $0 -c              # Clean up test data"
    echo "  $0 -n 50 -v        # Create 50 users and verify"
    echo "  $0 -s              # Show current statistics"
}

# Parse command line arguments
NUM_USERS=100
CLEANUP_ONLY=false
VERIFY=false
STATS_ONLY=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -n|--number)
            NUM_USERS="$2"
            shift 2
            ;;
        -c|--cleanup)
            CLEANUP_ONLY=true
            shift
            ;;
        -v|--verify)
            VERIFY=true
            shift
            ;;
        -s|--stats)
            STATS_ONLY=true
            shift
            ;;
        -h|--help)
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
    if [ "$STATS_ONLY" = true ]; then
        check_kvdb
        show_stats
        exit 0
    fi
    
    if [ "$CLEANUP_ONLY" = true ]; then
        check_kvdb
        cleanup
        exit 0
    fi
    
    check_kvdb
    create_database
    create_table
    populate_data $NUM_USERS
    
    if [ "$VERIFY" = true ]; then
        verify_data
    fi
    
    show_stats
    
    log_info "Test data population completed!"
    log_info "Database: $TEST_DB"
    log_info "Table: $TEST_TABLE"
    log_info "Users created: $NUM_USERS"
    log_info ""
    log_info "To clean up: $0 -c"
    log_info "To see stats: $0 -s"
}

# Run main function
main
