#!/bin/bash
# Fixed setup script for StreamShift MVP

set -e

echo "StreamShift MVP Setup"
echo "===================="

function wait_for_port() {
    local port=$1
    local service=$2
    echo "Waiting for $service on port $port..."

    for i in {1..60}; do
        if nc -z localhost $port 2>/dev/null; then
            echo "$service is ready!"
            return 0
        fi
        sleep 2
        echo -n "."
    done

    echo "ERROR: $service failed to start"
    return 1
}

function wait_for_debezium() {
    echo "Waiting for Debezium to be fully ready..."

    for i in {1..30}; do
        if curl -s localhost:8083/connectors >/dev/null 2>&1; then
            echo "Debezium is responding!"
            return 0
        fi
        sleep 3
        echo -n "."
    done

    echo "ERROR: Debezium not responding"
    return 1
}

function register_connector() {
    echo "Registering Debezium connector..."

    # Extra wait to ensure Debezium is really ready
    sleep 15

    curl -X POST -H "Content-Type:application/json" localhost:8083/connectors/ -d '{
      "name": "inventory-connector",
      "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max": "1",
        "database.hostname": "postgres-source",
        "database.port": "5432",
        "database.user": "postgres",
        "database.password": "postgres",
        "database.dbname": "inventory",
        "database.server.name": "dbserver1",
        "schema.include.list": "inventory",
        "table.include.list": "inventory.customers",
        "plugin.name": "pgoutput",
        "slot.name": "streamshift_slot"
      }
    }'

    echo ""
    echo "Connector registration attempted!"

    # Verify registration
    sleep 5
    echo "Checking connector status..."
    curl -s localhost:8083/connectors/inventory-connector/status | grep -q "RUNNING" && echo "Connector is RUNNING!" || echo "Connector may need more time to start"
}

function check_status() {
    echo "Checking system status..."

    echo "Connectors:"
    curl -s localhost:8083/connectors || echo "Debezium not responding"

    echo ""
    echo "Kafka topics:"
    docker exec streamshift-kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | grep dbserver1 || echo "No Debezium topics yet"

    echo ""
    echo "Database test:"
    docker exec streamshift-postgres-source psql -U postgres -d inventory -c "SELECT COUNT(*) FROM inventory.customers;" 2>/dev/null || echo "Database connection failed"
}

function test_changes() {
    echo "Testing database changes..."

    docker exec streamshift-postgres-source psql -U postgres -d inventory -c "
        INSERT INTO inventory.customers (first_name, last_name, email)
        VALUES ('Test', 'User', 'test@example.com');
    "

    sleep 3

    docker exec streamshift-postgres-source psql -U postgres -d inventory -c "
        UPDATE inventory.customers
        SET first_name = 'Updated'
        WHERE email = 'test@example.com';
    "

    sleep 3

    docker exec streamshift-postgres-source psql -U postgres -d inventory -c "
        DELETE FROM inventory.customers
        WHERE email = 'test@example.com';
    "

    echo "Test changes completed! Check your app logs."
}

case "${1:-}" in
    "test")
        test_changes
        ;;
    "status")
        check_status
        ;;
    "clean")
        echo "Cleaning up..."
        docker-compose down -v
        ;;
    "connector")
        register_connector
        ;;
    *)
        echo "Starting infrastructure..."
        docker-compose up -d

        wait_for_port 9092 "Kafka"
        wait_for_port 5432 "PostgreSQL"
        wait_for_port 8083 "Debezium HTTP"

        wait_for_debezium
        register_connector

        echo ""
        echo "Setup complete!"
        echo ""
        echo "Next steps:"
        echo "1. Check status: ./scripts/setup-postgres.sh status"
        echo "2. Install Maven: brew install maven"
        echo "3. Start app: mvn spring-boot:run"
        echo "4. Test: ./scripts/setup-postgres.sh test"
        echo "5. Monitor: http://localhost:8080"
        ;;
esac