#!/bin/bash
# Setup script for StreamShift

set -e

echo "StreamShift Setup"
echo "=============================="

function wait_for_service() {
    local port=$1
    local service=$2
    local max_attempts=${3:-30}

    echo "Waiting for $service on port $port..."

    for i in $(seq 1 $max_attempts); do
        if nc -z localhost $port 2>/dev/null; then
            echo "✓ $service is ready!"
            return 0
        fi
        sleep 2
        echo -n "."
    done

    echo "✗ $service failed to start on port $port"
    return 1
}

function check_kafka_ready() {
    echo "Checking if Kafka is fully ready..."

    for i in {1..30}; do
        if docker exec streamshift-kafka kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
            echo "✓ Kafka is accepting connections!"
            return 0
        fi
        sleep 3
        echo -n "."
    done

    echo "✗ Kafka not responding to topic commands"
    return 1
}

function register_connector() {
    echo "Registering Debezium connector..."

    # Wait for Debezium to be ready
    for i in {1..20}; do
        if curl -s localhost:8083/connectors >/dev/null 2>&1; then
            echo "✓ Debezium is responding!"
            break
        fi
        sleep 5
        echo -n "."
    done

    # Give it a bit more time
    sleep 10

    # Register the connector
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
        "topic.prefix": "dbserver1",
        "schema.include.list": "inventory",
        "table.include.list": "inventory.customers",
        "plugin.name": "pgoutput",
        "slot.name": "streamshift_slot"
      }
    }'

    echo ""
    echo "Connector registration completed!"
}

function show_status() {
    echo "=== System Status ==="

    echo "Docker containers:"
    docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

    echo ""
    echo "Debezium connectors:"
    curl -s localhost:8083/connectors 2>/dev/null || echo "Debezium not responding"

    echo ""
    echo "Kafka topics:"
    docker exec streamshift-kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | head -10
}

function test_changes() {
    echo "Testing database changes..."

    docker exec streamshift-postgres-source psql -U postgres -d inventory -c "
        INSERT INTO inventory.customers (first_name, last_name, email)
        VALUES ('Test', 'User', 'test@example.com');
    "

    echo "Inserted test record. Check your StreamShift logs!"
}

function restart_kafka() {
    echo "Restarting Kafka services..."
    docker-compose restart kafka
    sleep 10
    docker-compose restart debezium
    sleep 15
}

case "${1:-}" in
    "start")
        echo "Starting infrastructure..."
        docker-compose up -d

        wait_for_service 2181 "Zookeeper"
        wait_for_service 5432 "PostgreSQL Source"
        wait_for_service 5433 "PostgreSQL Target"

        # Give Kafka more time
        sleep 20
        wait_for_service 9092 "Kafka"
        check_kafka_ready

        wait_for_service 8083 "Debezium"
        register_connector

        wait_for_service 9090 "Prometheus"
        wait_for_service 3000 "Grafana"

        echo ""
        echo "✓ Setup complete!"
        echo "Next: mvn spring-boot:run"
        ;;
    "restart-kafka")
        restart_kafka
        ;;
    "connector")
        register_connector
        ;;
    "test")
        test_changes
        ;;
    "status")
        show_status
        ;;
    "clean")
        echo "Cleaning up..."
        docker-compose down -v
        docker system prune -f
        ;;
    "logs")
        service=${2:-kafka}
        docker logs streamshift-$service
        ;;
    *)
        echo "Usage: $0 {start|restart-kafka|connector|test|status|clean|logs}"
        echo ""
        echo "Commands:"
        echo "  start         - Start all services"
        echo "  restart-kafka - Restart Kafka and Debezium"
        echo "  connector     - Register Debezium connector"
        echo "  test          - Insert test data"
        echo "  status        - Show system status"
        echo "  clean         - Stop and remove everything"
        echo "  logs [service] - Show logs (default: kafka)"
        ;;
esac