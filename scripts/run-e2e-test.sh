#!/bin/bash
set -e # Exit immediately if a command exits with a non-zero status

# --- 1. SETUP PHASE ---
echo "--- 1. Starting up environment ---"
docker-compose down -v
docker-compose up -d

# Wait for services and register Debezium connector
./scripts/setup-postgres.sh start

echo "Building and starting StreamShift application..."
# Target DB is running on port 5433 and is named 'inventory_target'.
mvn spring-boot:run -Dspring-boot.run.arguments="\
--streamshift.target.db.url=jdbc:postgresql://localhost:5433/inventory_target \
--streamshift.target.db.username=postgres \
--streamshift.target.db.password=postgres" &
APP_PID=$!

# CRITICAL: Wait for StreamShift to fully initialize and connect to Kafka
echo "Waiting 15 seconds for StreamShift consumer to connect..."
sleep 15

# --- 2. ACTION PHASE: Execute Transactions (CRUD Test) ---
SOURCE_DB_CONTAINER="streamshift-postgres-source"
TARGET_DB_CONTAINER="streamshift-postgres-target"

echo "--- 2. Executing Test Transactions on Source DB ---"

# Transaction 1: INSERT
docker exec $SOURCE_DB_CONTAINER psql -U postgres -d inventory -c "
INSERT INTO inventory.customers (first_name, last_name, email) VALUES ('Test', 'Insert', 'insert@test.com');
"

# Transaction 2: UPDATE
docker exec $SOURCE_DB_CONTAINER psql -U postgres -d inventory -c "
UPDATE inventory.customers SET first_name = 'Updated' WHERE email = 'insert@test.com';
"

# Transaction 3: INSERT
docker exec $SOURCE_DB_CONTAINER psql -U postgres -d inventory -c "
INSERT INTO inventory.customers (first_name, last_name, email, id) VALUES ('Delete', 'Me', 'delete@test.com', 99);
"

# Transaction 4: DELETE
docker exec $SOURCE_DB_CONTAINER psql -U postgres -d inventory -c "
DELETE FROM inventory.customers WHERE email = 'delete@test.com';
"

# Transaction 5: UPDATE on a NON-EXISTENT record
# This test is successful if the pipeline DOES NOT break, because no Debezium event is generated
# when 0 rows are affected by an UPDATE in the source database.
docker exec $SOURCE_DB_CONTAINER psql -U postgres -d inventory -c "
UPDATE inventory.customers SET first_name = 'FAIL_TEST' WHERE id = 9999;
"

# Transaction 6: INSERT (Final standalone record) - FIXED
docker exec $SOURCE_DB_CONTAINER psql -U postgres -d inventory -c "
INSERT INTO inventory.customers (first_name, last_name, email) VALUES ('Jane', 'Foster', 'jane.foster@test.com');
"

echo "Waiting 5 seconds for events to be processed by StreamShift..."
sleep 5

# --- 3. ASSERTION PHASE: Verify Target DB ---
echo "--- 3. Verifying Results in Target DB ---"
TEST_FAIL=0

# Verification 1: Check for the final updated state of 'insert@test.com'
FINAL_NAME=$(docker exec $TARGET_DB_CONTAINER psql -h localhost -U postgres -d inventory_target -t -A -c "
SELECT first_name FROM customers WHERE email = 'insert@test.com';
" | xargs)

if [ "$FINAL_NAME" == "Updated" ]; then
    echo "PASS: Updated record 'Updated' verified."
else
    echo "FAIL: Expected 'Updated' for insert@test.com but found '$FINAL_NAME'."
    TEST_FAIL=1
fi

# Verification 2: Check for the newly inserted record - FIXED
JANE_NAME=$(docker exec $TARGET_DB_CONTAINER psql -h localhost -U postgres -d inventory_target -t -A -c "
SELECT first_name FROM customers WHERE email = 'jane.foster@test.com';
" | xargs)

if [ "$JANE_NAME" == "Jane" ]; then
    echo "PASS: New record 'Jane' verified."
else
    echo "FAIL: Expected 'Jane' but found '$JANE_NAME'."
    TEST_FAIL=1
fi

# Verification 3: Check for the deleted record (expect empty result)
DELETED_RECORD=$(docker exec $TARGET_DB_CONTAINER psql -h localhost -U postgres -d inventory_target -t -A -c "
SELECT first_name FROM customers WHERE email = 'delete@test.com';
" | xargs)

if [ -z "$DELETED_RECORD" ]; then
    echo "PASS: Deleted record 'delete@test.com' successfully removed."
else
    echo "FAIL: Deleted record 'delete@test.com' was found (value: '$DELETED_RECORD')."
    TEST_FAIL=1
fi

# --- 4. CLEANUP AND EXIT ---
echo "--- 4. Cleanup ---"
kill $APP_PID
wait $APP_PID 2>/dev/null
docker-compose down -v

if [ "$TEST_FAIL" == "1" ]; then
    echo "--- E2E TEST FAILED ---"
    exit 1
else
    echo "--- E2E TEST SUCCESS ---"
    exit 0
fi