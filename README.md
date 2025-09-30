# StreamShift

A zero-downtime database migration tool using Change Data Capture (CDC) to stream real-time data changes from source to target databases through Apache Kafka.

## Overview

StreamShift enables seamless database migrations without taking your applications offline. By leveraging Debezium CDC and Kafka streaming, it captures every INSERT, UPDATE, and DELETE operation from your source database and applies them to your target database in real-time.

## Architecture
```mermaid
graph TD
    A[Source Database<br/>PostgreSQL] -->|WAL| B[Debezium CDC<br/>Captures changes]
    B --> C[Apache Kafka<br/>Streams events]
    C --> D[StreamShift Application<br/>Processes & validates]
    D --> E[Target Database<br/>PostgreSQL]
    
    style A fill:#e1f5fe
    style B fill:#fff3e0
    style C fill:#f3e5f5
    style D fill:#e8f5e8
    style E fill:#e1f5fe