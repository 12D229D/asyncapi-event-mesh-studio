-- ============================================================================
-- AsyncAPI 3.0 Event-Driven Architecture Studio - Relational Schema (SQLite 3.45+)
-- Storage for AsyncAPI Contracts, Event Schemas, Schema Registry and Broker Logs
-- ============================================================================

PRAGMA foreign_keys = ON;

-- 1. Table: async_contracts (AsyncAPI 3.0 specifications)
CREATE TABLE IF NOT EXISTS async_contracts (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    version TEXT NOT NULL DEFAULT '1.0.0',
    protocol TEXT NOT NULL CHECK(protocol IN ('kafka', 'mqtt', 'ws', 'amqp', 'sse')),
    description TEXT,
    yaml_content TEXT NOT NULL,
    json_spec TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 2. Table: event_schemas (Schema Registry v1, v2 and compatibility records)
CREATE TABLE IF NOT EXISTS event_schemas (
    id TEXT PRIMARY KEY,
    contract_id TEXT NOT NULL,
    schema_name TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    format TEXT NOT NULL DEFAULT 'json_schema' CHECK(format IN ('json_schema', 'avro', 'protobuf')),
    compatibility_mode TEXT NOT NULL DEFAULT 'BACKWARD' CHECK(compatibility_mode IN ('BACKWARD', 'FORWARD', 'FULL', 'NONE')),
    schema_definition TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (contract_id) REFERENCES async_contracts(id) ON DELETE CASCADE
);

-- 3. Table: schema_compatibility_audits (Diff evaluations between schema versions)
CREATE TABLE IF NOT EXISTS schema_compatibility_audits (
    id TEXT PRIMARY KEY,
    schema_name TEXT NOT NULL,
    version_from INTEGER NOT NULL,
    version_to INTEGER NOT NULL,
    mode TEXT NOT NULL,
    is_compatible BOOLEAN NOT NULL DEFAULT 1,
    breaking_changes_json TEXT,
    evaluated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 4. Table: broker_metrics (Time-series telemetry of simulated broker flow)
CREATE TABLE IF NOT EXISTS broker_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    contract_id TEXT,
    protocol TEXT NOT NULL,
    throughput_mps REAL NOT NULL DEFAULT 0.0,
    consumer_lag INTEGER NOT NULL DEFAULT 0,
    dlq_count INTEGER NOT NULL DEFAULT 0,
    error_rate REAL NOT NULL DEFAULT 0.0,
    recorded_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 5. Table: event_audit_log (Simulated messages published through the Event Mesh)
CREATE TABLE IF NOT EXISTS event_audit_log (
    id TEXT PRIMARY KEY,
    contract_id TEXT,
    channel_name TEXT NOT NULL,
    partition_id INTEGER DEFAULT 0,
    message_key TEXT,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('ACKNOWLEDGED', 'PROCESSING', 'RETRYING', 'DEAD_LETTER_QUEUE')),
    retry_count INTEGER DEFAULT 0,
    latency_ms REAL DEFAULT 0.0,
    published_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for high-performance querying
CREATE INDEX IF NOT EXISTS idx_contracts_protocol ON async_contracts(protocol);
CREATE INDEX IF NOT EXISTS idx_schemas_contract ON event_schemas(contract_id, schema_name);
CREATE INDEX IF NOT EXISTS idx_event_audit_channel ON event_audit_log(channel_name, status);
CREATE INDEX IF NOT EXISTS idx_metrics_recorded ON broker_metrics(recorded_at);

-- Initial seed data
INSERT OR REPLACE INTO async_contracts (id, title, version, protocol, description, yaml_content) VALUES 
('ecommerce-kafka', 'E-Commerce Order Fulfillment Mesh', '3.0.0', 'kafka', 'Distributed event mesh handling checkout, payments, inventory reservation and DLQ routing.', 
'asyncapi: 3.0.0
info:
  title: E-Commerce Order Fulfillment Mesh
  version: 3.0.0
  description: Distributed event stream for checkout, fraud screening and inventory
servers:
  production-kafka:
    host: broker.kafka.internal:9092
    protocol: kafka
    description: Production Apache Kafka Cluster (3 Partitions)
channels:
  orderEvents:
    address: retail.orders.v1
    messages:
      orderCreated:
        $ref: "#/components/messages/OrderCreated"
      paymentProcessed:
        $ref: "#/components/messages/PaymentProcessed"
  orderDlq:
    address: retail.orders.dlq
    messages:
      orderFailed:
        $ref: "#/components/messages/OrderFailed"
operations:
  publishOrderCreated:
    action: send
    channel:
      $ref: "#/channels/orderEvents"
  consumePaymentProcessed:
    action: receive
    channel:
      $ref: "#/channels/orderEvents"
components:
  messages:
    OrderCreated:
      payload:
        type: object
        required: [orderId, customerId, amount, currency, status]
        properties:
          orderId: { type: string, format: uuid }
          customerId: { type: string }
          amount: { type: number, minimum: 0 }
          currency: { type: string, enum: [USD, EUR, GBP] }
          status: { type: string, enum: [PENDING, PAID, FULFILLED] }
');
