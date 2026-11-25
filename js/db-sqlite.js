/**
 * SQLite Local Storage & Repository Manager (SQLite 3.45+ / D1 Model)
 * Persists AsyncAPI contracts, Schema Registry versions, broker telemetry and event logs
 */

const DatabaseSQLite = (() => {
  const STORAGE_KEY = 'codex_asyncapi_studio_sqlite';

  let dbState = {
    async_contracts: [],
    event_schemas: [],
    broker_metrics: [],
    event_audit_log: [],
    schema_compatibility_audits: []
  };

  function init() {
    try {
      const saved = localStorage.getItem(STORAGE_KEY);
      if (saved) {
        dbState = JSON.parse(saved);
      } else {
        seedDefaults();
      }
    } catch (e) {
      console.warn('Could not load from localStorage, initializing fresh state', e);
      seedDefaults();
    }
  }

  function seedDefaults() {
    dbState = {
      async_contracts: [
        {
          id: 'ecommerce-kafka',
          title: 'E-Commerce Order Fulfillment Mesh',
          version: '3.0.0',
          protocol: 'kafka',
          description: 'Distributed event mesh handling checkout, payments, inventory reservation and DLQ routing.',
          created_at: new Date().toISOString()
        },
        {
          id: 'iot-fleet-mqtt',
          title: 'Smart Fleet Telematics & Predictive Maintenance',
          version: '3.0.0',
          protocol: 'mqtt',
          description: 'MQTT v5 event broker collecting telemetry, tire pressure and geofencing from connected electric vehicles.',
          created_at: new Date().toISOString()
        },
        {
          id: 'realtime-chat-ws',
          title: 'Omnichannel Realtime Chat & Collab Mesh',
          version: '3.0.0',
          protocol: 'ws',
          description: 'Duplex WebSocket streaming mesh with client-server bi-directional channels.',
          created_at: new Date().toISOString()
        }
      ],
      event_schemas: [
        {
          id: 'schema-order-v1',
          contract_id: 'ecommerce-kafka',
          schema_name: 'OrderCreatedPayload',
          version: 1,
          format: 'json_schema',
          compatibility_mode: 'BACKWARD',
          created_at: new Date().toISOString()
        },
        {
          id: 'schema-order-v2',
          contract_id: 'ecommerce-kafka',
          schema_name: 'OrderCreatedPayload',
          version: 2,
          format: 'json_schema',
          compatibility_mode: 'BACKWARD',
          created_at: new Date().toISOString()
        }
      ],
      broker_metrics: [],
      event_audit_log: [],
      schema_compatibility_audits: []
    };
    persist();
  }

  function persist() {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(dbState));
    } catch (e) {
      console.warn('Storage quota exceeded, pruning old logs', e);
      if (dbState.event_audit_log.length > 100) {
        dbState.event_audit_log = dbState.event_audit_log.slice(-100);
      }
    }
  }

  function saveContract(contract) {
    const existingIdx = dbState.async_contracts.findIndex(c => c.id === contract.id);
    if (existingIdx >= 0) {
      dbState.async_contracts[existingIdx] = { ...dbState.async_contracts[existingIdx], ...contract, updated_at: new Date().toISOString() };
    } else {
      dbState.async_contracts.push({ ...contract, created_at: new Date().toISOString() });
    }
    persist();
  }

  function logEvent(event) {
    dbState.event_audit_log.unshift({
      id: event.id || 'evt_' + Math.random().toString(36).substring(2, 9),
      channel_name: event.channel,
      partition_id: event.partition || 0,
      message_key: event.key || 'null',
      payload_json: typeof event.payload === 'string' ? event.payload : JSON.stringify(event.payload || {}),
      status: event.status || 'ACKNOWLEDGED',
      retry_count: event.retryCount || 0,
      latency_ms: Number((10 + Math.random() * 20).toFixed(1)),
      published_at: new Date().toISOString()
    });

    if (dbState.event_audit_log.length > 250) {
      dbState.event_audit_log.pop();
    }
    persist();
  }

  function recordMetrics(m) {
    dbState.broker_metrics.push({
      id: dbState.broker_metrics.length + 1,
      protocol: m.protocol || 'kafka',
      throughput_mps: m.throughput,
      consumer_lag: m.consumerLag,
      dlq_count: m.dlqCount,
      error_rate: m.failureRate,
      recorded_at: new Date().toISOString()
    });

    if (dbState.broker_metrics.length > 100) {
      dbState.broker_metrics.shift();
    }
    persist();
  }

  function getTableData(tableName) {
    return dbState[tableName] || [];
  }

  /**
   * Generates a fully formatted SQLite dump script (.sql)
   */
  function exportSqlDump() {
    let sql = `-- ============================================================================\n`;
    sql += `-- AsyncAPI 3.0 Event Mesh Studio - SQLite 3.45+ Database Export\n`;
    sql += `-- Generated on: ${new Date().toISOString()}\n`;
    sql += `-- ============================================================================\n\n`;
    sql += `PRAGMA foreign_keys = ON;\n\n`;

    // 1. async_contracts
    sql += `-- Table: async_contracts\n`;
    sql += `CREATE TABLE IF NOT EXISTS async_contracts (\n`;
    sql += `  id TEXT PRIMARY KEY,\n  title TEXT NOT NULL,\n  version TEXT NOT NULL DEFAULT '1.0.0',\n  protocol TEXT NOT NULL,\n  description TEXT,\n  created_at DATETIME DEFAULT CURRENT_TIMESTAMP\n);\n`;
    dbState.async_contracts.forEach(c => {
      sql += `INSERT OR REPLACE INTO async_contracts (id, title, version, protocol, description) VALUES (${escapeSql(c.id)}, ${escapeSql(c.title)}, ${escapeSql(c.version)}, ${escapeSql(c.protocol)}, ${escapeSql(c.description)});\n`;
    });
    sql += `\n`;

    // 2. event_schemas
    sql += `-- Table: event_schemas\n`;
    sql += `CREATE TABLE IF NOT EXISTS event_schemas (\n`;
    sql += `  id TEXT PRIMARY KEY,\n  contract_id TEXT NOT NULL,\n  schema_name TEXT NOT NULL,\n  version INTEGER NOT NULL,\n  format TEXT NOT NULL,\n  compatibility_mode TEXT NOT NULL\n);\n`;
    dbState.event_schemas.forEach(s => {
      sql += `INSERT OR REPLACE INTO event_schemas (id, contract_id, schema_name, version, format, compatibility_mode) VALUES (${escapeSql(s.id)}, ${escapeSql(s.contract_id)}, ${escapeSql(s.schema_name)}, ${s.version}, ${escapeSql(s.format)}, ${escapeSql(s.compatibility_mode)});\n`;
    });
    sql += `\n`;

    // 3. event_audit_log
    sql += `-- Table: event_audit_log (Recent simulated messages)\n`;
    sql += `CREATE TABLE IF NOT EXISTS event_audit_log (\n`;
    sql += `  id TEXT PRIMARY KEY,\n  channel_name TEXT NOT NULL,\n  partition_id INTEGER,\n  message_key TEXT,\n  payload_json TEXT NOT NULL,\n  status TEXT NOT NULL,\n  latency_ms REAL,\n  published_at DATETIME\n);\n`;
    dbState.event_audit_log.slice(0, 50).forEach(e => {
      sql += `INSERT OR REPLACE INTO event_audit_log (id, channel_name, partition_id, message_key, payload_json, status, latency_ms) VALUES (${escapeSql(e.id)}, ${escapeSql(e.channel_name)}, ${e.partition_id}, ${escapeSql(e.message_key)}, ${escapeSql(e.payload_json)}, ${escapeSql(e.status)}, ${e.latency_ms});\n`;
    });

    return sql;
  }

  function escapeSql(str) {
    if (str === null || str === undefined) return 'NULL';
    return "'" + String(str).replace(/'/g, "''") + "'";
  }

  return {
    init,
    saveContract,
    logEvent,
    recordMetrics,
    getTableData,
    exportSqlDump,
    seedDefaults
  };
})();

if (typeof window !== 'undefined') {
  window.DatabaseSQLite = DatabaseSQLite;
}
