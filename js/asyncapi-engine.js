/**
 * AsyncAPI 3.0 Contract Engine
 * Parser, validator, linter and reference architectures for Kafka, MQTT, WebSockets and AMQP
 */

const AsyncAPIEngine = (() => {
  // Built-in Reference Architectures
  const TEMPLATES = {
    'ecommerce-kafka': {
      id: 'ecommerce-kafka',
      title: 'E-Commerce Order Fulfillment & Payment Mesh',
      protocol: 'kafka',
      version: '3.0.0',
      description: 'Distributed Apache Kafka stream handling checkout, payment auth, inventory allocation and DLQ routing across 3 partitions.',
      yaml: `asyncapi: 3.0.0
info:
  title: E-Commerce Order Fulfillment & Payment Mesh
  version: 3.0.0
  description: Distributed Apache Kafka stream handling checkout, payment auth and DLQ routing.
servers:
  production-kafka:
    host: broker.kafka.us-east.aws.internal:9092
    protocol: kafka
    description: Multi-broker Apache Kafka Cluster (3 Partitions, Replication Factor 3)
    bindings:
      kafka:
        schemaRegistryUrl: https://schema-registry.internal:8081
channels:
  orderEvents:
    address: retail.orders.v1
    description: Primary order stream partitioned by customer UUID
    bindings:
      kafka:
        partitions: 3
        replicas: 3
        retention: 604800000
    messages:
      orderCreated:
        $ref: '#/components/messages/OrderCreated'
      paymentProcessed:
        $ref: '#/components/messages/PaymentProcessed'
  orderDeadLetterQueue:
    address: retail.orders.dlq
    description: Quarantine queue for corrupted payloads, deserialization errors or fraud flags
    bindings:
      kafka:
        partitions: 1
    messages:
      orderFailed:
        $ref: '#/components/messages/OrderFailed'
operations:
  publishOrderCreated:
    action: send
    channel:
      $ref: '#/channels/orderEvents'
    description: Published by Web Checkout Frontend & Mobile App
  consumeOrderForPayment:
    action: receive
    channel:
      $ref: '#/channels/orderEvents'
    description: Consumed by Payment Gateway Consumer Group
  consumeOrderForInventory:
    action: receive
    channel:
      $ref: '#/channels/orderEvents'
    description: Consumed by Logistics & Warehouse Allocation Service
  routeToDeadLetterQueue:
    action: send
    channel:
      $ref: '#/channels/orderDeadLetterQueue'
    description: Routing failed transactions to DLQ
components:
  messages:
    OrderCreated:
      name: OrderCreated
      title: New Customer Order
      contentType: application/json
      bindings:
        kafka:
          key:
            type: string
            description: Customer UUID used for partition hashing
      payload:
        type: object
        required: [orderId, customerId, amount, currency, status]
        properties:
          orderId:
            type: string
            format: uuid
          customerId:
            type: string
          amount:
            type: number
            minimum: 0.01
          currency:
            type: string
            enum: [USD, EUR, GBP, CAD]
          status:
            type: string
            enum: [PENDING, PAID, FULFILLED]
    PaymentProcessed:
      name: PaymentProcessed
      title: Payment Authorization Status
      contentType: application/json
      payload:
        type: object
        required: [orderId, transactionId, status, processedAt]
        properties:
          orderId:
            type: string
          transactionId:
            type: string
          status:
            type: string
            enum: [SUCCESS, DECLINED, FRAUD_SUSPECTED]
          processedAt:
            type: string
            format: date-time
    OrderFailed:
      name: OrderFailed
      title: Dead-Letter Poison Message
      contentType: application/json
      payload:
        type: object
        required: [originalPayload, failureReason, retryCount, timestamp]
        properties:
          originalPayload:
            type: object
          failureReason:
            type: string
          retryCount:
            type: integer
          timestamp:
            type: string`,
      schemaV1: {
        title: "OrderCreatedPayload",
        version: 1,
        type: "object",
        required: ["orderId", "customerId", "amount", "currency", "status"],
        properties: {
          orderId: { type: "string", format: "uuid" },
          customerId: { type: "string" },
          amount: { type: "number", minimum: 0.01 },
          currency: { type: "string", enum: ["USD", "EUR", "GBP", "CAD"] },
          status: { type: "string", enum: ["PENDING", "PAID", "FULFILLED"] }
        }
      },
      schemaV2: {
        title: "OrderCreatedPayload",
        version: 2,
        type: "object",
        required: ["orderId", "customerId", "amount", "currency", "status", "taxRegion"],
        properties: {
          orderId: { type: "string", format: "uuid" },
          customerId: { type: "string" },
          amount: { type: "number", minimum: 0.01 },
          currency: { type: "string", enum: ["USD", "EUR", "GBP", "CAD"] },
          status: { type: "string", enum: ["PENDING", "PAID", "FULFILLED"] },
          taxRegion: { type: "string" },
          loyaltyPointsEarned: { type: "integer", default: 0 }
        }
      }
    },

    'iot-fleet-mqtt': {
      id: 'iot-fleet-mqtt',
      title: 'Smart Fleet Telematics & Predictive Maintenance',
      protocol: 'mqtt',
      version: '3.0.0',
      description: 'MQTT v5 event broker collecting telemetry, tire pressure and geofencing from connected electric vehicles with QoS 1/2.',
      yaml: `asyncapi: 3.0.0
info:
  title: Smart Fleet Telematics & Predictive Maintenance
  version: 3.0.0
  description: High-frequency telemetry ingestion over MQTT v5
servers:
  fleetMqttBroker:
    host: mqtt.fleetmesh.cloud:8883
    protocol: mqtt
    description: Secure EMQX / Mosquitto MQTT v5 Cluster with TLS
    bindings:
      mqtt:
        cleanSession: false
        keepAlive: 60
channels:
  vehicleTelemetry:
    address: telematics/vehicles/{vin}/metrics
    parameters:
      vin:
        description: 17-character Vehicle Identification Number
    bindings:
      mqtt:
        qos: 1
        retain: false
    messages:
      telemetryReport:
        $ref: '#/components/messages/TelemetryReport'
  emergencyAlerts:
    address: telematics/alerts/critical
    bindings:
      mqtt:
        qos: 2
        retain: true
    messages:
      collisionWarning:
        $ref: '#/components/messages/CriticalAlert'
operations:
  publishVehicleTelemetry:
    action: send
    channel:
      $ref: '#/channels/vehicleTelemetry'
    description: Broadcasted by onboard ECU / OBD-II dongle every 2 seconds
  processFleetDiagnostics:
    action: receive
    channel:
      $ref: '#/channels/vehicleTelemetry'
    description: Streamed into TimescaleDB time-series pipeline
  dispatchEmergencyResponse:
    action: receive
    channel:
      $ref: '#/channels/emergencyAlerts'
components:
  messages:
    TelemetryReport:
      name: TelemetryReport
      title: Vehicle Sensor Ingestion
      contentType: application/json
      payload:
        type: object
        required: [vin, batteryLevel, speedKmh, latitude, longitude, timestamp]
        properties:
          vin: { type: string, pattern: "^[A-HJ-NPR-Z0-9]{17}$" }
          batteryLevel: { type: number, minimum: 0, maximum: 100 }
          speedKmh: { type: number, minimum: 0 }
          latitude: { type: number }
          longitude: { type: number }
          motorTempC: { type: number }
          timestamp: { type: integer }
    CriticalAlert:
      name: CriticalAlert
      title: Immediate SOS Collision or Brake Fault
      contentType: application/json
      payload:
        type: object
        required: [vin, alertCode, severity, timestamp]
        properties:
          vin: { type: string }
          alertCode: { type: string, enum: [AIRBAG_DEPLOYED, BRAKE_FAILURE, BATTERY_OVERHEAT] }
          severity: { type: string, enum: [HIGH, CRITICAL] }
          timestamp: { type: integer }`,
      schemaV1: {
        title: "TelemetryReport",
        version: 1,
        type: "object",
        required: ["vin", "batteryLevel", "speedKmh", "latitude", "longitude", "timestamp"],
        properties: {
          vin: { type: "string" },
          batteryLevel: { type: "number" },
          speedKmh: { type: "number" },
          latitude: { type: "number" },
          longitude: { type: "number" },
          timestamp: { type: "integer" }
        }
      },
      schemaV2: {
        title: "TelemetryReport",
        version: 2,
        type: "object",
        required: ["vin", "batteryLevel", "speedKmh", "latitude", "longitude", "timestamp"],
        properties: {
          vin: { type: "string" },
          batteryLevel: { type: "number" },
          speedKmh: { type: "number" },
          latitude: { type: "number" },
          longitude: { type: "number" },
          motorTempC: { type: "number", default: 45.0 },
          odometerKm: { type: "number", default: 0.0 },
          timestamp: { type: "integer" }
        }
      }
    },

    'realtime-chat-ws': {
      id: 'realtime-chat-ws',
      title: 'Omnichannel Realtime Chat & Collab Mesh',
      protocol: 'ws',
      version: '3.0.0',
      description: 'Duplex WebSocket protocol powering real-time rooms, presence, typing broadcasts and delivery receipts.',
      yaml: `asyncapi: 3.0.0
info:
  title: Omnichannel Realtime Chat & Collab Mesh
  version: 3.0.0
  description: Duplex WebSocket streaming mesh with client-server bi-directional channels
servers:
  edgeWsGateway:
    host: realtime.chatmesh.io/v1/stream
    protocol: ws
    description: Cloudflare Durable Objects / WebSocket Edge Gateway
channels:
  roomStream:
    address: rooms/{roomId}/events
    messages:
      chatMessage:
        $ref: '#/components/messages/ChatMessage'
      typingIndicator:
        $ref: '#/components/messages/TypingIndicator'
      presenceState:
        $ref: '#/components/messages/PresenceState'
operations:
  sendChatMessage:
    action: send
    channel:
      $ref: '#/channels/roomStream'
  receiveRoomBroadcast:
    action: receive
    channel:
      $ref: '#/channels/roomStream'
components:
  messages:
    ChatMessage:
      name: ChatMessage
      title: Room Conversation Message
      payload:
        type: object
        required: [messageId, roomId, senderId, content, timestamp]
        properties:
          messageId: { type: string, format: uuid }
          roomId: { type: string }
          senderId: { type: string }
          content: { type: string, maxLength: 4000 }
          timestamp: { type: integer }
    TypingIndicator:
      name: TypingIndicator
      payload:
        type: object
        required: [roomId, senderId, isTyping]
        properties:
          roomId: { type: string }
          senderId: { type: string }
          isTyping: { type: boolean }
    PresenceState:
      name: PresenceState
      payload:
        type: object
        required: [userId, status]
        properties:
          userId: { type: string }
          status: { type: string, enum: [ONLINE, AWAY, OFFLINE] }`,
      schemaV1: {
        title: "ChatMessage",
        version: 1,
        type: "object",
        required: ["messageId", "roomId", "senderId", "content", "timestamp"],
        properties: {
          messageId: { type: "string" },
          roomId: { type: "string" },
          senderId: { type: "string" },
          content: { type: "string" },
          timestamp: { type: "integer" }
        }
      },
      schemaV2: {
        title: "ChatMessage",
        version: 2,
        type: "object",
        required: ["messageId", "roomId", "senderId", "content", "timestamp"],
        properties: {
          messageId: { type: "string" },
          roomId: { type: "string" },
          senderId: { type: "string" },
          content: { type: "string" },
          attachments: { type: "array", default: [] },
          timestamp: { type: "integer" }
        }
      }
    },

    'fintech-fraud-amqp': {
      id: 'fintech-fraud-amqp',
      title: 'Fintech Real-Time Fraud & AML Detection Mesh',
      protocol: 'amqp',
      version: '3.0.0',
      description: 'AMQP 0-9-1 / RabbitMQ topic exchanges routing payments through machine learning scoring models and AML flags.',
      yaml: `asyncapi: 3.0.0
info:
  title: Fintech Real-Time Fraud & AML Detection Mesh
  version: 3.0.0
  description: High-throughput transaction routing across AMQP topic exchanges
servers:
  rabbitCluster:
    host: amqp.bankmesh.fintech.corp:5672
    protocol: amqp
    description: Clustered RabbitMQ broker with Dead-Letter Exchanges (DLX)
channels:
  paymentExchange:
    address: banking.transactions.topic
    bindings:
      amqp:
        is: routingKey
        exchange:
          name: transactions.exchange
          type: topic
          durable: true
    messages:
      trxSubmitted:
        $ref: '#/components/messages/TransactionSubmitted'
  fraudAlerts:
    address: banking.fraud.quarantine
    bindings:
      amqp:
        is: queue
        queue:
          name: q.fraud.quarantine
          durable: true
    messages:
      fraudFlagged:
        $ref: '#/components/messages/FraudAlert'
operations:
  publishTransaction:
    action: send
    channel:
      $ref: '#/channels/paymentExchange'
  evaluateFraudML:
    action: receive
    channel:
      $ref: '#/channels/paymentExchange'
  quarantineTransaction:
    action: send
    channel:
      $ref: '#/channels/fraudAlerts'
components:
  messages:
    TransactionSubmitted:
      name: TransactionSubmitted
      payload:
        type: object
        required: [trxId, accountFrom, accountTo, amount, currency, merchantCategory]
        properties:
          trxId: { type: string }
          accountFrom: { type: string }
          accountTo: { type: string }
          amount: { type: number }
          currency: { type: string }
          merchantCategory: { type: string }
    FraudAlert:
      name: FraudAlert
      payload:
        type: object
        required: [trxId, riskScore, reason, actionTaken]
        properties:
          trxId: { type: string }
          riskScore: { type: number, minimum: 0, maximum: 100 }
          reason: { type: string }
          actionTaken: { type: string, enum: [HOLD, REJECT, MANUAL_REVIEW] }`,
      schemaV1: {
        title: "TransactionSubmitted",
        version: 1,
        type: "object",
        required: ["trxId", "accountFrom", "accountTo", "amount", "currency", "merchantCategory"],
        properties: {
          trxId: { type: "string" },
          accountFrom: { type: "string" },
          accountTo: { type: "string" },
          amount: { type: "number" },
          currency: { type: "string" },
          merchantCategory: { type: "string" }
        }
      },
      schemaV2: {
        title: "TransactionSubmitted",
        version: 2,
        type: "object",
        required: ["trxId", "accountFrom", "accountTo", "amount", "currency", "merchantCategory"],
        properties: {
          trxId: { type: "string" },
          accountFrom: { type: "string" },
          accountTo: { type: "string" },
          amount: { type: "number" },
          currency: { type: "string" },
          merchantCategory: { type: "string" },
          deviceFingerprint: { type: "string", default: "unknown" }
        }
      }
    }
  };

  /**
   * Lightweight YAML Parser specifically optimized for AsyncAPI specs
   */
  function parseYaml(yamlString) {
    const lines = yamlString.split('\n');
    const result = {};
    const stack = [{ indent: -1, obj: result }];

    for (let i = 0; i < lines.length; i++) {
      let line = lines[i];
      // Strip comments
      const commentIdx = line.indexOf('#');
      if (commentIdx !== -1) {
        // Simple check if comment is not inside quotes
        line = line.substring(0, commentIdx);
      }
      if (!line.trim()) continue;

      const indent = line.search(/\S/);
      const content = line.trim();

      while (stack.length > 1 && stack[stack.length - 1].indent >= indent) {
        stack.pop();
      }

      const current = stack[stack.length - 1].obj;

      if (content.startsWith('- ')) {
        // Array item
        const itemVal = content.substring(2).trim();
        // If current is array
        if (Array.isArray(current)) {
          current.push(parseScalar(itemVal));
        }
      } else if (content.includes(':')) {
        const colonIdx = content.indexOf(':');
        const key = content.substring(0, colonIdx).trim().replace(/^['"]|['"]$/g, '');
        const valStr = content.substring(colonIdx + 1).trim();

        if (valStr === '' || valStr.startsWith('#')) {
          // Object or array starts
          const nextLine = lines[i + 1] ? lines[i + 1].trim() : '';
          let newChild = nextLine.startsWith('- ') ? [] : {};
          current[key] = newChild;
          stack.push({ indent: indent, obj: newChild });
        } else {
          // Scalar value
          current[key] = parseScalar(valStr);
        }
      }
    }
    return result;
  }

  function parseScalar(val) {
    if (val === 'true') return true;
    if (val === 'false') return false;
    if (val === 'null' || val === '~') return null;
    if (!isNaN(val) && val !== '') return Number(val);
    // Remove enclosing quotes
    if ((val.startsWith('"') && val.endsWith('"')) || (val.startsWith("'") && val.endsWith("'"))) {
      return val.substring(1, val.length - 1);
    }
    // Check for inline arrays [A, B, C]
    if (val.startsWith('[') && val.endsWith(']')) {
      return val.substring(1, val.length - 1)
        .split(',')
        .map(s => parseScalar(s.trim()))
        .filter(s => s !== '');
    }
    // Check for inline objects { a: 1 }
    if (val.startsWith('{') && val.endsWith('}')) {
      const obj = {};
      const pairs = val.substring(1, val.length - 1).split(',');
      for (const pair of pairs) {
        const [k, v] = pair.split(':');
        if (k && v) obj[k.trim()] = parseScalar(v.trim());
      }
      return obj;
    }
    return val;
  }

  /**
   * AsyncAPI 3.0 Linter & Validator
   */
  function lintAsyncAPI(spec) {
    const issues = [];
    const valid = true;

    if (!spec || typeof spec !== 'object') {
      return {
        isValid: false,
        issues: [{ type: 'error', message: 'El documento no tiene una estructura de datos válida.' }]
      };
    }

    // Check asyncapi version
    if (!spec.asyncapi) {
      issues.push({ type: 'error', message: "Falta el atributo obligatorio 'asyncapi' en la raíz (debe ser '3.0.0')." });
    } else if (!String(spec.asyncapi).startsWith('3.')) {
      issues.push({ type: 'error', message: `Versión no compatible: ${spec.asyncapi}. AsyncAPI Studio requiere versión 3.x.` });
    } else {
      issues.push({ type: 'info', message: `AsyncAPI Specification v${spec.asyncapi} detectada.` });
    }

    // Check info block
    if (!spec.info) {
      issues.push({ type: 'error', message: "Falta la sección obligatoria 'info'." });
    } else {
      if (!spec.info.title) issues.push({ type: 'error', message: "La sección 'info' debe tener un 'title'." });
      if (!spec.info.version) issues.push({ type: 'error', message: "La sección 'info' debe tener una 'version'." });
    }

    // Check channels
    if (!spec.channels || Object.keys(spec.channels).length === 0) {
      issues.push({ type: 'warning', message: "No se han definido 'channels' en la especificación." });
    } else {
      const channelCount = Object.keys(spec.channels).length;
      issues.push({ type: 'info', message: `${channelCount} canal(es) de mensajería registrados.` });
      for (const [chanKey, chanVal] of Object.entries(spec.channels)) {
        if (!chanVal.address) {
          issues.push({ type: 'warning', message: `Canal '${chanKey}' no especifica un 'address' explícito.` });
        }
      }
    }

    // Check operations
    if (!spec.operations || Object.keys(spec.operations).length === 0) {
      issues.push({ type: 'warning', message: "No se han definido 'operations' (send / receive)." });
    } else {
      for (const [opKey, opVal] of Object.entries(spec.operations)) {
        if (!opVal.action || !['send', 'receive'].includes(opVal.action)) {
          issues.push({ type: 'error', message: `Operación '${opKey}' debe tener action: 'send' o 'receive'.` });
        }
        if (!opVal.channel) {
          issues.push({ type: 'error', message: `Operación '${opKey}' no referencia un 'channel'.` });
        }
      }
    }

    const hasErrors = issues.some(i => i.type === 'error');
    return {
      isValid: !hasErrors,
      issues
    };
  }

  /**
   * Generates graph topology nodes and links from AsyncAPI 3.0 spec
   */
  function buildTopologyGraph(spec, templateMeta) {
    const nodes = [];
    const links = [];

    const protocol = (spec.servers && Object.values(spec.servers)[0]?.protocol) || templateMeta?.protocol || 'kafka';
    const serverName = (spec.servers && Object.keys(spec.servers)[0]) || 'Broker Cluster';
    const brokerHost = (spec.servers && Object.values(spec.servers)[0]?.host) || 'localhost:9092';

    // 1. Central Broker Node
    const brokerNode = {
      id: 'node-broker',
      type: 'broker',
      protocol: protocol,
      label: serverName,
      sublabel: brokerHost,
      x: 480,
      y: 260,
      radius: 40,
      partitions: protocol === 'kafka' ? 3 : (protocol === 'mqtt' ? 1 : 2)
    };
    nodes.push(brokerNode);

    // 2. Dead-Letter Queue (DLQ) Node
    const dlqNode = {
      id: 'node-dlq',
      type: 'dlq',
      label: 'Dead-Letter Queue',
      sublabel: 'Quarantine & Poison Pills',
      x: 740,
      y: 430,
      radius: 28,
      piledCount: 0
    };
    nodes.push(dlqNode);

    // 3. Channels / Topics Nodes
    const channelNodesMap = {};
    let chIdx = 0;
    const channels = spec.channels || {};
    const channelKeys = Object.keys(channels);
    const totalChannels = channelKeys.length || 1;

    channelKeys.forEach((key, idx) => {
      const ch = channels[key];
      const address = ch.address || key;
      const isDlq = key.toLowerCase().includes('dlq') || address.toLowerCase().includes('dlq');
      
      const cNode = {
        id: `channel-${key}`,
        type: isDlq ? 'dlq-channel' : 'channel',
        label: address,
        sublabel: key,
        x: 480,
        y: isDlq ? 380 : 130 + idx * 75,
        radius: 26,
        data: ch
      };
      nodes.push(cNode);
      channelNodesMap[key] = cNode;

      // Link to broker
      links.push({
        from: brokerNode.id,
        to: cNode.id,
        label: 'hosts',
        color: 'hsla(217, 33%, 55%, 0.4)'
      });

      if (isDlq) {
        links.push({
          from: cNode.id,
          to: dlqNode.id,
          label: 'reroutes',
          color: 'hsla(354, 78%, 56%, 0.7)'
        });
      }
    });

    // 4. Operations (Publishers & Subscribers)
    const operations = spec.operations || {};
    let pubCount = 0;
    let subCount = 0;

    Object.entries(operations).forEach(([opId, op]) => {
      const isSend = op.action === 'send';
      let targetChanKey = null;
      if (typeof op.channel === 'string') {
        targetChanKey = op.channel.replace('#/channels/', '');
      } else if (op.channel && op.channel.$ref) {
        targetChanKey = op.channel.$ref.replace('#/channels/', '');
      }

      const chanNode = channelNodesMap[targetChanKey] || nodes.find(n => n.type === 'channel') || brokerNode;

      if (isSend) {
        // Producer / Publisher
        const pNode = {
          id: `producer-${opId}`,
          type: 'producer',
          label: op.title || opId,
          sublabel: `Action: SEND (${protocol.toUpperCase()})`,
          x: 140,
          y: 130 + pubCount * 110,
          radius: 28,
          opData: op
        };
        nodes.push(pNode);
        links.push({
          from: pNode.id,
          to: chanNode.id,
          label: 'publishes',
          color: protocol === 'kafka' ? 'hsla(38, 95%, 52%, 0.8)' : 'hsla(150, 72%, 46%, 0.8)'
        });
        pubCount++;
      } else {
        // Consumer / Subscriber
        const sNode = {
          id: `consumer-${opId}`,
          type: 'consumer',
          label: op.title || opId,
          sublabel: `Action: RECEIVE (Lag: 0)`,
          x: 820,
          y: 120 + subCount * 110,
          radius: 28,
          lag: 0,
          opData: op
        };
        nodes.push(sNode);
        links.push({
          from: chanNode.id,
          to: sNode.id,
          label: 'consumes',
          color: 'hsla(190, 95%, 50%, 0.8)'
        });
        subCount++;
      }
    });

    // Fallbacks if no operations defined
    if (pubCount === 0) {
      nodes.push({
        id: 'producer-default',
        type: 'producer',
        label: 'Telemetry Source',
        sublabel: 'Publishes Events',
        x: 140,
        y: 200,
        radius: 28
      });
      links.push({ from: 'producer-default', to: nodes.find(n => n.type === 'channel')?.id || brokerNode.id });
    }
    if (subCount === 0) {
      nodes.push({
        id: 'consumer-default',
        type: 'consumer',
        label: 'Worker Pool',
        sublabel: 'Consumes Events',
        x: 820,
        y: 200,
        radius: 28,
        lag: 0
      });
      links.push({ from: nodes.find(n => n.type === 'channel')?.id || brokerNode.id, to: 'consumer-default' });
    }

    return { nodes, links, protocol, info: spec.info || {} };
  }

  return {
    TEMPLATES,
    parseYaml,
    lintAsyncAPI,
    buildTopologyGraph
  };
})();

// Export globally
if (typeof window !== 'undefined') {
  window.AsyncAPIEngine = AsyncAPIEngine;
}
