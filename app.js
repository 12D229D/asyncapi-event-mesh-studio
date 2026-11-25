/**
 * Main Controller for AsyncAPI 3.0 Event Mesh Studio
 * Coordinates Canvas simulator, AsyncAPI editor, Schema Registry guard and SQLite persistence
 */

document.addEventListener('DOMContentLoaded', () => {
  // Global State
  let currentArchKey = 'ecommerce-kafka';
  let currentTemplate = AsyncAPIEngine.TEMPLATES[currentArchKey];
  let liveEvents = [];
  let isSimPlaying = true;
  let activeSqlTable = 'async_contracts';

  // DOM Elements
  const selectArchitecture = document.getElementById('selectArchitecture');
  const headerProtocolTag = document.getElementById('headerProtocolTag');
  const btnExportYaml = document.getElementById('btnExportYaml');
  const btnExportSql = document.getElementById('btnExportSql');
  const btnDownloadSqlFromTab = document.getElementById('btnDownloadSqlFromTab');

  // Simulator Controls
  const btnToggleSim = document.getElementById('btnToggleSim');
  const textSimState = document.getElementById('textSimState');
  const iconSimState = document.getElementById('iconSimState');
  const btnStepSim = document.getElementById('btnStepSim');
  const btnResetMetrics = document.getElementById('btnResetMetrics');
  const sliderThroughput = document.getElementById('sliderThroughput');
  const valThroughput = document.getElementById('valThroughput');
  const sliderFailureRate = document.getElementById('sliderFailureRate');
  const valFailureRate = document.getElementById('valFailureRate');
  const sliderConsumerLag = document.getElementById('sliderConsumerLag');
  const valConsumerLag = document.getElementById('valConsumerLag');
  const btnResetCanvasView = document.getElementById('btnResetCanvasView');

  // HUD
  const hudThroughput = document.getElementById('hudThroughput');
  const hudTotalMessages = document.getElementById('hudTotalMessages');
  const hudConsumerLag = document.getElementById('hudConsumerLag');
  const hudDlqCount = document.getElementById('hudDlqCount');

  // Tabs
  const tabButtons = document.querySelectorAll('.tab-btn');
  const tabContents = document.querySelectorAll('.tab-content');

  // Inspector
  const inspectorTitle = document.getElementById('inspectorTitle');
  const inspectorTypeBadge = document.getElementById('inspectorTypeBadge');
  const inspNodeName = document.getElementById('inspNodeName');
  const inspNodeAddress = document.getElementById('inspNodeAddress');
  const inspNodeSemantics = document.getElementById('inspNodeSemantics');
  const inspGaugeFill = document.getElementById('inspGaugeFill');
  const inspGaugeLabel = document.getElementById('inspGaugeLabel');
  const inspSchemaSnippet = document.getElementById('inspSchemaSnippet');
  const btnTriggerNodeEvent = document.getElementById('btnTriggerNodeEvent');

  // Editor & Linter
  const yamlEditorTextarea = document.getElementById('yamlEditorTextarea');
  const btnValidateEditor = document.getElementById('btnValidateEditor');
  const btnCopyYaml = document.getElementById('btnCopyYaml');
  const linterIndicator = document.getElementById('linterIndicator');
  const linterStatusText = document.getElementById('linterStatusText');
  const linterIssuesList = document.getElementById('linterIssuesList');
  const structureTreeList = document.getElementById('structureTreeList');

  // Schema Guard
  const selectCompatibilityMode = document.getElementById('selectCompatibilityMode');
  const btnRunAudit = document.getElementById('btnRunAudit');
  const auditVerdictBox = document.getElementById('auditVerdictBox');
  const auditVerdictText = document.getElementById('auditVerdictText');
  const auditSummaryText = document.getElementById('auditSummaryText');
  const diffResultsList = document.getElementById('diffResultsList');
  const schemaV1Editor = document.getElementById('schemaV1Editor');
  const schemaV2Editor = document.getElementById('schemaV2Editor');

  // Event Stream Table
  const streamTableBody = document.getElementById('streamTableBody');
  const streamCounterBadge = document.getElementById('streamCounterBadge');
  const streamSearchInput = document.getElementById('streamSearchInput');
  const btnClearStream = document.getElementById('btnClearStream');

  // Modals
  const modalDispatchEvent = document.getElementById('modalDispatchEvent');
  const btnOpenDispatchModal = document.getElementById('btnOpenDispatchModal');
  const btnCloseDispatchModal = document.getElementById('btnCloseDispatchModal');
  const btnCancelDispatch = document.getElementById('btnCancelDispatch');
  const btnConfirmDispatch = document.getElementById('btnConfirmDispatch');
  const inputMsgChannel = document.getElementById('inputMsgChannel');
  const inputMsgKey = document.getElementById('inputMsgKey');
  const inputMsgPayload = document.getElementById('inputMsgPayload');

  const modalInspectPayload = document.getElementById('modalInspectPayload');
  const btnCloseInspectModal = document.getElementById('btnCloseInspectModal');
  const btnCloseInspectBtn = document.getElementById('btnCloseInspectBtn');
  const inspectMetaContent = document.getElementById('inspectMetaContent');
  const inspectPayloadContent = document.getElementById('inspectPayloadContent');

  // SQLite View
  const sqlTableItems = document.querySelectorAll('.sql-table-item');
  const currentSqlTableName = document.getElementById('currentSqlTableName');
  const sqlTableViewer = document.getElementById('sqlTableViewer');
  const btnResetDatabase = document.getElementById('btnResetDatabase');
  const countContracts = document.getElementById('countContracts');
  const countSchemas = document.getElementById('countSchemas');
  const countAudit = document.getElementById('countAudit');
  const countMetrics = document.getElementById('countMetrics');

  // Toast Container
  const toastContainer = document.getElementById('toastContainer');

  // 1. Initialize SQLite Store
  DatabaseSQLite.init();

  // 2. Initialize Canvas Simulator
  const canvasEl = document.getElementById('eventMeshCanvas');
  BrokerSimulator.init(canvasEl, {
    onNodeSelect: handleNodeSelected,
    onMessageAck: handleMessageAck
  });

  // 3. Load initial Architecture
  loadArchitecture(currentArchKey);

  // Setup periodic Telemetry sync
  setInterval(syncTelemetryHUD, 200);

  // --- Functions ---

  function loadArchitecture(key) {
    currentArchKey = key;
    currentTemplate = AsyncAPIEngine.TEMPLATES[key];
    if (!currentTemplate) return;

    // Update Header Protocol Badge
    updateProtocolTag(currentTemplate.protocol);

    // Populate YAML Editor
    yamlEditorTextarea.value = currentTemplate.yaml;

    // Populate Schema Registry editors
    schemaV1Editor.value = JSON.stringify(currentTemplate.schemaV1, null, 2);
    schemaV2Editor.value = JSON.stringify(currentTemplate.schemaV2, null, 2);

    // Run Schema Audit
    runSchemaAudit();

    // Parse YAML and rebuild Topology
    applyYamlToMesh(currentTemplate.yaml);

    // Save to SQLite
    DatabaseSQLite.saveContract({
      id: currentTemplate.id,
      title: currentTemplate.title,
      version: currentTemplate.version,
      protocol: currentTemplate.protocol,
      description: currentTemplate.description,
      yaml_content: currentTemplate.yaml
    });

    updateSqlCounters();
    showToast(`Arquitectura '${currentTemplate.title}' cargada.`, 'info');
  }

  function updateProtocolTag(protocol) {
    headerProtocolTag.className = `badge-protocol badge-${protocol}`;
    const labels = {
      kafka: 'Apache Kafka',
      mqtt: 'MQTT v5',
      ws: 'WebSockets Duplex',
      amqp: 'RabbitMQ (AMQP)'
    };
    headerProtocolTag.textContent = labels[protocol] || protocol.toUpperCase();
  }

  function applyYamlToMesh(yamlText) {
    try {
      const parsedSpec = AsyncAPIEngine.parseYaml(yamlText);
      const lintResult = AsyncAPIEngine.lintAsyncAPI(parsedSpec);
      updateLinterUI(lintResult);

      const graph = AsyncAPIEngine.buildTopologyGraph(parsedSpec, currentTemplate);
      BrokerSimulator.setTopology(graph);

      // Update Structure Tree
      updateStructureTree(parsedSpec);

      // Default select first producer or broker
      const firstNode = graph.nodes.find(n => n.type === 'producer') || graph.nodes[0];
      if (firstNode) handleNodeSelected(firstNode);

    } catch (err) {
      console.error('Error applying YAML to mesh:', err);
      updateLinterUI({
        isValid: false,
        issues: [{ type: 'error', message: 'Error de sintaxis al procesar YAML: ' + err.message }]
      });
    }
  }

  function updateLinterUI(lintResult) {
    if (lintResult.isValid) {
      linterIndicator.className = 'status-indicator status-valid';
      linterStatusText.textContent = 'Contrato Válido (AsyncAPI 3.0.0)';
    } else {
      linterIndicator.className = 'status-indicator status-invalid';
      linterStatusText.textContent = 'Inconsistencias Detectadas';
    }

    linterIssuesList.innerHTML = '';
    lintResult.issues.forEach(issue => {
      const li = document.createElement('li');
      li.className = `lint-item ${issue.type}`;
      li.textContent = issue.message;
      linterIssuesList.appendChild(li);
    });
  }

  function updateStructureTree(spec) {
    const serversCount = spec.servers ? Object.keys(spec.servers).length : 0;
    const channelsCount = spec.channels ? Object.keys(spec.channels).length : 0;
    const operationsCount = spec.operations ? Object.keys(spec.operations).length : 0;
    const messagesCount = (spec.components && spec.components.messages) ? Object.keys(spec.components.messages).length : 0;

    structureTreeList.innerHTML = `
      <li class="tree-node"><span>servers:</span><span style="color: var(--accent-amber);">${serversCount} cluster(s)</span></li>
      <li class="tree-node"><span>channels:</span><span style="color: var(--accent-emerald);">${channelsCount} tópico(s)</span></li>
      <li class="tree-node"><span>operations:</span><span style="color: var(--accent-cyan);">${operationsCount} operacione(s)</span></li>
      <li class="tree-node"><span>components.messages:</span><span style="color: var(--text-main);">${messagesCount} esquema(s)</span></li>
    `;
  }

  function handleNodeSelected(node) {
    inspectorTitle.textContent = node.label || 'Componente';
    inspectorTypeBadge.textContent = (node.type || 'NODE').toUpperCase();

    inspNodeName.textContent = node.label;
    inspNodeAddress.textContent = node.sublabel || 'Sin dirección específica';

    if (node.type === 'broker') {
      inspectorTypeBadge.className = 'status-badge status-ack';
      inspNodeSemantics.textContent = `${node.protocol.toUpperCase()} High-Availability Broker (Replicas: 3)`;
    } else if (node.type === 'producer') {
      inspectorTypeBadge.className = 'status-badge status-retry';
      inspNodeSemantics.textContent = 'At-Least-Once (Idempotent Publisher)';
    } else if (node.type === 'consumer') {
      inspectorTypeBadge.className = 'status-badge status-ack';
      inspNodeSemantics.textContent = `Consumer Group (Auto-Commit: False, Lag: ${node.lag || 0})`;
    } else if (node.type === 'dlq' || node.type === 'dlq-channel') {
      inspectorTypeBadge.className = 'status-badge status-dlq';
      inspNodeSemantics.textContent = 'Poison Pill Quarantine (TTL: 14 days)';
    } else {
      inspectorTypeBadge.className = 'status-badge status-ack';
      inspNodeSemantics.textContent = 'Partitioned FIFO Log / Pub-Sub Topic';
    }

    const lagPercent = Math.min(100, Math.max(10, (node.lag || 0) * 1.6));
    inspGaugeFill.style.width = `${lagPercent}%`;
    inspGaugeLabel.textContent = `Carga observada: ${lagPercent.toFixed(0)}% (Lag: ${node.lag || 0})`;

    if (node.data && node.data.messages) {
      inspSchemaSnippet.textContent = JSON.stringify(node.data.messages, null, 2);
    } else {
      inspSchemaSnippet.textContent = `$ref: '#/components/messages/${node.label.replace(/\s+/g, '')}'`;
    }
  }

  function handleMessageAck(msgData, status) {
    const eventRecord = {
      id: msgData.id,
      channel: msgData.channel,
      partition: msgData.partition,
      key: msgData.key,
      payload: msgData.payload || generateSyntheticPayload(msgData.channel),
      status: status,
      timestamp: new Date().toLocaleTimeString(),
      latency: (8 + Math.random() * 15).toFixed(1)
    };

    liveEvents.unshift(eventRecord);
    if (liveEvents.length > 80) liveEvents.pop();

    // Persist in SQLite
    DatabaseSQLite.logEvent(eventRecord);

    // Update Stream UI
    renderStreamTable();
    updateSqlCounters();
  }

  function generateSyntheticPayload(channel) {
    if (currentArchKey === 'iot-fleet-mqtt') {
      return {
        vin: '1HGCR2F83HA02' + Math.floor(1000 + Math.random() * 9000),
        batteryLevel: Number((65 + Math.random() * 30).toFixed(1)),
        speedKmh: Math.floor(40 + Math.random() * 80),
        motorTempC: Number((42 + Math.random() * 20).toFixed(1)),
        timestamp: Date.now()
      };
    } else if (currentArchKey === 'realtime-chat-ws') {
      return {
        messageId: 'chat_' + Math.random().toString(36).substring(2, 9),
        roomId: 'room_dev_mesh',
        senderId: 'user_' + Math.floor(100 + Math.random() * 900),
        content: 'Syncing distributed event state over WebSockets...',
        timestamp: Date.now()
      };
    } else {
      return {
        orderId: 'ord_' + Math.floor(10000 + Math.random() * 90000),
        customerId: 'cust_' + Math.floor(1000 + Math.random() * 9000),
        amount: Number((25 + Math.random() * 250).toFixed(2)),
        currency: 'USD',
        status: 'PAID'
      };
    }
  }

  function renderStreamTable() {
    const query = streamSearchInput.value.toLowerCase().trim();
    const filtered = liveEvents.filter(e => {
      if (!query) return true;
      return (
        e.id.toLowerCase().includes(query) ||
        e.channel.toLowerCase().includes(query) ||
        (e.key && e.key.toLowerCase().includes(query)) ||
        e.status.toLowerCase().includes(query)
      );
    });

    streamCounterBadge.textContent = `${liveEvents.length} eventos`;

    streamTableBody.innerHTML = '';
    filtered.slice(0, 30).forEach(ev => {
      const tr = document.createElement('tr');
      const isDlq = ev.status === 'DEAD_LETTER_QUEUE';
      const badgeClass = isDlq ? 'status-dlq' : 'status-ack';

      tr.innerHTML = `
        <td class="mono" style="font-weight: 700; color: ${isDlq ? 'var(--accent-rose)' : 'var(--accent-amber)'};">${ev.id}</td>
        <td class="mono" style="color: var(--text-muted);">${ev.timestamp}</td>
        <td><span class="status-badge" style="background: hsla(217, 33%, 30%, 0.4);">${ev.channel}</span></td>
        <td class="mono">p${ev.partition} | ${ev.key}</td>
        <td><span class="status-badge ${badgeClass}">${ev.status}</span></td>
        <td class="mono" style="color: var(--accent-emerald);">${ev.latency} ms</td>
        <td class="mono" style="max-width: 280px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: var(--text-muted);">
          ${JSON.stringify(ev.payload)}
        </td>
      `;

      tr.addEventListener('click', () => {
        openInspectModal(ev);
      });

      streamTableBody.appendChild(tr);
    });
  }

  function syncTelemetryHUD() {
    const m = BrokerSimulator.getMetrics();
    hudThroughput.textContent = `${m.throughput.toFixed(1)} msg/s`;
    hudTotalMessages.textContent = m.totalMessages.toLocaleString();
    hudConsumerLag.textContent = `${m.consumerLag} msg`;
    hudDlqCount.textContent = m.dlqCount.toLocaleString();

    // Record periodic metrics in SQLite
    if (m.totalMessages % 20 === 0 && m.totalMessages > 0) {
      DatabaseSQLite.recordMetrics({ ...m, protocol: currentTemplate.protocol });
      updateSqlCounters();
    }
  }

  function runSchemaAudit() {
    let schemaV1, schemaV2;
    try {
      schemaV1 = JSON.parse(schemaV1Editor.value);
      schemaV2 = JSON.parse(schemaV2Editor.value);
    } catch (e) {
      showToast('Error de sintaxis JSON en Schema v1 o v2: ' + e.message, 'error');
      return;
    }

    const mode = selectCompatibilityMode.value;
    const audit = SchemaCompatibility.evaluate(schemaV1, schemaV2, mode);

    if (audit.isCompatible) {
      auditVerdictBox.className = 'audit-verdict verdict-compatible';
      auditVerdictText.textContent = `COMPATIBLE CON MODO ${mode} (Safe to Deploy)`;
    } else {
      auditVerdictBox.className = 'audit-verdict verdict-breaking';
      auditVerdictText.textContent = `RUPTURA DETECTADA (Breaking Changes)`;
    }

    auditSummaryText.textContent = audit.summary;

    diffResultsList.innerHTML = '';
    audit.issues.forEach(issue => {
      const li = document.createElement('li');
      li.className = `diff-item ${issue.type}`;
      li.innerHTML = `
        <div style="display: flex; flex-direction: column; gap: 0.2rem;">
          <span style="font-weight: 700; font-family: var(--font-mono); font-size: 0.8rem; color: ${issue.type === 'breaking' ? 'var(--accent-rose)' : 'var(--accent-emerald)'};">
            ${issue.path}
          </span>
          <span style="font-size: 0.78rem; color: var(--text-main);">${issue.message}</span>
          ${issue.remedy ? `<span style="font-size: 0.72rem; color: var(--accent-amber);">Sugerencia: ${issue.remedy}</span>` : ''}
        </div>
        <span class="status-badge ${issue.type === 'breaking' ? 'status-dlq' : 'status-ack'}">
          ${issue.type.toUpperCase()}
        </span>
      `;
      diffResultsList.appendChild(li);
    });
  }

  function openInspectModal(eventRecord) {
    inspectMetaContent.textContent = JSON.stringify({
      id: eventRecord.id,
      channel: eventRecord.channel,
      partition: eventRecord.partition,
      key: eventRecord.key,
      status: eventRecord.status,
      latency_ms: eventRecord.latency,
      timestamp: eventRecord.timestamp
    }, null, 2);

    inspectPayloadContent.textContent = JSON.stringify(eventRecord.payload, null, 2);
    modalInspectPayload.classList.add('active');
  }

  function updateSqlCounters() {
    countContracts.textContent = DatabaseSQLite.getTableData('async_contracts').length;
    countSchemas.textContent = DatabaseSQLite.getTableData('event_schemas').length;
    countAudit.textContent = DatabaseSQLite.getTableData('event_audit_log').length;
    countMetrics.textContent = DatabaseSQLite.getTableData('broker_metrics').length;

    renderSqlTableView(activeSqlTable);
  }

  function renderSqlTableView(tableName) {
    activeSqlTable = tableName;
    currentSqlTableName.textContent = tableName;
    const rows = DatabaseSQLite.getTableData(tableName);
    sqlTableViewer.textContent = `-- Tabla: ${tableName} (${rows.length} registros)\n` + JSON.stringify(rows, null, 2);
  }

  function showToast(message, type = 'info') {
    const toast = document.createElement('div');
    toast.className = 'toast';
    toast.innerHTML = `
      <span style="color: ${type === 'error' ? 'var(--accent-rose)' : 'var(--accent-amber)'}; font-weight: 800;">●</span>
      <span>${message}</span>
    `;
    toastContainer.appendChild(toast);
    setTimeout(() => {
      toast.style.opacity = '0';
      toast.style.transform = 'translateY(10px)';
      toast.style.transition = 'all 0.3s ease';
      setTimeout(() => toast.remove(), 300);
    }, 3200);
  }

  // --- Event Listeners ---

  // Architecture Selector
  selectArchitecture.addEventListener('change', (e) => {
    loadArchitecture(e.target.value);
  });

  // Play/Pause Simulator
  btnToggleSim.addEventListener('click', () => {
    isSimPlaying = !isSimPlaying;
    if (isSimPlaying) {
      BrokerSimulator.play();
      textSimState.textContent = 'Pausar';
      iconSimState.innerHTML = '<rect x="6" y="4" width="4" height="16"/><rect x="14" y="4" width="4" height="16"/>';
      btnToggleSim.className = 'btn btn-emerald';
    } else {
      BrokerSimulator.pause();
      textSimState.textContent = 'Reanudar';
      iconSimState.innerHTML = '<polygon points="5,3 19,12 5,21"/>';
      btnToggleSim.className = 'btn btn-primary';
    }
  });

  // Step
  btnStepSim.addEventListener('click', () => {
    BrokerSimulator.step();
  });

  // Reset Metrics
  btnResetMetrics.addEventListener('click', () => {
    BrokerSimulator.resetMetrics();
    liveEvents = [];
    renderStreamTable();
    syncTelemetryHUD();
    showToast('Métricas del broker restablecidas a cero.', 'info');
  });

  // Sliders
  sliderThroughput.addEventListener('input', (e) => {
    valThroughput.textContent = e.target.value;
    BrokerSimulator.setSimConfig({ throughput: e.target.value });
  });

  sliderFailureRate.addEventListener('input', (e) => {
    valFailureRate.textContent = e.target.value + '%';
    BrokerSimulator.setSimConfig({ failureRate: e.target.value });
  });

  sliderConsumerLag.addEventListener('input', (e) => {
    valConsumerLag.textContent = e.target.value;
    BrokerSimulator.setSimConfig({ consumerLag: e.target.value });
  });

  btnResetCanvasView.addEventListener('click', () => {
    BrokerSimulator.resetView();
  });

  // Tabs Switching
  tabButtons.forEach(btn => {
    btn.addEventListener('click', () => {
      const targetTab = btn.getAttribute('data-tab');
      tabButtons.forEach(b => b.classList.remove('active'));
      tabContents.forEach(c => c.classList.remove('active'));

      btn.classList.add('active');
      document.getElementById(targetTab).classList.add('active');

      if (targetTab === 'tab-database') {
        renderSqlTableView(activeSqlTable);
      }
    });
  });

  // Trigger Node Traffic
  btnTriggerNodeEvent.addEventListener('click', () => {
    BrokerSimulator.spawnCustomMessage();
    showToast('Ráfaga de evento generada desde el nodo seleccionado.', 'info');
  });

  // Validate YAML Editor Button
  btnValidateEditor.addEventListener('click', () => {
    applyYamlToMesh(yamlEditorTextarea.value);
    showToast('Especificación AsyncAPI 3.0 validada y proyectada al lienzo.', 'info');
  });

  // Copy YAML
  btnCopyYaml.addEventListener('click', () => {
    navigator.clipboard.writeText(yamlEditorTextarea.value);
    showToast('YAML de AsyncAPI 3.0 copiado al portapapeles.', 'info');
  });

  // Export YAML File
  btnExportYaml.addEventListener('click', () => {
    const blob = new Blob([yamlEditorTextarea.value], { type: 'text/yaml;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `asyncapi-${currentArchKey}.yaml`;
    a.click();
    URL.revokeObjectURL(url);
    showToast('Archivo asyncapi.yaml descargado.', 'info');
  });

  // Export SQLite Dump File (.sql)
  const downloadSqlDump = () => {
    const sqlContent = DatabaseSQLite.exportSqlDump();
    const blob = new Blob([sqlContent], { type: 'text/plain;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `asyncapi-event-mesh-${currentArchKey}.sql`;
    a.click();
    URL.revokeObjectURL(url);
    showToast('Volcado relacional SQLite 3.45+ (.sql) descargado con éxito.', 'info');
  };
  btnExportSql.addEventListener('click', downloadSqlDump);
  btnDownloadSqlFromTab.addEventListener('click', downloadSqlDump);

  // Schema Audit Button
  btnRunAudit.addEventListener('click', runSchemaAudit);
  selectCompatibilityMode.addEventListener('change', runSchemaAudit);

  // Stream Search & Clear
  streamSearchInput.addEventListener('input', renderStreamTable);
  btnClearStream.addEventListener('click', () => {
    liveEvents = [];
    renderStreamTable();
    showToast('Registro de eventos en vivo despejado.', 'info');
  });

  // Custom Event Dispatch Modal
  btnOpenDispatchModal.addEventListener('click', () => {
    modalDispatchEvent.classList.add('active');
  });
  btnCloseDispatchModal.addEventListener('click', () => {
    modalDispatchEvent.classList.remove('active');
  });
  btnCancelDispatch.addEventListener('click', () => {
    modalDispatchEvent.classList.remove('active');
  });

  btnConfirmDispatch.addEventListener('click', () => {
    let payload;
    try {
      payload = JSON.parse(inputMsgPayload.value);
    } catch (e) {
      showToast('Payload JSON no válido: ' + e.message, 'error');
      return;
    }

    const customMsg = {
      id: 'custom-' + Math.random().toString(36).substring(2, 9),
      channel: inputMsgChannel.value.trim() || 'retail.orders.v1',
      key: inputMsgKey.value.trim() || 'usr_manual',
      partition: Math.floor(Math.random() * 3),
      payload: payload,
      willFail: false,
      protocol: currentTemplate.protocol
    };

    BrokerSimulator.spawnCustomMessage(customMsg);
    modalDispatchEvent.classList.remove('active');
    showToast('Evento personalizado inyectado en el broker con éxito.', 'info');
  });

  // Inspect Modal Close
  btnCloseInspectModal.addEventListener('click', () => {
    modalInspectPayload.classList.remove('active');
  });
  btnCloseInspectBtn.addEventListener('click', () => {
    modalInspectPayload.classList.remove('active');
  });

  // SQLite Sidebar Tables
  sqlTableItems.forEach(item => {
    item.addEventListener('click', () => {
      sqlTableItems.forEach(i => i.classList.remove('active'));
      item.classList.add('active');
      renderSqlTableView(item.getAttribute('data-sqltable'));
    });
  });

  btnResetDatabase.addEventListener('click', () => {
    DatabaseSQLite.seedDefaults();
    updateSqlCounters();
    showToast('Base de datos SQLite local restablecida a valores semilla.', 'info');
  });
});
