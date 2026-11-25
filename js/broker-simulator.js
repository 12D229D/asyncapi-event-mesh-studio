/**
 * Broker Simulator 2D Canvas Engine
 * Renders interactive EDA topology, partitions, particle streams, backpressure & DLQ
 */

const BrokerSimulator = (() => {
  let canvas, ctx;
  let isRunning = true;
  let animFrameId = null;

  // Camera & Interaction State
  let camera = { x: 0, y: 0, zoom: 1 };
  let isDraggingNode = false;
  let isPanning = false;
  let draggedNode = null;
  let hoveredNode = null;
  let selectedNode = null;
  let mouseStart = { x: 0, y: 0 };

  // Graph topology
  let nodes = [];
  let links = [];
  let particles = [];
  let currentProtocol = 'kafka';

  // Metrics
  let metrics = {
    throughput: 12, // msg/s
    failureRate: 5,  // %
    consumerLag: 0,
    totalMessages: 0,
    dlqCount: 0,
    avgLatencyMs: 14.2
  };

  // Event handlers
  let onNodeSelectCallback = null;
  let onMessageAckCallback = null;
  let lastParticleSpawnTime = 0;

  function init(canvasElement, options = {}) {
    canvas = canvasElement;
    ctx = canvas.getContext('2d');

    resize();
    window.addEventListener('resize', resize);

    // Mouse & Touch events
    canvas.addEventListener('mousedown', handleMouseDown);
    canvas.addEventListener('mousemove', handleMouseMove);
    canvas.addEventListener('mouseup', handleMouseUp);
    canvas.addEventListener('wheel', handleWheel, { passive: false });

    if (options.onNodeSelect) onNodeSelectCallback = options.onNodeSelect;
    if (options.onMessageAck) onMessageAckCallback = options.onMessageAck;

    startLoop();
  }

  function resize() {
    if (!canvas) return;
    const rect = canvas.getBoundingClientRect();
    const dpr = window.devicePixelRatio || 1;
    canvas.width = rect.width * dpr;
    canvas.height = rect.height * dpr;
    ctx.scale(dpr, dpr);
  }

  function setTopology(graphData) {
    nodes = graphData.nodes || [];
    links = graphData.links || [];
    currentProtocol = graphData.protocol || 'kafka';
    particles = [];
    selectedNode = null;
  }

  function setSimConfig(newConfig) {
    if (newConfig.throughput !== undefined) metrics.throughput = Number(newConfig.throughput);
    if (newConfig.failureRate !== undefined) metrics.failureRate = Number(newConfig.failureRate);
    if (newConfig.consumerLag !== undefined) metrics.consumerLag = Number(newConfig.consumerLag);
  }

  function getMetrics() {
    return { ...metrics };
  }

  function resetMetrics() {
    metrics.totalMessages = 0;
    metrics.dlqCount = 0;
    particles = [];
    nodes.forEach(n => {
      if (n.type === 'dlq') n.piledCount = 0;
      if (n.type === 'consumer') n.lag = 0;
    });
  }

  function startLoop() {
    if (animFrameId) cancelAnimationFrame(animFrameId);
    let lastTime = performance.now();

    function loop(time) {
      const dt = (time - lastTime) / 1000;
      lastTime = time;

      if (isRunning) {
        update(time, dt);
      }
      render();
      animFrameId = requestAnimationFrame(loop);
    }
    animFrameId = requestAnimationFrame(loop);
  }

  function play() { isRunning = true; }
  function pause() { isRunning = false; }
  function step() {
    update(performance.now(), 0.05);
    render();
  }

  /**
   * Spawns synthetic particles based on throughput and links
   */
  function update(time, dt) {
    const spawnInterval = 1000 / Math.max(1, metrics.throughput);
    if (time - lastParticleSpawnTime > spawnInterval) {
      lastParticleSpawnTime = time;
      spawnMessageParticle();
    }

    // Update particles
    for (let i = particles.length - 1; i >= 0; i--) {
      const p = particles[i];
      p.progress += p.speed * dt;

      if (p.progress >= 1) {
        handleParticleArrival(p);
        particles.splice(i, 1);
      }
    }

    // Update node states (lag, partition fills)
    nodes.forEach(n => {
      if (n.type === 'consumer') {
        n.lag = Math.round(metrics.consumerLag * (0.8 + Math.sin(time / 600) * 0.2));
      }
    });
  }

  function spawnMessageParticle(customPayload = null) {
    const producers = nodes.filter(n => n.type === 'producer');
    if (producers.length === 0) return;

    const producer = producers[Math.floor(Math.random() * producers.length)];
    const outLinks = links.filter(l => l.from === producer.id);
    if (outLinks.length === 0) return;

    const link = outLinks[Math.floor(Math.random() * outLinks.length)];
    const targetNode = nodes.find(n => n.id === link.to);
    if (!targetNode) return;

    metrics.totalMessages++;

    // Determine if message will fail / go to DLQ
    const willFail = Math.random() * 100 < metrics.failureRate;
    const partitionId = Math.floor(Math.random() * 3);

    const messageData = customPayload || {
      id: 'msg-' + Math.random().toString(36).substring(2, 9),
      key: 'usr_' + Math.floor(1000 + Math.random() * 9000),
      channel: targetNode.label || 'retail.orders.v1',
      partition: partitionId,
      protocol: currentProtocol,
      willFail,
      timestamp: new Date().toISOString()
    };

    particles.push({
      fromNode: producer,
      toNode: targetNode,
      progress: 0,
      speed: 0.8 + Math.random() * 0.4,
      willFail,
      data: messageData,
      color: getProtocolParticleColor(currentProtocol, willFail)
    });
  }

  function getProtocolParticleColor(protocol, willFail) {
    if (willFail) return 'hsl(354, 78%, 56%)'; // Rose/Red for DLQ bound
    switch (protocol) {
      case 'kafka': return 'hsl(38, 95%, 52%)'; // Amber
      case 'mqtt': return 'hsl(150, 72%, 46%)'; // Emerald
      case 'ws': return 'hsl(190, 95%, 50%)';   // Cyan
      case 'amqp': return 'hsl(265, 82%, 66%)'; // Purple
      default: return 'hsl(210, 100%, 60%)';
    }
  }

  function handleParticleArrival(p) {
    const reachedNode = p.toNode;

    // If reached channel or broker, forward to consumers or DLQ
    if (reachedNode.type === 'channel' || reachedNode.type === 'broker') {
      if (p.willFail) {
        // Forward to DLQ
        const dlqNode = nodes.find(n => n.type === 'dlq');
        if (dlqNode) {
          particles.push({
            fromNode: reachedNode,
            toNode: dlqNode,
            progress: 0,
            speed: 1.1,
            willFail: true,
            data: p.data,
            color: 'hsl(354, 78%, 56%)'
          });
        }
      } else {
        // Forward to consumer
        const nextLinks = links.filter(l => l.from === reachedNode.id);
        const validLinks = nextLinks.filter(l => {
          const dest = nodes.find(n => n.id === l.to);
          return dest && dest.type === 'consumer';
        });

        if (validLinks.length > 0) {
          const nextLink = validLinks[Math.floor(Math.random() * validLinks.length)];
          const consumerNode = nodes.find(n => n.id === nextLink.to);
          if (consumerNode) {
            particles.push({
              fromNode: reachedNode,
              toNode: consumerNode,
              progress: 0,
              speed: 1.0,
              willFail: false,
              data: p.data,
              color: getProtocolParticleColor(currentProtocol, false)
            });
          }
        }
      }
    } else if (reachedNode.type === 'dlq') {
      metrics.dlqCount++;
      reachedNode.piledCount = (reachedNode.piledCount || 0) + 1;
      if (onMessageAckCallback) {
        onMessageAckCallback(p.data, 'DEAD_LETTER_QUEUE');
      }
    } else if (reachedNode.type === 'consumer') {
      if (onMessageAckCallback) {
        onMessageAckCallback(p.data, 'ACKNOWLEDGED');
      }
    }
  }

  /**
   * Main Render Loop
   */
  function render() {
    if (!canvas || !ctx) return;
    const rect = canvas.getBoundingClientRect();
    const w = rect.width;
    const h = rect.height;

    ctx.clearRect(0, 0, w, h);
    ctx.save();

    // Apply Camera transform
    ctx.translate(camera.x, camera.y);
    ctx.scale(camera.zoom, camera.zoom);

    // 1. Draw Links
    links.forEach(link => {
      const src = nodes.find(n => n.id === link.from);
      const dst = nodes.find(n => n.id === link.to);
      if (!src || !dst) return;

      ctx.beginPath();
      ctx.moveTo(src.x, src.y);
      // Bezier curve for aesthetics
      const midX = (src.x + dst.x) / 2;
      const midY = (src.y + dst.y) / 2 + (src.y === dst.y ? 20 : 0);
      ctx.quadraticCurveTo(midX, midY, dst.x, dst.y);

      ctx.strokeStyle = link.color || 'hsla(217, 33%, 38%, 0.4)';
      ctx.lineWidth = 2;
      ctx.setLineDash(link.label === 'reroutes' ? [4, 4] : []);
      ctx.stroke();
      ctx.setLineDash([]);
    });

    // 2. Draw Message Particles
    particles.forEach(p => {
      const src = p.fromNode;
      const dst = p.toNode;
      const midX = (src.x + dst.x) / 2;
      const midY = (src.y + dst.y) / 2 + (src.y === dst.y ? 20 : 0);

      // Quadratic interpolation
      const t = p.progress;
      const px = (1 - t) * (1 - t) * src.x + 2 * (1 - t) * t * midX + t * t * dst.x;
      const py = (1 - t) * (1 - t) * src.y + 2 * (1 - t) * t * midY + t * t * dst.y;

      ctx.beginPath();
      ctx.arc(px, py, 4.5, 0, Math.PI * 2);
      ctx.fillStyle = p.color;
      ctx.shadowColor = p.color;
      ctx.shadowBlur = 10;
      ctx.fill();
      ctx.shadowBlur = 0;
    });

    // 3. Draw Nodes
    nodes.forEach(node => {
      renderNode(node);
    });

    ctx.restore();
  }

  function renderNode(node) {
    const isSelected = selectedNode && selectedNode.id === node.id;
    const isHovered = hoveredNode && hoveredNode.id === node.id;

    ctx.save();
    ctx.translate(node.x, node.y);

    // Outer glow for selected/hovered
    if (isSelected || isHovered) {
      ctx.beginPath();
      ctx.arc(0, 0, node.radius + 8, 0, Math.PI * 2);
      ctx.fillStyle = isSelected ? 'hsla(38, 95%, 52%, 0.25)' : 'hsla(210, 100%, 60%, 0.15)';
      ctx.fill();
    }

    // Node body
    ctx.beginPath();
    ctx.arc(0, 0, node.radius, 0, Math.PI * 2);

    let fillColor = 'hsl(220, 39%, 13%)';
    let strokeColor = 'hsla(217, 33%, 38%, 0.6)';

    if (node.type === 'broker') {
      fillColor = 'hsl(220, 42%, 16%)';
      strokeColor = currentProtocol === 'kafka' ? 'hsl(38, 95%, 52%)' :
                    currentProtocol === 'mqtt' ? 'hsl(150, 72%, 46%)' : 'hsl(190, 95%, 50%)';
    } else if (node.type === 'producer') {
      fillColor = 'hsla(38, 95%, 52%, 0.12)';
      strokeColor = 'hsl(38, 95%, 52%)';
    } else if (node.type === 'consumer') {
      fillColor = 'hsla(190, 95%, 50%, 0.12)';
      strokeColor = 'hsl(190, 95%, 50%)';
    } else if (node.type === 'channel') {
      fillColor = 'hsla(150, 72%, 46%, 0.12)';
      strokeColor = 'hsl(150, 72%, 46%)';
    } else if (node.type === 'dlq' || node.type === 'dlq-channel') {
      fillColor = 'hsla(354, 78%, 56%, 0.18)';
      strokeColor = 'hsl(354, 78%, 56%)';
    }

    ctx.fillStyle = fillColor;
    ctx.fill();
    ctx.lineWidth = isSelected ? 3 : 2;
    ctx.strokeStyle = strokeColor;
    ctx.stroke();

    // Node Icon / Badge inside
    ctx.fillStyle = '#ffffff';
    ctx.font = 'bold 10px JetBrains Mono';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';

    let iconText = 'NODE';
    if (node.type === 'broker') iconText = currentProtocol.toUpperCase();
    else if (node.type === 'producer') iconText = 'PUB';
    else if (node.type === 'consumer') iconText = 'SUB';
    else if (node.type === 'channel') iconText = 'TOPIC';
    else if (node.type === 'dlq') iconText = 'DLQ';
    ctx.fillText(iconText, 0, -4);

    // Sub-metric inside node
    if (node.type === 'dlq' && node.piledCount > 0) {
      ctx.fillStyle = 'hsl(354, 85%, 65%)';
      ctx.font = 'bold 9px JetBrains Mono';
      ctx.fillText(`${node.piledCount}`, 0, 8);
    } else if (node.type === 'consumer' && node.lag > 0) {
      ctx.fillStyle = 'hsl(38, 95%, 60%)';
      ctx.font = 'bold 9px JetBrains Mono';
      ctx.fillText(`Lag:${node.lag}`, 0, 8);
    }

    // Node Text Label below
    ctx.font = '600 11px Inter';
    ctx.fillStyle = 'hsl(210, 40%, 98%)';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'top';
    ctx.fillText(node.label, 0, node.radius + 6);

    // Sublabel
    if (node.sublabel) {
      ctx.font = '400 9.5px Inter';
      ctx.fillStyle = 'hsl(215, 20%, 65%)';
      ctx.fillText(node.sublabel, 0, node.radius + 20);
    }

    // Kafka Partition markers if broker
    if (node.type === 'broker' && currentProtocol === 'kafka') {
      const partCount = node.partitions || 3;
      const partW = 12;
      const startX = -((partCount * (partW + 4)) / 2) + partW / 2;
      for (let p = 0; p < partCount; p++) {
        ctx.fillStyle = 'hsla(38, 95%, 52%, 0.4)';
        ctx.fillRect(startX + p * (partW + 4) - partW / 2, node.radius + 34, partW, 5);
      }
    }

    ctx.restore();
  }

  // --- Mouse / Touch Interactivity ---
  function getMousePos(e) {
    const rect = canvas.getBoundingClientRect();
    const x = (e.clientX - rect.left - camera.x) / camera.zoom;
    const y = (e.clientY - rect.top - camera.y) / camera.zoom;
    return { x, y };
  }

  function getNodeAt(x, y) {
    for (let i = nodes.length - 1; i >= 0; i--) {
      const n = nodes[i];
      const dist = Math.hypot(n.x - x, n.y - y);
      if (dist <= n.radius) return n;
    }
    return null;
  }

  function handleMouseDown(e) {
    const pos = getMousePos(e);
    const node = getNodeAt(pos.x, pos.y);

    if (node) {
      isDraggingNode = true;
      draggedNode = node;
      selectedNode = node;
      if (onNodeSelectCallback) onNodeSelectCallback(node);
    } else {
      isPanning = true;
      mouseStart = { x: e.clientX, y: e.clientY };
    }
  }

  function handleMouseMove(e) {
    const pos = getMousePos(e);

    if (isDraggingNode && draggedNode) {
      draggedNode.x = pos.x;
      draggedNode.y = pos.y;
    } else if (isPanning) {
      camera.x += e.clientX - mouseStart.x;
      camera.y += e.clientY - mouseStart.y;
      mouseStart = { x: e.clientX, y: e.clientY };
    } else {
      hoveredNode = getNodeAt(pos.x, pos.y);
      canvas.style.cursor = hoveredNode ? 'pointer' : 'grab';
    }
  }

  function handleMouseUp() {
    isDraggingNode = false;
    draggedNode = null;
    isPanning = false;
    canvas.style.cursor = 'grab';
  }

  function handleWheel(e) {
    e.preventDefault();
    const zoomFactor = 1.08;
    if (e.deltaY < 0) {
      camera.zoom = Math.min(2.5, camera.zoom * zoomFactor);
    } else {
      camera.zoom = Math.max(0.4, camera.zoom / zoomFactor);
    }
  }

  function resetView() {
    camera = { x: 0, y: 0, zoom: 1 };
  }

  return {
    init,
    setTopology,
    setSimConfig,
    getMetrics,
    resetMetrics,
    play,
    pause,
    step,
    resetView,
    spawnCustomMessage: spawnMessageParticle
  };
})();

if (typeof window !== 'undefined') {
  window.BrokerSimulator = BrokerSimulator;
}
