'use strict';

const http = require('http');
const WebSocket = require('ws');

const PORT = process.env.PORT || 3000;

// HTTP server (sirve para health y para "upgrade" a WebSocket)
const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      ok: true,
      ts: Date.now(),
      clients: wss.clients.size
    }));
    return;
  }

  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Twybox WS core online. Use /health or WebSocket.\n');
});

// WebSocket server
const wss = new WebSocket.Server({ server });

// Memoria mínima (última posición por unidad)
const lastByUnit = new Map();

function safeJsonParse(s) {
  try { return JSON.parse(s); } catch { return null; }
}

function broadcast(obj) {
  const msg = JSON.stringify(obj);
  for (const client of wss.clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(msg);
    }
  }
}

wss.on('connection', (ws, req) => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  console.log('WS connected:', req.socket.remoteAddress);

  // Opcional: al conectar, mandar "snapshot" de últimas posiciones
  ws.send(JSON.stringify({
    type: 'hello',
    ts: Date.now(),
    snapshot: Object.fromEntries(lastByUnit)
  }));

  ws.on('message', (data) => {
    const text = Buffer.isBuffer(data) ? data.toString('utf8') : String(data);
    const msg = safeJsonParse(text);

    if (!msg || typeof msg !== 'object') {
      ws.send(JSON.stringify({ type: 'error', error: 'invalid_json' }));
      return;
    }

    // Tipos de mensaje esperados
    // 1) Posición: { type:'pos', unitId:'C001', lat:-34..., lng:-58..., ts?: 123 }
    if (msg.type === 'pos') {
      const unitId = String(msg.unitId || '').trim();
      const lat = Number(msg.lat);
      const lng = Number(msg.lng);
      const ts = msg.ts ? Number(msg.ts) : Date.now();

      if (!unitId || !Number.isFinite(lat) || !Number.isFinite(lng)) {
        ws.send(JSON.stringify({ type: 'error', error: 'bad_pos_payload' }));
        return;
      }

      const payload = { type: 'pos', unitId, lat, lng, ts };

      // Guardar última posición en memoria
      lastByUnit.set(unitId, { lat, lng, ts });

      // Redistribuir a todos los tableros conectados
      broadcast(payload);

      return;
    }

    // 2) Ping lógico (opcional)
    if (msg.type === 'ping') {
      ws.send(JSON.stringify({ type: 'pong', ts: Date.now() }));
      return;
    }

    ws.send(JSON.stringify({ type: 'error', error: 'unknown_type' }));
  });

  ws.on('close', () => {
    console.log('WS disconnected');
  });

  ws.on('error', (err) => {
    console.error('WS error:', err.message);
  });
});

// Ping/Pong para limpiar conexiones muertas
setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, 30000);

server.listen(PORT, () => {
  console.log(`Twybox WS core listening on port ${PORT}`);
});
