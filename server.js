'use strict';

const http = require('http');
const WebSocket = require('ws');

const { startPersistWorker } = require('./persist_mysql');
const { handleKpiDaily } = require('./kpi_daily');


const PORT = process.env.PORT || 3000;

/* ================== CONFIG CENTRAL ================== */
const CFG = {
  MIN_INTERVAL_MS: 10_000,        // throttle por unidad
  UNIT_TTL_MS: 3 * 60 * 1000,     // offline
  MOVE_THRESHOLD_M: 10,           // moving / stopped
  MAX_JUMP_M: 300,                // salto imposible
  CLEAN_EVERY_MS: 60 * 1000       // limpieza
};
/* ==================================================== */

const persistQueue = [];
const MAX_QUEUE = 5000;

startPersistWorker({ persistQueue });
console.log('>>> startPersistWorker() ejecutado <<<');

// HTTP server (sirve para health y para "upgrade" a WebSocket)
const server = http.createServer((req, res) => {

  // === CORS ===
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }    

  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      ok: true,
      ts: Date.now(),
      clients: wss.clients.size
    }));
    return;
  }

  if (req.url === '/stats') {
  
    let unitsActive = 0;
    let unitsOffline = 0;
  
    for (const tmap of lastByTenant.values()) {
      for (const data of tmap.values()) {
        if (data.isOffline) unitsOffline++;
        else unitsActive++;
      }
    }
  
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      ts: Date.now(),
      connections: stats.connections,
      posAccepted: stats.posAccepted,
      posRejected: stats.posRejected,
      unitsActive,
      unitsOffline,
      queue: persistQueue.length
    }));
    return;
  }
  
  if (req.url.startsWith('/last')) {
    const url = new URL(req.url, 'http://localhost');
    const tenantId = url.searchParams.get('tenantId');
    if (!tenantId) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'tenantId_required' }));
      return;
    }
    const tmap = lastByTenant.get(tenantId) || new Map();
    const result = [];
    for (const [unitId, data] of tmap.entries()) {
      result.push({
        unitId,
        lat: data.lat,
        lng: data.lng,
        ts: data.ts,
        status: data.status || 'stopped',
        isOffline: !!data.isOffline
      });
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      tenantId,
      units: result,
      ts: Date.now()
    }));
    return;
  }

  if (req.url.startsWith('/kpi/daily')) {
    return handleKpiDaily(req, res);
  }  

  // DEFAULT
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Twybox WS core online. Use /health or WebSocket.\n');
});

// WebSocket server
const wss = new WebSocket.Server({ server });
const stats = {
  connections: 0,
  posAccepted: 0,
  posRejected: 0
};

// tenantId -> Map(unitId -> { lat, lng, ts })
const lastByTenant = new Map();

// tenantId -> Map(unitId -> lastAcceptedTs)
const lastSeenTsByTenant = new Map();


function safeJsonParse(s) {
  try { return JSON.parse(s); } catch { return null; }
}

function getTenantMap(tenantId) {
  if (!lastByTenant.has(tenantId)) {
    lastByTenant.set(tenantId, new Map());
  }
  return lastByTenant.get(tenantId);
}

function getTenantTsMap(tenantId) {
  if (!lastSeenTsByTenant.has(tenantId)) {
    lastSeenTsByTenant.set(tenantId, new Map());
  }
  return lastSeenTsByTenant.get(tenantId);
}

function broadcastToTenant(tenantId, obj) {
  const msg = JSON.stringify(obj);
  for (const client of wss.clients) {
    if (
      client.readyState === WebSocket.OPEN &&
      client.tenantId === tenantId
    ) {
      client.send(msg);
    }
  }
}

wss.on('connection', (ws, req) => {
    
  ws.tenantId = null;
  ws.unitId = null;
  
  stats.connections++;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  console.log('WS connected:', req.socket.remoteAddress);

  // Opcional: al conectar, mandar "snapshot" de últimas posiciones
    ws.send(JSON.stringify({
      type: 'hello',
      ts: Date.now(),
      note: 'send tenantId to receive data'
    }));

  ws.on('message', (data) => {
    const text = Buffer.isBuffer(data) ? data.toString('utf8') : String(data);
    const msg = safeJsonParse(text);

    if (!msg || typeof msg !== 'object') {
      ws.send(JSON.stringify({ type: 'error', error: 'invalid_json' }));
      return;
    }
    
    
    if (msg.type === 'register') {
      const tenantId = String(msg.tenantId || '').trim();
      const unitId   = String(msg.unitId || '').trim();
    
      if (!tenantId || !unitId) {
        ws.send(JSON.stringify({
          type: 'error',
          error: 'bad_register_payload'
        }));
        return;
      }
    
      ws.tenantId = tenantId;
      ws.unitId = unitId;
    
      ws.send(JSON.stringify({
        type: 'registered',
        tenantId,
        unitId,
        ts: Date.now()
      }));
      
    const tmap = lastByTenant.get(tenantId);
    
    if (tmap && tmap.size > 0) {
      const snapshot = [];
      for (const [uId, data] of tmap.entries()) {
        snapshot.push({
          unitId: uId,
          lat: data.lat,
          lng: data.lng,
          ts: data.ts
        });
      }
    
      ws.send(JSON.stringify({
        v: 1,
        type: 'snapshot',
        tenantId,
        units: snapshot,
        ts: Date.now()
      }));
    }
    
      return;
    }

    // Tipos de mensaje esperados
    // 1) Posición: { type:'pos', unitId:'C001', lat:-34..., lng:-58..., ts?: 123 }
    if (msg.type === 'pos') {
        
        if (!ws.tenantId || !ws.unitId) {
          ws.send(JSON.stringify({
            type: 'error',
            error: 'not_registered'
          }));
          return;
        }
        
      const tenantId = String(msg.tenantId || '').trim();
      const unitId   = String(msg.unitId || '').trim();
        
      const latRaw = Number(msg.lat);
      const lngRaw = Number(msg.lng);
        
      let lat = latRaw;
      let lng = lngRaw;
        
      if (Math.abs(latRaw) > 90 || Math.abs(lngRaw) > 180) {
          lat = latRaw / 1e6;
          lng = lngRaw / 1e6;
      }        
        
      const ts  = msg.ts ? Number(msg.ts) : Date.now();
    
      if (!tenantId || !unitId || !Number.isFinite(lat) || !Number.isFinite(lng)) {
        stats.posRejected++;  
        ws.send(JSON.stringify({ type: 'error', error: 'bad_pos_payload' }));
        return;
      }
    
        if (tenantId !== ws.tenantId || unitId !== ws.unitId) {
          stats.posRejected++;
          ws.send(JSON.stringify({
            type: 'error',
            error: 'identity_mismatch'
          }));
          return;
        }

    
    // Throttle: aceptar ~1 evento cada 10s por unidad
    const MIN_INTERVAL = CFG.MIN_INTERVAL_MS;
    
    const tsMap = getTenantTsMap(tenantId);
    const lastTs = tsMap.get(unitId) || 0;
    
    if (ts - lastTs < MIN_INTERVAL) {
      stats.posRejected++;    
      ws.send(JSON.stringify({
        type: 'error',
        error: 'rate_limited',
        retryInMs: MIN_INTERVAL - (ts - lastTs)
      }));
      return;
    }
    
    // Registrar timestamp aceptado
    tsMap.set(unitId, ts);
    stats.posAccepted++;

    // Guardar última posición
    const tmap = getTenantMap(tenantId);   
    const prev = tmap.get(unitId); // puede ser undefined        
    // -------------------------------------------------------    
    // === FILTRO DE SALTO IMPOSIBLE ===
    if (prev) {
      const dtSec = (ts - prev.ts) / 1000;
      if (dtSec > 0) {
        const R = 6371000; // radio Tierra (m)
        const toRad = x => x * Math.PI / 180;
    
        const dLat = toRad(lat - prev.lat);
        const dLng = toRad(lng - prev.lng);
    
        const a =
          Math.sin(dLat/2) ** 2 +
          Math.cos(toRad(prev.lat)) *
          Math.cos(toRad(lat)) *
          Math.sin(dLng/2) ** 2;
    
        const distM = 2 * R * Math.asin(Math.sqrt(a));
    
        // umbral: 300 m en ~10 s
        if (distM > CFG.MAX_JUMP_M && dtSec <= 20) {
          // salto imposible → ignorar
          return;
        }
      }
    }
    // -----------------------------------------------------  
    // === ESTADO DE MOVIMIENTO ===
    let status = 'stopped';
    
    const prev2 = tmap.get(unitId);
    if (prev2) {
      const R = 6371000;
      const toRad = x => x * Math.PI / 180;
    
      const dLat = toRad(lat - prev2.lat);
      const dLng = toRad(lng - prev2.lng);
    
      const a =
        Math.sin(dLat / 2) ** 2 +
        Math.cos(toRad(prev2.lat)) *
        Math.cos(toRad(lat)) *
        Math.sin(dLng / 2) ** 2;
    
      const distM = 2 * R * Math.asin(Math.sqrt(a));
    
      if (distM >= CFG.MOVE_THRESHOLD_M) {
        status = 'moving';
      }
    }
    // -----------------------------------------------------        
    // === PERSISTENCIA DESACOPLADA (ENCOLAR) ===
    persistQueue.push({
      tenantId: ws.tenantId,
      unitId: ws.unitId,
      lat,
      lng,
      ts,
      status
    });
    // -----------------------------------------------------       
    if (persistQueue.length > MAX_QUEUE) {
      persistQueue.shift();
    }      
      tmap.set(unitId, { lat, lng, ts, status, isOffline: false });
      
      broadcastToTenant(tenantId, {
        v: 1,
        type: 'pos',
        tenantId,
        unitId,
        lat,
        lng,
        ts
      });
    
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
    stats.connections--;
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
setInterval(() => {
  const now = Date.now();

  for (const [tenantId, tmap] of lastByTenant.entries()) {
    for (const [unitId, data] of tmap.entries()) {
        if (now - data.ts > CFG.UNIT_TTL_MS) {

          // marcar offline (NO borrar)
          data.isOffline = true;
          data.status = 'offline';

          // === PERSISTIR OFFLINE EN DB ===
          persistQueue.push({
            type: 'offline',
            tenantId,
            unitId,
            server_ts: new Date(data.ts)
          });          
        
          // notificar offline al tenant
          broadcastToTenant(tenantId, {
            v: 1,
            type: 'offline',
            tenantId,
            unitId,
            ts: now
          });
        }
    }

    // si el tenant quedó vacío, limpiarlo
    if (tmap.size === 0) {
      lastByTenant.delete(tenantId);
    }
  }
}, CFG.CLEAN_EVERY_MS);

