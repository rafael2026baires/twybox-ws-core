'use strict';

console.log('>>> kpi_daily.js CARGADO <<<');

const mysql = require('mysql2/promise');
const { DateTime } = require('luxon');

const pool = mysql.createPool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  database: process.env.DB_NAME,
  waitForConnections: true,
  connectionLimit: 5
});

function calcularEstadoOperativo(unit) {
  if (unit.is_offline === 1) return 'offline';
  if (unit.moving_streak >= 3) return 'moving_sostenido';
  if (unit.status === 'moving') return 'moving_ocasional';
  return 'stopped';
}

async function handleKpiDaily(req, res) {
  try {
    const url = new URL(req.url, `http://${req.headers.host}`);
    const tenantId = url.searchParams.get('tenantId');

    if (!tenantId) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'tenantId_required' }));
      return;
    }

    // 1️⃣ Config del tenant
    const [cfgRows] = await pool.execute(
      `SELECT timezone, day_start_hour FROM tenant_config WHERE tenant_id = ?`,
      [tenantId]
    );

    const timezone = cfgRows[0]?.timezone || 'UTC';
    const dayStartHour = cfgRows[0]?.day_start_hour ?? 0;

    // 2️⃣ Calcular inicio de día OPERATIVO EN JS
    const dayStart = DateTime
      .now()
      .setZone(timezone)
      .startOf('day')
      .plus({ hours: dayStartHour })
      .toUTC()
      .toSQL({ includeOffset: false });

    // 3️⃣ Presencia
    const [rows] = await pool.execute(
      `
      SELECT
        u.unit_id,
        CASE WHEN COUNT(h.id) > 0 THEN 'presente' ELSE 'ausente' END AS presencia
      FROM geo_units_last u
      LEFT JOIN geo_units_history h
        ON h.tenant_id = u.tenant_id
       AND h.unit_id = u.unit_id
       AND h.server_ts >= ?
      WHERE u.tenant_id = ?
      GROUP BY u.unit_id
      `,
      [dayStart, tenantId]
    );

    // 4️⃣ Eventos hoy
    const [rowsEventos] = await pool.execute(
      `
      SELECT unit_id, COUNT(*) AS eventos_hoy
      FROM geo_units_history
      WHERE tenant_id = ? AND server_ts >= ?
      GROUP BY unit_id
      `,
      [tenantId, dayStart]
    );

    // 5️⃣ Jornada
    const [rowsJornada] = await pool.execute(
      `
      SELECT unit_id,
             MIN(server_ts) AS inicio_dia,
             MAX(server_ts) AS fin_dia
      FROM geo_units_history
      WHERE tenant_id = ? AND server_ts >= ?
      GROUP BY unit_id
      `,
      [tenantId, dayStart]
    );

    // 6️⃣ Estado actual
    const [rowsOffline] = await pool.execute(
      `
      SELECT unit_id, status, is_offline, moving_streak
      FROM geo_units_last
      WHERE tenant_id = ?
      `,
      [tenantId]
    );

    // Maps
    const eventosMap = Object.fromEntries(rowsEventos.map(r => [r.unit_id, r.eventos_hoy]));
    const jornadaMap = Object.fromEntries(rowsJornada.map(r => [r.unit_id, r]));
    const offlineMap = Object.fromEntries(rowsOffline.map(r => [r.unit_id, r]));

    const units = rows.map(r => {
      const base = offlineMap[r.unit_id] || {};
      const unit = {
        unit_id: r.unit_id,
        presencia: r.presencia,
        eventos_hoy: eventosMap[r.unit_id] || 0,
        inicio_dia: jornadaMap[r.unit_id]?.inicio_dia || null,
        fin_dia: jornadaMap[r.unit_id]?.fin_dia || null,
        status: base.status || null,
        is_offline: base.is_offline || 0,
        moving_streak: base.moving_streak || 0
      };
      unit.estado_operativo = calcularEstadoOperativo(unit);
      return unit;
    });

    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ tenantId, dayStart, units }));

  } catch (err) {
    console.error('[kpi_daily]', err);
    res.writeHead(500, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'internal_error' }));
  }
}

module.exports = { handleKpiDaily };
