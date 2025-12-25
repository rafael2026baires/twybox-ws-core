'use strict';

console.log('>>> kpi_daily.js CARGADO <<<');

const mysql = require('mysql2/promise');

const pool = mysql.createPool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  database: process.env.DB_NAME,
  waitForConnections: true,
  connectionLimit: 5
});

async function handleKpiDaily(req, res) {
  try {
    const url = new URL(req.url, `http://${req.headers.host}`);
    const tenantId = url.searchParams.get('tenantId');

    if (!tenantId) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'tenantId_required' }));
      return;
    }

    // ----------------------------------------------------------------------------------------------------
    const [rows] = await pool.execute(
      `
      SELECT
        u.unit_id,
        CASE
          WHEN COUNT(h.id) > 0 THEN 'presente'
          ELSE 'ausente'
        END AS presencia
      FROM geo_units_last u
      LEFT JOIN geo_units_history h
        ON h.tenant_id = u.tenant_id
       AND h.unit_id   = u.unit_id
       AND h.server_ts >= CURDATE()
      WHERE u.tenant_id = ?
      GROUP BY u.unit_id
      `,
      [tenantId]
    );
    const [rowsEventos] = await pool.execute(
      `
      SELECT
        unit_id,
        COUNT(*) AS eventos_hoy
      FROM geo_units_history
      WHERE tenant_id = ?
        AND server_ts >= CURDATE()
      GROUP BY unit_id
      `,
      [tenantId]
    );    
    const [rowsMinutos] = await pool.execute(
      `
      SELECT
        unit_id,
        TIMESTAMPDIFF(
          MINUTE,
          MAX(server_ts),
          NOW()
        ) AS minutos_desde_ultima_senal
      FROM geo_units_history
      WHERE tenant_id = ?
      GROUP BY unit_id
      `,
      [tenantId]
    );  
    const [rowsJornada] = await pool.execute(
      `
      SELECT
        unit_id,
        MIN(server_ts) AS inicio_dia,
        MAX(server_ts) AS fin_dia
      FROM geo_units_history
      WHERE tenant_id = ?
        AND server_ts >= CURDATE()
      GROUP BY unit_id
      `,
      [tenantId]
    );    
    // ----------------------------------------------------------------------------------------------------
    // indexar eventos por unit_id
    const eventosMap = {};
    for (const r of rowsEventos) {
      eventosMap[r.unit_id] = r.eventos_hoy;
    }
    // indexar minutos desde última señal por unit_id
    const minutosMap = {};
    for (const r of rowsMinutos) {
      minutosMap[r.unit_id] = r.minutos_desde_ultima_senal;
    }   
    // indexar inicio / fin por unit_id
    const jornadaMap = {};
    for (const r of rowsJornada) {
      jornadaMap[r.unit_id] = {
        inicio_dia: r.inicio_dia,
        fin_dia: r.fin_dia
      };
    }       
    // unir presencia + eventos
    const units = rows.map(r => ({
      unit_id: r.unit_id,
      presencia: r.presencia,
      eventos_hoy: eventosMap[r.unit_id] || 0,
      minutos_desde_ultima_senal: minutosMap[r.unit_id] ?? null,
      inicio_dia: jornadaMap[r.unit_id]?.inicio_dia ?? null,
      fin_dia: jornadaMap[r.unit_id]?.fin_dia ?? null
    }));

   

    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      tenantId,
      units
    }));

  } catch (err) {
    console.error('[kpi_daily]', err);
    res.writeHead(500, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'internal_error' }));
  }
}

module.exports = { handleKpiDaily };
