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

    // ===== ACA ESTA TU SELECT =====
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

    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      tenantId,
      units: rows
    }));

  } catch (err) {
    console.error('[kpi_daily]', err);
    res.writeHead(500, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'internal_error' }));
  }
}

module.exports = { handleKpiDaily };
