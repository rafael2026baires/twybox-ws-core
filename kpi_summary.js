'use strict';

console.log('>>> kpi_summary.js CARGADO <<<');

const mysql = require('mysql2/promise');

const pool = mysql.createPool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  database: process.env.DB_NAME,
  waitForConnections: true,
  connectionLimit: 5
});

async function handleKpiSummary(req, res) {
  try {
    const url = new URL(req.url, `http://${req.headers.host}`);
    const tenantId = url.searchParams.get('tenantId');

    if (!tenantId) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'tenantId_required' }));
      return;
    }

    const [rows] = await pool.execute(
      `
      SELECT
        COUNT(*)                                   AS total_units,
        SUM(is_offline = 0)                        AS online,
        SUM(is_offline = 1)                        AS offline,
        SUM(estado_operativo = 'moving')           AS moving,
        SUM(estado_operativo = 'stopped')          AS stopped
      FROM geo_units_last
      WHERE tenant_id = ?
      `,
      [tenantId]
    );

    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      tenantId,
      ...rows[0]
    }));

  } catch (err) {
    console.error('[kpi_summary]', err);
    res.writeHead(500, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'internal_error' }));
  }
}

module.exports = { handleKpiSummary };
