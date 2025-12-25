console.log('>>> kpi_daily.js CARGADO <<<');


'use strict';

async function handleKpiDaily(req, res) {
  // Por ahora SOLO confirmamos que el endpoint funciona.
  // Todavía NO hay SQL.
  const url = new URL(req.url, `http://${req.headers.host}`);
  const tenantId = url.searchParams.get('tenantId');

  if (!tenantId) {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'tenantId_required' }));
    return;
  }

  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({
    ok: true,
    endpoint: '/kpi/daily',
    tenantId,
    note: 'kpi_daily.js conectado (sin SQL todavía)'
  }));
}

module.exports = { handleKpiDaily };
