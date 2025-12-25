'use strict';

const mysql = require('mysql2/promise');

console.log('>>> persist_mysql.js (MYSQL) CARGADO <<<');

function startPersistWorker({ persistQueue }) {

  const pool = mysql.createPool({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASS,
    database: process.env.DB_NAME,
    waitForConnections: true,
    connectionLimit: 5
  });

  setInterval(async () => {
    if (persistQueue.length === 0) return;

    const batch = persistQueue.splice(0, 20);

    try {
      const conn = await pool.getConnection();

      for (const ev of batch) {

        // === EVENTO OFFLINE ===
        if (ev.type === 'offline') {
          await conn.execute(
            `UPDATE geo_units_last
             SET is_offline = 1,
                 status = 'offline'
             WHERE tenant_id = ? AND unit_id = ?`,
            [ev.tenantId, ev.unitId]
          );
          continue;
        }
        
        await conn.execute(
          `INSERT INTO geo_units_history
           (tenant_id, unit_id, lat, lng, server_ts)
           VALUES (?, ?, ?, ?, FROM_UNIXTIME(?/1000))`,
          [ev.tenantId, ev.unitId, ev.lat, ev.lng, ev.ts]
        );

        await conn.execute(
          `INSERT INTO geo_units_last
           (tenant_id, unit_id, lat, lng, server_ts, is_offline, status)
           VALUES (?, ?, ?, ?, FROM_UNIXTIME(?/1000), 0, ?)
           ON DUPLICATE KEY UPDATE
             lat=VALUES(lat),
             lng=VALUES(lng),
             server_ts=VALUES(server_ts),
             is_offline=0,
             status=VALUES(status)`,
          [ev.tenantId, ev.unitId, ev.lat, ev.lng, ev.ts, ev.status]
        );
      }

      conn.release();
      console.log('[persist] mysql ok:', batch.length);

    } catch (err) {
      console.error('[persist] mysql error:', err.message);
      persistQueue.unshift(...batch); // reintento simple
    }

  }, 3000);
}

module.exports = { startPersistWorker };
