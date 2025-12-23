'use strict';

console.log('>>> persist_mysql.js CARGADO <<<');

function startPersistWorker({ persistQueue }) {
  setInterval(() => {
    if (persistQueue.length === 0) return;
    const batch = persistQueue.splice(0, 50);
    console.log('[persist] batch recibido:', batch.length);
  }, 2000);
}

module.exports = { startPersistWorker };
