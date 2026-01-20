const fetch = require('node-fetch');

const INGEST_URL = 'https://twybox360.com/sistemas/geolocalizacion/demo/ingest_point.php';

async function sendPoint() {
  const payload = {
    tenant_id: 'pyme_demo',
    unit_id: 'U-UKFB2Z',
    lat: -34.60,
    lng: -58.38,
    server_ts: new Date().toISOString().slice(0, 19).replace('T', ' ')
  };

  try {
    const r = await fetch(INGEST_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });

    const j = await r.json();
    console.log('sent:', j);
  } catch (err) {
    console.error('error sending point:', err.message);
  }
}

setInterval(sendPoint, 10000);
