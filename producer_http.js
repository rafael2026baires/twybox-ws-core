import fetch from 'node-fetch';

const INGEST_URL = 'https://twybox360.com/sistemas/geolocalizacion/demo/ingest_point.php';

async function sendPoint() {
  const payload = {
    tenant_id: 'TEST',
    unit_id: 'U001',
    lat: -34.6037,
    lng: -58.3816,
    server_ts: new Date().toISOString().slice(0, 19).replace('T', ' ')
  };

  const r = await fetch(INGEST_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });

  const j = await r.json();
  console.log(j);
}

setInterval(sendPoint, 10000);
