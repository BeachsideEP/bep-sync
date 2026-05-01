// BEP-SYNC-V5
require('dotenv').config();
const https = require('https');
const http = require('http');

const CLINIKO_BASE = 'https://api.au2.cliniko.com/v1';
const CLINIKO_KEY = process.env.CLINIKO_API_KEY;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;
const PER_PAGE = 100;

if (!CLINIKO_KEY || !SUPABASE_URL || !SUPABASE_KEY) {
  console.error('Missing env vars');
  process.exit(1);
}

function httpGet(url, headers) {
  return new Promise((resolve, reject) => {
    const lib = url.startsWith('https') ? https : http;
    lib.get(url, { headers }, (res) => {
      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => {
        if (res.statusCode >= 400) {
          reject(new Error('HTTP ' + res.statusCode + ' on ' + url + ': ' + body.slice(0, 300)));
          return;
        }
        try { resolve(JSON.parse(body)); }
        catch(e) { reject(new Error('JSON parse error: ' + body.slice(0, 200))); }
      });
    }).on('error', reject);
  });
}

function httpRequest(method, url, headers, body) {
  return new Promise((resolve, reject) => {
    const payload = body ? JSON.stringify(body) : '';
    const opts = {
      method,
      headers: Object.assign({ 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(payload) }, headers),
    };
    const lib = url.startsWith('https') ? https : http;
    const req = lib.request(new URL(url), opts, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        if (res.statusCode >= 400) {
          reject(new Error('HTTP ' + res.statusCode + ' on ' + method + ' ' + url + ': ' + data.slice(0, 300)));
          return;
        }
        try { resolve(data ? JSON.parse(data) : {}); }
        catch(e) { resolve({}); }
      });
    });
    req.on('error', reject);
    if (payload) req.write(payload);
    req.end();
  });
}

const clinikoHeaders = {
  'Authorization': 'Basic ' + Buffer.from(CLINIKO_KEY + ':').toString('base64'),
  'Accept': 'application/json',
  'User-Agent': 'BEP-Dashboard-Sync/2.0 (admin@beachsideep.com.au)',
};

async function clinikoFetchAll(path, entityKey) {
  const results = [];
  let url = CLINIKO_BASE + '/' + path;
  let page = 0;
  while (url) {
    page++;
    if (page <= 2 || page % 10 === 0) console.log('  [Cliniko] page ' + page + ': ' + url.replace(CLINIKO_BASE, '').split('?')[0]);
    const data = await httpGet(url, clinikoHeaders);
    const items = data[entityKey] || [];
    results.push(...items);
    await sleep(350);
    url = (data.links && data.links.next) ? data.links.next : null;
  }
  return results;
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

const supabaseHeaders = {
  'apikey': SUPABASE_KEY,
  'Authorization': 'Bearer ' + SUPABASE_KEY,
};

async function supabaseUpsert(table, rows, batchSize) {
  batchSize = batchSize || 200;
  if (rows.length === 0) return;
  for (let i = 0; i < rows.length; i += batchSize) {
    const batch = rows.slice(i, i + batchSize);
    await httpRequest(
      'POST',
      SUPABASE_URL + '/rest/v1/' + table,
      Object.assign({ 'Prefer': 'resolution=merge-duplicates,return=minimal' }, supabaseHeaders),
      batch
    );
  }
}

async function getLastSync(entity) {
  const url = SUPABASE_URL + '/rest/v1/sync_state?entity=eq.' + entity + '&select=last_synced_at';
  const data = await httpGet(url, supabaseHeaders);
  return (data[0] && data[0].last_synced_at) ? data[0].last_synced_at : '2000-01-01T00:00:00Z';
}

async function updateLastSync(entity) {
  await httpRequest(
    'PATCH',
    SUPABASE_URL + '/rest/v1/sync_state?entity=eq.' + entity,
    Object.assign({ 'Prefer': 'return=minimal' }, supabaseHeaders),
    { last_synced_at: new Date().toISOString() }
  );
}

async function writeSyncLog(entity, startedAt, records, status, err) {
  await httpRequest(
    'POST',
    SUPABASE_URL + '/rest/v1/sync_logs',
    Object.assign({ 'Prefer': 'return=minimal' }, supabaseHeaders),
    { entity, started_at: startedAt, finished_at: new Date().toISOString(), records_processed: records, status, error_message: err || null }
  );
}

function extractId(linkObj, fallbackId) {
  const link = (linkObj && linkObj.links && linkObj.links.self) ? linkObj.links.self : '';
  if (link) return Number(link.split('/').pop());
  if (fallbackId) return Number(fallbackId);
  return null;
}

function mapAppointment(a, isCancelled) {
  const pracId = extractId(a.practitioner, null) || (a.links && a.links.practitioner ? Number(a.links.practitioner.split('/').pop()) : null);
  const patId  = extractId(a.patient, null)  || (a.links && a.links.patient  ? Number(a.links.patient.split('/').pop())  : null);

  let status;
  if (isCancelled) {
    status = 'cancelled';
  } else if (a.did_not_arrive === true) {
    status = 'dna';
  } else if (new Date(a.starts_at) < new Date()) {
    status = 'completed';
  } else {
    status = 'booked';
  }

  return {
    id:                    Number(a.id),
    patient_id:            patId,
    practitioner_id:       pracId,
    appointment_type:      (a.appointment_type && a.appointment_type.name) ? a.appointment_type.name : null,
    starts_at:             a.starts_at,
    ends_at:               a.ends_at || null,
    did_not_arrive:        a.did_not_arrive === true,
    patient_arrived:       a.patient_arrived === true,
    cancelled_at:          isCancelled ? (a.cancelled_at || a.updated_at || a.created_at) : null,
    cancellation_note:     a.cancellation_note || a.cancellation_reason || null,
    treatment_note_status: Number(a.treatment_note_status) || 0,
    is_group:              false,
    status_clean:          status,
    is_completed:          status === 'completed',
    is_dna:                status === 'dna',
    is_cancelled:          status === 'cancelled',
    created_at:            a.created_at,
    updated_at:            a.updated_at,
  };
}

async function syncPractitioners() {
  const startedAt = new Date().toISOString();
  console.log('\n Syncing practitioners...');
  try {
    const items = await clinikoFetchAll('practitioners?per_page=' + PER_PAGE, 'practitioners');
    const rows = items.map(p => ({
      id:         Number(p.id),
      name:       (p.first_name + ' ' + p.last_name).trim(),
      first_name: p.first_name,
      last_name:  p.last_name,
      active:     !p.archived_at,
      created_at: p.created_at,
      updated_at: p.updated_at,
    }));
    await supabaseUpsert('practitioners', rows);
    await updateLastSync('practitioners');
    await writeSyncLog('practitioners', startedAt, rows.length, 'success');
    console.log('  OK: ' + rows.length + ' practitioners');
  } catch(e) {
    await writeSyncLog('practitioners', startedAt, 0, 'error', e.message);
    throw e;
  }
}

async function syncPatients() {
  const startedAt = new Date().toISOString();
  const lastSync = await getLastSync('patients');
  console.log('\n Syncing patients since ' + lastSync + '...');
  try {
    const items = await clinikoFetchAll(
      'patients?per_page=' + PER_PAGE + '&updated_since=' + encodeURIComponent(lastSync),
      'patients'
    );
    const rows = items.map(p => ({
      id:              Number(p.id),
      first_name:      p.first_name,
      last_name:       p.last_name,
      dob:             p.date_of_birth || null,
      referral_source: p.referral_source || null,
      created_at:      p.created_at,
      updated_at:      p.updated_at,
    }));
    await supabaseUpsert('patients', rows);
    await updateLastSync('patients');
    await writeSyncLog('patients', startedAt, rows.length, 'success');
    console.log('  OK: ' + rows.length + ' patients');
  } catch(e) {
    await writeSyncLog('patients', startedAt, 0, 'error', e.message);
    throw e;
  }
}

async function syncAppointments() {
  const startedAt = new Date().toISOString();
  const lastSync = await getLastSync('appointments');
  console.log('\n Syncing appointments since ' + lastSync + '...');
  try {
    // Active appointments
    const active = await clinikoFetchAll(
      'individual_appointments?per_page=' + PER_PAGE + '&updated_since=' + encodeURIComponent(lastSync),
      'individual_appointments'
    );
    console.log('  Active: ' + active.length);

    // Cancelled appointments — Cliniko separate endpoint
    const cancelled = await clinikoFetchAll(
      'individual_appointments/cancelled?per_page=' + PER_PAGE + '&updated_since=' + encodeURIComponent(lastSync),
      'individual_appointments'
    );
    console.log('  Cancelled: ' + cancelled.length);

    if (cancelled.length > 0) {
      console.log('  [Debug] Sample cancelled keys:', Object.keys(cancelled[0]).join(', '));
      console.log('  [Debug] Sample cancelled_at:', cancelled[0].cancelled_at);
    }

    const activeRows    = active.map(a => mapAppointment(a, false));
    const cancelledRows = cancelled.map(a => mapAppointment(a, true));
    const allRows       = activeRows.concat(cancelledRows);

    // Dedup by id — cancelled version wins
    const byId = {};
    activeRows.forEach(r => { byId[r.id] = r; });
    cancelledRows.forEach(r => { byId[r.id] = r; }); // overwrite with cancelled
    const dedupedRows = Object.values(byId);

    await supabaseUpsert('appointments', dedupedRows);
    await updateLastSync('appointments');
    await writeSyncLog('appointments', startedAt, dedupedRows.length, 'success');

    const c = dedupedRows.filter(r => r.is_cancelled).length;
    const d = dedupedRows.filter(r => r.is_dna).length;
    const done = dedupedRows.filter(r => r.is_completed).length;
    console.log('  OK: ' + dedupedRows.length + ' appointments (completed=' + done + ', cancelled=' + c + ', dna=' + d + ')');
  } catch(e) {
    await writeSyncLog('appointments', startedAt, 0, 'error', e.message);
    throw e;
  }
}

async function syncInvoices() {
  const startedAt = new Date().toISOString();
  const lastSync = await getLastSync('invoices');
  console.log('\n Syncing invoices since ' + lastSync + '...');
  try {
    const items = await clinikoFetchAll(
      'invoices?per_page=' + PER_PAGE + '&updated_since=' + encodeURIComponent(lastSync),
      'invoices'
    );

    if (items.length > 0) {
      console.log('  [Debug] Invoice fields:', Object.keys(items[0]).join(', '));
      console.log('  [Debug] Sample amounts: net_amount=' + items[0].net_amount + ' total_amount=' + items[0].total_amount + ' amount_due=' + items[0].amount_due);
    }

    const rows = items.map(inv => {
      const total = parseFloat(inv.net_amount) || parseFloat(inv.total_amount) || parseFloat(inv.amount_due) || 0;
      const apptId = extractId(inv.appointment, null) || (inv.links && inv.links.appointment ? Number(inv.links.appointment.split('/').pop()) : null);
      const patId  = extractId(inv.patient, null)     || (inv.links && inv.links.patient     ? Number(inv.links.patient.split('/').pop())     : null);
      const pracId = extractId(inv.practitioner, null) || (inv.links && inv.links.practitioner ? Number(inv.links.practitioner.split('/').pop()) : null);
      return {
        id:              Number(inv.id),
        patient_id:      patId,
        appointment_id:  apptId,
        practitioner_id: pracId,
        total_amount:    total,
        status:          inv.status || null,
        created_at:      inv.created_at,
        updated_at:      inv.updated_at,
      };
    });

    await supabaseUpsert('invoices', rows);
    await updateLastSync('invoices');
    await writeSyncLog('invoices', startedAt, rows.length, 'success');
    const totalRev = rows.reduce((s, r) => s + r.total_amount, 0);
    console.log('  OK: ' + rows.length + ' invoices, total $' + totalRev.toFixed(2));
  } catch(e) {
    await writeSyncLog('invoices', startedAt, 0, 'error', e.message);
    throw e;
  }
}

async function updateAppointmentRevenue() {
  console.log('\n Updating revenue...');
  const url = SUPABASE_URL + '/rest/v1/invoices?select=appointment_id,total_amount&appointment_id=not.is.null&limit=10000';
  const invoices = await httpGet(url, supabaseHeaders);
  const map = {};
  for (const inv of invoices) {
    if (!inv.appointment_id) continue;
    map[inv.appointment_id] = (map[inv.appointment_id] || 0) + (parseFloat(inv.total_amount) || 0);
  }
  const updates = Object.entries(map);
  let done = 0;
  for (const [id, rev] of updates) {
    await httpRequest('PATCH', SUPABASE_URL + '/rest/v1/appointments?id=eq.' + id,
      Object.assign({ 'Prefer': 'return=minimal' }, supabaseHeaders), { actual_revenue: rev });
    done++;
    if (done % 100 === 0) { console.log('  ... ' + done + '/' + updates.length); await sleep(200); }
  }
  console.log('  OK: Revenue updated for ' + done + ' appointments');
}

async function runSync() {
  console.log('============================================================');
  console.log('BEP Sync V5 - ' + new Date().toISOString());
  console.log('============================================================');
  const t = Date.now();
  let failed = false;
  try { await syncPractitioners(); } catch(e) { console.error('FAILED practitioners:', e.message); failed = true; }
  try { await syncPatients();      } catch(e) { console.error('FAILED patients:', e.message);      failed = true; }
  try { await syncAppointments();  } catch(e) { console.error('FAILED appointments:', e.message);  failed = true; }
  try { await syncInvoices();      } catch(e) { console.error('FAILED invoices:', e.message);      failed = true; }
  if (!failed) {
    try { await updateAppointmentRevenue(); } catch(e) { console.error('FAILED revenue:', e.message); }
  }
  console.log('\n============================================================');
  console.log('Sync ' + (failed ? 'WITH ERRORS' : 'COMPLETE') + ' in ' + ((Date.now()-t)/1000).toFixed(1) + 's');
  console.log('============================================================');
}

runSync().catch(e => { console.error('Fatal:', e); process.exit(1); });
