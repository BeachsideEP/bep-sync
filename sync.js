// BEP-SYNC-V6
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
        catch(e) { reject(new Error('JSON parse: ' + body.slice(0, 200))); }
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
          reject(new Error('HTTP ' + res.statusCode + ' ' + method + ' ' + url + ': ' + data.slice(0, 300)));
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

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

const supabaseHeaders = {
  'apikey': SUPABASE_KEY,
  'Authorization': 'Bearer ' + SUPABASE_KEY,
};

async function supabaseUpsert(table, rows, batchSize) {
  batchSize = batchSize || 200;
  if (!rows.length) return;
  for (let i = 0; i < rows.length; i += batchSize) {
    await httpRequest('POST', SUPABASE_URL + '/rest/v1/' + table,
      Object.assign({ 'Prefer': 'resolution=merge-duplicates,return=minimal' }, supabaseHeaders),
      rows.slice(i, i + batchSize));
  }
}

async function getLastSync(entity) {
  const data = await httpGet(SUPABASE_URL + '/rest/v1/sync_state?entity=eq.' + entity + '&select=last_synced_at', supabaseHeaders);
  return (data[0] && data[0].last_synced_at) ? data[0].last_synced_at : '2000-01-01T00:00:00Z';
}

async function updateLastSync(entity) {
  await httpRequest('PATCH', SUPABASE_URL + '/rest/v1/sync_state?entity=eq.' + entity,
    Object.assign({ 'Prefer': 'return=minimal' }, supabaseHeaders),
    { last_synced_at: new Date().toISOString() });
}

async function writeSyncLog(entity, startedAt, records, status, err) {
  await httpRequest('POST', SUPABASE_URL + '/rest/v1/sync_logs',
    Object.assign({ 'Prefer': 'return=minimal' }, supabaseHeaders),
    { entity, started_at: startedAt, finished_at: new Date().toISOString(), records_processed: records, status, error_message: err || null });
}

function extractId(obj) {
  if (!obj) return null;
  const link = obj.links && obj.links.self ? obj.links.self : '';
  if (link) return Number(link.split('/').pop());
  if (obj.id) return Number(obj.id);
  return null;
}

// ── FETCH APPOINTMENT TYPES LOOKUP ───────────────────────────
async function fetchAppointmentTypes() {
  console.log('  Fetching appointment types...');
  const items = await clinikoFetchAll('appointment_types?per_page=' + PER_PAGE, 'appointment_types');
  const lookup = {};
  items.forEach(t => {
    lookup[String(t.id)] = t.name;
  });
  console.log('  Found ' + items.length + ' appointment types');
  if (items.length > 0) {
    console.log('  Sample types:', items.slice(0,5).map(t => t.id + ':' + t.name).join(', '));
  }
  return lookup;
}

function mapAppointment(a, isCancelled, apptTypeLookup) {
  const pracId = extractId(a.practitioner);
  const patId  = extractId(a.patient);

  // Extract appointment type ID from linked object, look up name
  const apptTypeId = extractId(a.appointment_type);
  const apptTypeName = apptTypeId ? (apptTypeLookup[String(apptTypeId)] || null) : null;

  // Detect group/class by name
  const isGroup = apptTypeName ? /class|group/i.test(apptTypeName) : false;

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
    appointment_type:      apptTypeName,
    starts_at:             a.starts_at,
    ends_at:               a.ends_at || null,
    did_not_arrive:        a.did_not_arrive === true,
    patient_arrived:       a.patient_arrived === true,
    cancelled_at:          isCancelled ? (a.cancelled_at || a.updated_at || a.created_at) : null,
    cancellation_note:     a.cancellation_note || a.cancellation_reason || null,
    treatment_note_status: Number(a.treatment_note_status) || 0,
    is_group:              isGroup,
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
      id: Number(p.id), name: (p.first_name + ' ' + p.last_name).trim(),
      first_name: p.first_name, last_name: p.last_name,
      active: !p.archived_at, created_at: p.created_at, updated_at: p.updated_at,
    }));
    await supabaseUpsert('practitioners', rows);
    await updateLastSync('practitioners');
    await writeSyncLog('practitioners', startedAt, rows.length, 'success');
    console.log('  OK: ' + rows.length + ' practitioners');
  } catch(e) { await writeSyncLog('practitioners', startedAt, 0, 'error', e.message); throw e; }
}

async function syncPatients() {
  const startedAt = new Date().toISOString();
  const lastSync = await getLastSync('patients');
  console.log('\n Syncing patients since ' + lastSync + '...');
  try {
    const items = await clinikoFetchAll(
      'patients?per_page=' + PER_PAGE + '&updated_since=' + encodeURIComponent(lastSync), 'patients');
    const rows = items.map(p => ({
      id: Number(p.id), first_name: p.first_name, last_name: p.last_name,
      dob: p.date_of_birth || null, referral_source: p.referral_source || null,
      created_at: p.created_at, updated_at: p.updated_at,
    }));
    await supabaseUpsert('patients', rows);
    await updateLastSync('patients');
    await writeSyncLog('patients', startedAt, rows.length, 'success');
    console.log('  OK: ' + rows.length + ' patients');
  } catch(e) { await writeSyncLog('patients', startedAt, 0, 'error', e.message); throw e; }
}

async function syncAppointments() {
  const startedAt = new Date().toISOString();
  const lastSync = await getLastSync('appointments');
  console.log('\n Syncing appointments since ' + lastSync + '...');
  try {
    // Fetch appointment types first — needed to get type names
    const apptTypeLookup = await fetchAppointmentTypes();

    // Active appointments
    const active = await clinikoFetchAll(
      'individual_appointments?per_page=' + PER_PAGE + '&updated_since=' + encodeURIComponent(lastSync),
      'individual_appointments');
    console.log('  Active: ' + active.length);

    // Cancelled appointments
    const cancelled = await clinikoFetchAll(
      'individual_appointments/cancelled?per_page=' + PER_PAGE + '&updated_since=' + encodeURIComponent(lastSync),
      'individual_appointments');
    console.log('  Cancelled: ' + cancelled.length);

    // Map and dedup — cancelled version wins
    const byId = {};
    active.forEach(a => { byId[a.id] = mapAppointment(a, false, apptTypeLookup); });
    cancelled.forEach(a => { byId[a.id] = mapAppointment(a, true, apptTypeLookup); });
    const rows = Object.values(byId);

    await supabaseUpsert('appointments', rows);
    await updateLastSync('appointments');

    const c = rows.filter(r => r.is_cancelled).length;
    const d = rows.filter(r => r.is_dna).length;
    const done = rows.filter(r => r.is_completed).length;
    const grp = rows.filter(r => r.is_group).length;
    console.log('  OK: ' + rows.length + ' appts — completed=' + done + ' cancelled=' + c + ' dna=' + d + ' group/class=' + grp);

    await writeSyncLog('appointments', startedAt, rows.length, 'success');
  } catch(e) { await writeSyncLog('appointments', startedAt, 0, 'error', e.message); throw e; }
}

async function syncInvoices() {
  const startedAt = new Date().toISOString();
  const lastSync = await getLastSync('invoices');
  console.log('\n Syncing invoices since ' + lastSync + '...');
  try {
    const items = await clinikoFetchAll(
      'invoices?per_page=' + PER_PAGE + '&updated_since=' + encodeURIComponent(lastSync), 'invoices');

    const rows = items.map(inv => {
      const total = parseFloat(inv.net_amount) || parseFloat(inv.total_amount) || 0;
      return {
        id:              Number(inv.id),
        patient_id:      extractId(inv.patient),
        appointment_id:  extractId(inv.appointment),
        practitioner_id: extractId(inv.practitioner),
        total_amount:    total,
        status:          inv.status || null,
        created_at:      inv.created_at,
        updated_at:      inv.updated_at,
      };
    });

    await supabaseUpsert('invoices', rows);
    await updateLastSync('invoices');
    await writeSyncLog('invoices', startedAt, rows.length, 'success');
    console.log('  OK: ' + rows.length + ' invoices, $' + rows.reduce((s,r) => s+r.total_amount, 0).toFixed(2));
  } catch(e) { await writeSyncLog('invoices', startedAt, 0, 'error', e.message); throw e; }
}

async function updateAppointmentRevenue() {
  console.log('\n Updating revenue...');
  const invoices = await httpGet(
    SUPABASE_URL + '/rest/v1/invoices?select=appointment_id,total_amount&appointment_id=not.is.null&limit=10000',
    supabaseHeaders);
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
  console.log('BEP Sync V6 - ' + new Date().toISOString());
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
