// BEP-SYNC-V4
require('dotenv').config();
const https = require('https');
const http = require('http');

const CLINIKO_BASE = 'https://api.au2.cliniko.com/v1';
const CLINIKO_KEY = process.env.CLINIKO_API_KEY;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;
const PER_PAGE = 100;

if (!CLINIKO_KEY || !SUPABASE_URL || !SUPABASE_KEY) {
  console.error('Missing required env vars: CLINIKO_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_KEY');
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

// Cliniko returns different entity keys depending on endpoint
// individual_appointments, group_appointments, patients, practitioners, invoices
async function clinikoFetchAll(path, entityKey) {
  const results = [];
  let url = CLINIKO_BASE + '/' + path;
  let page = 0;
  while (url) {
    page++;
    console.log('  [Cliniko] page ' + page + ': ' + url.replace(CLINIKO_BASE, ''));
    const data = await httpGet(url, clinikoHeaders);

    // Debug first page to see what keys are returned
    if (page === 1) {
      const keys = Object.keys(data).filter(k => k !== 'links' && k !== 'total_entries');
      console.log('  [Debug] Response keys:', keys.join(', '));
    }

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

async function writeSyncLog(entity, startedAt, recordsProcessed, status, errorMessage) {
  await httpRequest(
    'POST',
    SUPABASE_URL + '/rest/v1/sync_logs',
    Object.assign({ 'Prefer': 'return=minimal' }, supabaseHeaders),
    {
      entity: entity,
      started_at: startedAt,
      finished_at: new Date().toISOString(),
      records_processed: recordsProcessed,
      status: status,
      error_message: errorMessage || null,
    }
  );
}

// Derive status from Cliniko appointment fields
// Cliniko individual_appointments have: cancelled_at, did_not_arrive, patient_arrived
function deriveStatus(a) {
  // Cancelled takes priority
  if (a.cancelled_at) return 'cancelled';
  // DNA
  if (a.did_not_arrive === true) return 'dna';
  // Past appointment = completed
  const now = new Date();
  const start = new Date(a.starts_at);
  if (start < now) return 'completed';
  return 'booked';
}

async function syncPractitioners() {
  const startedAt = new Date().toISOString();
  console.log('\n Syncing practitioners...');
  try {
    const items = await clinikoFetchAll('practitioners?per_page=' + PER_PAGE, 'practitioners');
    console.log('  Sample practitioner fields:', items[0] ? Object.keys(items[0]).join(', ') : 'none');
    const rows = items.map(function(p) {
      return {
        id: Number(p.id),
        name: (p.first_name + ' ' + p.last_name).trim(),
        first_name: p.first_name,
        last_name: p.last_name,
        active: !p.archived_at,
        created_at: p.created_at,
        updated_at: p.updated_at,
      };
    });
    await supabaseUpsert('practitioners', rows);
    await updateLastSync('practitioners');
    await writeSyncLog('practitioners', startedAt, rows.length, 'success');
    console.log('  OK: ' + rows.length + ' practitioners synced');
  } catch(e) {
    await writeSyncLog('practitioners', startedAt, 0, 'error', e.message);
    throw e;
  }
}

async function syncPatients() {
  const startedAt = new Date().toISOString();
  const lastSync = await getLastSync('patients');
  console.log('\n Syncing patients updated since ' + lastSync + '...');
  try {
    const items = await clinikoFetchAll('patients?per_page=' + PER_PAGE + '&updated_since=' + encodeURIComponent(lastSync), 'patients');
    const rows = items.map(function(p) {
      return {
        id: Number(p.id),
        first_name: p.first_name,
        last_name: p.last_name,
        dob: p.date_of_birth || null,
        referral_source: p.referral_source || null,
        created_at: p.created_at,
        updated_at: p.updated_at,
      };
    });
    await supabaseUpsert('patients', rows);
    await updateLastSync('patients');
    await writeSyncLog('patients', startedAt, rows.length, 'success');
    console.log('  OK: ' + rows.length + ' patients synced');
  } catch(e) {
    await writeSyncLog('patients', startedAt, 0, 'error', e.message);
    throw e;
  }
}

async function syncAppointments() {
  const startedAt = new Date().toISOString();
  const lastSync = await getLastSync('appointments');
  console.log('\n Syncing appointments updated since ' + lastSync + '...');
  try {
    // Cliniko uses 'individual_appointments' as the entity key
    const items = await clinikoFetchAll(
      'individual_appointments?per_page=' + PER_PAGE + '&updated_since=' + encodeURIComponent(lastSync),
      'individual_appointments'
    );

    if (items.length > 0) {
      console.log('  [Debug] Sample appointment keys:', Object.keys(items[0]).join(', '));
      console.log('  [Debug] Sample: cancelled_at=' + items[0].cancelled_at + ', did_not_arrive=' + items[0].did_not_arrive + ', patient_arrived=' + items[0].patient_arrived);
      // Find a cancelled one if any
      const cancelled = items.find(a => a.cancelled_at);
      if (cancelled) console.log('  [Debug] Found cancelled appt:', cancelled.id, 'cancelled_at:', cancelled.cancelled_at);
      else console.log('  [Debug] No cancelled appointments found in this batch');
    }

    const rows = items.map(function(a) {
      // Extract practitioner_id from links
      const pracLink = (a.practitioner && a.practitioner.links && a.practitioner.links.self)
        ? a.practitioner.links.self
        : ((a.links && a.links.practitioner) ? a.links.practitioner : '');
      const pracId = pracLink ? Number(pracLink.split('/').pop()) : null;

      // Extract patient_id from links
      const patLink = (a.patient && a.patient.links && a.patient.links.self)
        ? a.patient.links.self
        : ((a.links && a.links.patient) ? a.links.patient : '');
      const patId = patLink ? Number(patLink.split('/').pop()) : (a.patient && a.patient.id ? Number(a.patient.id) : null);

      const status = deriveStatus(a);

      return {
        id: Number(a.id),
        patient_id: patId,
        practitioner_id: pracId,
        appointment_type: (a.appointment_type && a.appointment_type.name) ? a.appointment_type.name : null,
        starts_at: a.starts_at,
        ends_at: a.ends_at || null,
        did_not_arrive: a.did_not_arrive === true,
        patient_arrived: a.patient_arrived === true,
        cancelled_at: a.cancelled_at || null,
        cancellation_note: a.cancellation_note || null,
        treatment_note_status: Number(a.treatment_note_status) || 0,
        is_group: false,
        status_clean: status,
        is_completed: status === 'completed',
        is_dna: status === 'dna',
        is_cancelled: status === 'cancelled',
        created_at: a.created_at,
        updated_at: a.updated_at,
      };
    });

    await supabaseUpsert('appointments', rows);
    await updateLastSync('appointments');
    await writeSyncLog('appointments', startedAt, rows.length, 'success');
    console.log('  OK: ' + rows.length + ' appointments synced');
    const cancelled = rows.filter(r => r.is_cancelled).length;
    const dna = rows.filter(r => r.is_dna).length;
    const completed = rows.filter(r => r.is_completed).length;
    console.log('  Status breakdown: completed=' + completed + ', cancelled=' + cancelled + ', dna=' + dna);
  } catch(e) {
    await writeSyncLog('appointments', startedAt, 0, 'error', e.message);
    throw e;
  }
}

async function syncInvoices() {
  const startedAt = new Date().toISOString();
  const lastSync = await getLastSync('invoices');
  console.log('\n Syncing invoices updated since ' + lastSync + '...');
  try {
    const items = await clinikoFetchAll('invoices?per_page=' + PER_PAGE + '&updated_since=' + encodeURIComponent(lastSync), 'invoices');

    if (items.length > 0) {
      console.log('  [Debug] Sample invoice keys:', Object.keys(items[0]).join(', '));
      console.log('  [Debug] Sample invoice total fields: net_amount=' + items[0].net_amount + ', total_amount=' + items[0].total_amount + ', amount_due=' + items[0].amount_due);
    }

    const rows = items.map(function(inv) {
      // Try all possible total fields Cliniko might use
      const total = parseFloat(inv.net_amount) || parseFloat(inv.total_amount) || parseFloat(inv.amount_due) || 0;

      const apptLink = (inv.appointment && inv.appointment.links && inv.appointment.links.self)
        ? inv.appointment.links.self
        : ((inv.links && inv.links.appointment) ? inv.links.appointment : '');
      const apptId = apptLink ? Number(apptLink.split('/').pop()) : null;

      const patLink = (inv.patient && inv.patient.links && inv.patient.links.self)
        ? inv.patient.links.self
        : ((inv.links && inv.links.patient) ? inv.links.patient : '');
      const patId = patLink ? Number(patLink.split('/').pop()) : null;

      const pracLink = (inv.practitioner && inv.practitioner.links && inv.practitioner.links.self)
        ? inv.practitioner.links.self
        : ((inv.links && inv.links.practitioner) ? inv.links.practitioner : '');
      const pracId = pracLink ? Number(pracLink.split('/').pop()) : null;

      return {
        id: Number(inv.id),
        patient_id: patId,
        appointment_id: apptId,
        practitioner_id: pracId,
        total_amount: total,
        status: inv.status || null,
        created_at: inv.created_at,
        updated_at: inv.updated_at,
      };
    });

    await supabaseUpsert('invoices', rows);
    await updateLastSync('invoices');
    await writeSyncLog('invoices', startedAt, rows.length, 'success');
    console.log('  OK: ' + rows.length + ' invoices synced');
    const totalRev = rows.reduce((s, r) => s + r.total_amount, 0);
    console.log('  Total revenue in batch: $' + totalRev.toFixed(2));
  } catch(e) {
    await writeSyncLog('invoices', startedAt, 0, 'error', e.message);
    throw e;
  }
}

async function updateAppointmentRevenue() {
  console.log('\n Updating appointment revenue from invoices...');
  const url = SUPABASE_URL + '/rest/v1/invoices?select=appointment_id,total_amount&appointment_id=not.is.null&limit=10000';
  const invoices = await httpGet(url, supabaseHeaders);
  const revenueMap = {};
  for (const inv of invoices) {
    const id = inv.appointment_id;
    if (!id) continue;
    revenueMap[id] = (revenueMap[id] || 0) + (parseFloat(inv.total_amount) || 0);
  }
  const updates = Object.entries(revenueMap);
  console.log('  Updating revenue for ' + updates.length + ' appointments...');
  let done = 0;
  for (const [apptId, revenue] of updates) {
    await httpRequest(
      'PATCH',
      SUPABASE_URL + '/rest/v1/appointments?id=eq.' + apptId,
      Object.assign({ 'Prefer': 'return=minimal' }, supabaseHeaders),
      { actual_revenue: revenue }
    );
    done++;
    if (done % 50 === 0) {
      console.log('  ... ' + done + '/' + updates.length);
      await sleep(200);
    }
  }
  console.log('  OK: Revenue updated for ' + done + ' appointments');
}

async function runSync() {
  console.log('============================================================');
  console.log('BEP Sync V4 - ' + new Date().toISOString());
  console.log('============================================================');
  const startTime = Date.now();
  let failed = false;

  try { await syncPractitioners(); } catch(e) { console.error('FAILED practitioners:', e.message); failed = true; }
  try { await syncPatients(); } catch(e) { console.error('FAILED patients:', e.message); failed = true; }
  try { await syncAppointments(); } catch(e) { console.error('FAILED appointments:', e.message); failed = true; }
  try { await syncInvoices(); } catch(e) { console.error('FAILED invoices:', e.message); failed = true; }
  if (!failed) {
    try { await updateAppointmentRevenue(); } catch(e) { console.error('FAILED revenue update:', e.message); }
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log('\n============================================================');
  console.log('Sync ' + (failed ? 'COMPLETED WITH ERRORS' : 'COMPLETE') + ' in ' + elapsed + 's');
  console.log('============================================================');
}

runSync().catch(function(e) {
  console.error('Fatal sync error:', e);
  process.exit(1);
});
