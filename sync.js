/**
 * BEP Dashboard — Cliniko → Supabase Sync Service
 *
 * Setup:
 *   npm install
 *   cp .env.example .env   (fill in your keys)
 *   node sync.js           (run manually or via cron)
 *
 * Cron (every 4 hours):
 *   0 */4 * * * cd /path/to/sync && node sync.js >> sync.log 2>&1
 */

require('dotenv').config();
const https  = require('https');
const http   = require('http');

// ── CONFIG ────────────────────────────────────────────────────
const CLINIKO_BASE  = 'https://api.au2.cliniko.com/v1';
const CLINIKO_KEY   = process.env.CLINIKO_API_KEY;
const SUPABASE_URL  = process.env.SUPABASE_URL;          // https://xxxx.supabase.co
const SUPABASE_KEY  = process.env.SUPABASE_SERVICE_KEY;  // service_role key (bypasses RLS)
const PER_PAGE      = 100;

if (!CLINIKO_KEY || !SUPABASE_URL || !SUPABASE_KEY) {
  console.error('Missing required env vars: CLINIKO_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_KEY');
  process.exit(1);
}

// ── HTTP HELPERS ──────────────────────────────────────────────

/** Simple promise-based HTTP GET */
function httpGet(url, headers = {}) {
  return new Promise((resolve, reject) => {
    const lib = url.startsWith('https') ? https : http;
    lib.get(url, { headers }, (res) => {
      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => {
        if (res.statusCode >= 400) {
          reject(new Error(`HTTP ${res.statusCode} on ${url}: ${body.slice(0, 300)}`));
          return;
        }
        try { resolve(JSON.parse(body)); }
        catch(e) { reject(new Error(`JSON parse error: ${body.slice(0, 200)}`)); }
      });
    }).on('error', reject);
  });
}

/** Simple promise-based HTTP POST/PATCH */
function httpRequest(method, url, headers = {}, body = null) {
  return new Promise((resolve, reject) => {
    const payload = body ? JSON.stringify(body) : '';
    const opts = {
      method,
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(payload),
        ...headers,
      },
    };
    const lib = url.startsWith('https') ? https : http;
    const urlObj = new URL(url);
    const req = lib.request(urlObj, opts, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        if (res.statusCode >= 400) {
          reject(new Error(`HTTP ${res.statusCode} on ${method} ${url}: ${data.slice(0, 300)}`));
          return;
        }
        try {
          resolve(data ? JSON.parse(data) : {});
        } catch(e) {
          resolve({});
        }
      });
    });
    req.on('error', reject);
    if (payload) req.write(payload);
    req.end();
  });
}

// ── CLINIKO HELPERS ───────────────────────────────────────────
const clinikoHeaders = {
  'Authorization': 'Basic ' + Buffer.from(CLINIKO_KEY + ':').toString('base64'),
  'Accept': 'application/json',
  'User-Agent': 'BEP-Dashboard-Sync/2.0 (admin@beachsideep.com.au)',
};

/** Fetch ALL pages from a Cliniko endpoint, following links.next */
async function clinikoFetchAll(path, entityKey) {
  const results = [];
  let url = `${CLINIKO_BASE}/${path}`;
  let page = 0;

  while (url) {
    page++;
    console.log(`  [Cliniko] page ${page}: ${url.replace(CLINIKO_BASE, '')}`);
    const data = await httpGet(url, clinikoHeaders);
    const items = data[entityKey] || [];
    results.push(...items);

    // Respect rate limits — Cliniko allows ~200 req/min
    await sleep(350);

    url = data.links?.next || null;
  }

  return results;
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

// ── SUPABASE HELPERS ──────────────────────────────────────────
const supabaseHeaders = {
  'apikey':        SUPABASE_KEY,
  'Authorization': 'Bearer ' + SUPABASE_KEY,
  'Prefer':        'resolution=merge-duplicates',
};

/** Upsert rows into a Supabase table in batches */
async function supabaseUpsert(table, rows, batchSize = 200) {
  if (rows.length === 0) return;
  for (let i = 0; i < rows.length; i += batchSize) {
    const batch = rows.slice(i, i + batchSize);
    await httpRequest(
      'POST',
      `${SUPABASE_URL}/rest/v1/${table}`,
      { ...supabaseHeaders, 'Prefer': 'resolution=merge-duplicates,return=minimal' },
      batch
    );
  }
}

/** Query Supabase sync_state for last sync time */
async function getLastSync(entity) {
  const url = `${SUPABASE_URL}/rest/v1/sync_state?entity=eq.${entity}&select=last_synced_at`;
  const data = await httpGet(url, supabaseHeaders);
  return (data[0]?.last_synced_at) || '2000-01-01T00:00:00Z';
}

/** Update sync_state after successful sync */
async function updateLastSync(entity) {
  await httpRequest(
    'PATCH',
    `${SUPABASE_URL}/rest/v1/sync_state?entity=eq.${entity}`,
    { ...supabaseHeaders, 'Prefer': 'return=minimal' },
    { last_synced_at: new Date().toISOString() }
  );
}

/** Write a sync log entry */
async function writeSyncLog(entity, startedAt, recordsProcessed, status, errorMessage = null) {
  await httpRequest(
    'POST',
    `${SUPABASE_URL}/rest/v1/sync_logs`,
    { ...supabaseHeaders, 'Prefer': 'return=minimal' },
    {
      entity,
      started_at:         startedAt,
      finished_at:        new Date().toISOString(),
      records_processed:  recordsProcessed,
      status,
      error_message:      errorMessage,
    }
  );
}

// ── STATUS NORMALISATION ──────────────────────────────────────
function deriveStatus(a) {
  if (a.cancelled_at || a.cancellation_time) return 'cancelled';
  if (a.did_not_arrive === true)             return 'dna';
  // Past appointment with no cancellation/DNA = completed
  const now = new Date();
  const start = new Date(a.starts_at);
  if (start < now) return 'completed';
  return 'booked';
}

// ── SYNC PRACTITIONERS ────────────────────────────────────────
async function syncPractitioners() {
  const startedAt = new Date().toISOString();
  console.log('\n👤 Syncing practitioners...');
  try {
    const items = await clinikoFetchAll(
      `practitioners?per_page=${PER_PAGE}`,
      'practitioners'
    );
    const rows = items.map(p => ({
      id:         Number(p.id),
      name:       `${p.first_name} ${p.last_name}`.trim(),
      first_name: p.first_name,
      last_name:  p.last_name,
      active:     !p.archived_at,
      created_at: p.created_at,
      updated_at: p.updated_at,
    }));
    await supabaseUpsert('practitioners', rows);
    await updateLastSync('practitioners');
    await writeSyncLog('practitioners', startedAt, rows.length, 'success');
    console.log(`  ✅ ${rows.length} practitioners synced`);
  } catch(e) {
    await writeSyncLog('practitioners', startedAt, 0, 'error', e.message);
    throw e;
  }
}

// ── SYNC PATIENTS ─────────────────────────────────────────────
async function syncPatients() {
  const startedAt = new Date().toISOString();
  const lastSync  = await getLastSync('patients');
  console.log(`\n👤 Syncing patients updated since ${lastSync}...`);
  try {
    const items = await clinikoFetchAll(
      `patients?per_page=${PER_PAGE}&updated_since=${encodeURIComponent(lastSync)}`,
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
    console.log(`  ✅ ${rows.length} patients synced`);
  } catch(e) {
    await writeSyncLog('patients', startedAt, 0, 'error', e.message);
    throw e;
  }
}

// ── SYNC APPOINTMENTS ─────────────────────────────────────────
async function syncAppointments() {
  const startedAt = new Date().toISOString();
  const lastSync  = await getLastSync('appointments');
  console.log(`\n📅 Syncing appointments updated since ${lastSync}...`);
  try {
    const items = await clinikoFetchAll(
      `appointments?per_page=${PER_PAGE}&updated_since=${encodeURIComponent(lastSync)}`,
      'appointments'
    );

    const rows = items.map(a => {
      // Extract practitioner_id from links
      const pracLink = a.practitioner?.links?.self || a.links?.practitioner || '';
      const pracId   = pracLink ? Number(pracLink.split('/').pop()) : null;

      // Extract patient_id from links
      const patLink  = a.patient?.links?.self || a.links?.patient || '';
      const patId    = patLink ? Number(patLink.split('/').pop()) : (a.patient?.id ? Number(a.patient.id) : null);

      // Extract appointment type
      const typeName = a.appointment_type?.links?.self
        ? null  // we don't resolve type names in sync — too many requests
        : (a.appointment_type?.name || null);

      const status = deriveStatus(a);

      return {
        id:                     Number(a.id),
        patient_id:             patId,
        practitioner_id:        pracId,
        appointment_type:       typeName,
        starts_at:              a.starts_at,
        ends_at:                a.ends_at || a.appointment_end,
        did_not_arrive:         a.did_not_arrive === true,
        patient_arrived:        a.patient_arrived === true,
        cancelled_at:           a.cancelled_at || a.cancellation_time || null,
        cancellation_note:      a.cancellation_note || null,
        treatment_note_status:  Number(a.treatment_note_status) || 0,
        is_group:               !!(a.patient_ids?.length > 1 || a.max_attendees > 1),
        status_clean:           status,
        is_completed:           status === 'completed',
        is_dna:                 status === 'dna',
        is_cancelled:           status === 'cancelled',
        created_at:             a.created_at,
        updated_at:             a.updated_at,
      };
    });

    await supabaseUpsert('appointments', rows);
    await updateLastSync('appointments');
    await writeSyncLog('appointments', startedAt, rows.length, 'success');
    console.log(`  ✅ ${rows.length} appointments synced`);
  } catch(e) {
    await writeSyncLog('appointments', startedAt, 0, 'error', e.message);
    throw e;
  }
}

// ── SYNC INVOICES ─────────────────────────────────────────────
async function syncInvoices() {
  const startedAt = new Date().toISOString();
  const lastSync  = await getLastSync('invoices');
  console.log(`\n💰 Syncing invoices updated since ${lastSync}...`);
  try {
    const items = await clinikoFetchAll(
      `invoices?per_page=${PER_PAGE}&updated_since=${encodeURIComponent(lastSync)}`,
      'invoices'
    );

    const rows = items.map(inv => {
      // Sum all line items
      const total = (inv.invoice_items || []).reduce((sum, item) => {
        return sum + (parseFloat(item.total_including_tax) || parseFloat(item.price) || 0);
      }, 0);

      // Extract appointment_id from links
      const apptLink = inv.appointment?.links?.self || inv.links?.appointment || '';
      const apptId   = apptLink ? Number(apptLink.split('/').pop()) : null;

      // Extract patient_id
      const patLink  = inv.patient?.links?.self || inv.links?.patient || '';
      const patId    = patLink ? Number(patLink.split('/').pop()) : null;

      // Extract practitioner_id
      const pracLink = inv.practitioner?.links?.self || inv.links?.practitioner || '';
      const pracId   = pracLink ? Number(pracLink.split('/').pop()) : null;

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
    console.log(`  ✅ ${rows.length} invoices synced`);
  } catch(e) {
    await writeSyncLog('invoices', startedAt, 0, 'error', e.message);
    throw e;
  }
}

// ── UPDATE REVENUE ON APPOINTMENTS ───────────────────────────
async function updateAppointmentRevenue() {
  console.log('\n💲 Updating appointment revenue from invoices...');
  // Pull all invoices with an appointment_id and aggregate
  const url = `${SUPABASE_URL}/rest/v1/invoices?select=appointment_id,total_amount&appointment_id=not.is.null`;
  const invoices = await httpGet(url, supabaseHeaders);

  // Aggregate by appointment_id
  const revenueMap = {};
  for (const inv of invoices) {
    const id = inv.appointment_id;
    if (!id) continue;
    revenueMap[id] = (revenueMap[id] || 0) + (parseFloat(inv.total_amount) || 0);
  }

  // Batch update appointments
  const updates = Object.entries(revenueMap);
  console.log(`  Updating revenue for ${updates.length} appointments...`);
  let done = 0;
  for (const [apptId, revenue] of updates) {
    await httpRequest(
      'PATCH',
      `${SUPABASE_URL}/rest/v1/appointments?id=eq.${apptId}`,
      { ...supabaseHeaders, 'Prefer': 'return=minimal' },
      { actual_revenue: revenue }
    );
    done++;
    // Throttle slightly
    if (done % 50 === 0) {
      console.log(`  ... ${done}/${updates.length}`);
      await sleep(200);
    }
  }
  console.log(`  ✅ Revenue updated for ${done} appointments`);
}

// ── MAIN ──────────────────────────────────────────────────────
async function runSync() {
  console.log('='.repeat(60));
  console.log(`BEP Sync — ${new Date().toISOString()}`);
  console.log('='.repeat(60));

  const startTime = Date.now();
  let failed = false;

  try {
    await syncPractitioners();
  } catch(e) { console.error('  ❌ Practitioners failed:', e.message); failed = true; }

  try {
    await syncPatients();
  } catch(e) { console.error('  ❌ Patients failed:', e.message); failed = true; }

  try {
    await syncAppointments();
  } catch(e) { console.error('  ❌ Appointments failed:', e.message); failed = true; }

  try {
    await syncInvoices();
  } catch(e) { console.error('  ❌ Invoices failed:', e.message); failed = true; }

  if (!failed) {
    try {
      await updateAppointmentRevenue();
    } catch(e) { console.error('  ❌ Revenue update failed:', e.message); }
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`\n${'='.repeat(60)}`);
  console.log(`Sync ${failed ? 'COMPLETED WITH ERRORS' : 'COMPLETE'} in ${elapsed}s`);
  console.log('='.repeat(60));
}

runSync().catch(e => {
  console.error('Fatal sync error:', e);
  process.exit(1);
});
