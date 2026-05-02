// BEP-SYNC-V8 (REWRITTEN - SAFE & DETERMINISTIC)

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

// -------------------- HTTP --------------------

function httpGet(url, headers) {
  return new Promise((resolve, reject) => {
    const lib = url.startsWith('https') ? https : http;

    lib.get(url, { headers }, (res) => {
      let body = '';

      res.on('data', c => body += c);

      res.on('end', () => {
        if (res.statusCode >= 400) {
          return reject(new Error(`HTTP ${res.statusCode}: ${body.slice(0, 300)}`));
        }
        try { resolve(JSON.parse(body)); }
        catch { reject(new Error('JSON parse error')); }
      });
    }).on('error', reject);
  });
}

function httpRequest(method, url, headers, body) {
  return new Promise((resolve, reject) => {
    const payload = body ? JSON.stringify(body) : '';
    const lib = url.startsWith('https') ? https : http;

    const req = lib.request(url, {
      method,
      headers: {
        ...headers,
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(payload)
      }
    }, (res) => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        if (res.statusCode >= 400) {
          return reject(new Error(`HTTP ${res.statusCode}: ${data.slice(0, 300)}`));
        }
        resolve(data ? JSON.parse(data) : {});
      });
    });

    req.on('error', reject);
    if (payload) req.write(payload);
    req.end();
  });
}

// -------------------- HEADERS --------------------

const clinikoHeaders = {
  Authorization: 'Basic ' + Buffer.from(CLINIKO_KEY + ':').toString('base64'),
  Accept: 'application/json',
  'User-Agent': 'BEP-Sync-V8'
};

const supabaseHeaders = {
  apikey: SUPABASE_KEY,
  Authorization: 'Bearer ' + SUPABASE_KEY
};

// -------------------- CORE HELPERS --------------------

const sleep = ms => new Promise(r => setTimeout(r, ms));

function extractId(obj) {
  if (!obj) return null;
  if (obj.links?.self) return Number(obj.links.self.split('/').pop());
  if (obj.id) return Number(obj.id);
  return null;
}

async function fetchAll(path, key) {
  let url = `${CLINIKO_BASE}/${path}`;
  const out = [];

  while (url) {
    const data = await httpGet(url, clinikoHeaders);
    out.push(...(data[key] || []));
    url = data.links?.next || null;
    await sleep(250);
  }

  return out;
}

// -------------------- UPSERT (STRICT) --------------------

async function upsert(table, rows) {
  if (!rows.length) return;

  const batchSize = 200;

  for (let i = 0; i < rows.length; i += batchSize) {
    const batch = rows.slice(i, i + batchSize);

    await httpRequest(
      'POST',
      `${SUPABASE_URL}/rest/v1/${table}?on_conflict=id`,
      {
        ...supabaseHeaders,
        Prefer: 'resolution=merge-duplicates,return=minimal'
      },
      batch
    );
  }
}

// -------------------- APPOINTMENTS --------------------

function mapAppointment(a, typeMap) {
  const apptTypeId = a.appointment_type?.links?.self?.split('/').pop() || null;

  const status =
    a.cancelled_at ? 'cancelled' :
    a.did_not_arrive ? 'dna' :
    new Date(a.starts_at) < new Date() ? 'completed' :
    'booked';

  return {
    id: Number(a.id),

    patient_id: extractId(a.patient),
    practitioner_id: extractId(a.practitioner),

    appointment_type: typeMap[apptTypeId] || null,

    starts_at: a.starts_at,
    ends_at: a.ends_at || null,

    did_not_arrive: !!a.did_not_arrive,
    patient_arrived: !!a.patient_arrived,

    status_clean: status,

    is_completed: status === 'completed',
    is_cancelled: status === 'cancelled',
    is_dna: status === 'dna',

    is_group: false, // FIX: no dual-source logic anymore

    attendee_count: a.max_attendees || 1,

    cancelled_at: a.cancelled_at || null,

    created_at: a.created_at,
    updated_at: a.updated_at
  };
}

// -------------------- SYNC TASKS --------------------

async function syncAppointmentTypes() {
  const items = await fetchAll('appointment_types?per_page=100', 'appointment_types');

  const map = {};
  items.forEach(t => {
    const id = t.links?.self?.split('/').pop() || t.id;
    map[id] = t.name;
  });

  return map;
}

async function syncAppointments() {
  const types = await syncAppointmentTypes();

  const active = await fetchAll(
    `individual_appointments?per_page=${PER_PAGE}`,
    'individual_appointments'
  );

  const cancelled = await fetchAll(
    `individual_appointments/cancelled?per_page=${PER_PAGE}`,
    'individual_appointments'
  );

  const all = new Map();

  // deterministic merge: same source rules only
  [...active, ...cancelled].forEach(a => {
    all.set(a.id, mapAppointment(a, types));
  });

  await upsert('appointments', [...all.values()]);
}

async function syncPatients() {
  const items = await fetchAll('patients?per_page=100', 'patients');

  await upsert('patients', items.map(p => ({
    id: Number(p.id),
    first_name: p.first_name,
    last_name: p.last_name,
    dob: p.date_of_birth || null,
    referral_source: p.referral_source || null,
    created_at: p.created_at,
    updated_at: p.updated_at
  })));
}

async function syncPractitioners() {
  const items = await fetchAll('practitioners?per_page=100', 'practitioners');

  await upsert('practitioners', items.map(p => ({
    id: Number(p.id),
    name: `${p.first_name} ${p.last_name}`.trim(),
    active: !p.archived_at,
    created_at: p.created_at,
    updated_at: p.updated_at
  })));
}

async function syncInvoices() {
  const items = await fetchAll('invoices?per_page=100', 'invoices');

  await upsert('invoices', items.map(inv => ({
    id: Number(inv.id),

    patient_id: extractId(inv.patient),
    appointment_id: extractId(inv.appointment),
    practitioner_id: extractId(inv.practitioner),

    total_amount: Number(inv.total_amount || 0),

    status: inv.status || null,

    issue_date: inv.issue_date || null,

    created_at: inv.created_at,
    updated_at: inv.updated_at
  })));
}

// -------------------- RUN --------------------

async function run() {
  console.log('BEP SYNC V8 START');

  await syncPractitioners();
  await syncPatients();
  await syncAppointments();
  await syncInvoices();

  console.log('BEP SYNC V8 COMPLETE');
}

run().catch(e => {
  console.error('Fatal:', e);
  process.exit(1);
});
