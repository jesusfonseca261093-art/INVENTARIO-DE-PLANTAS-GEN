// ── DATA ─────────────────────────────────────────────────────────────
const PARTS = [
  { no:"1",  pn:"CAT-01", qty:1, desc:"Valvula Check Lock 3/4\"",                brand:"" },
  { no:"2",  pn:"CAT-02", qty:1, desc:"Valvula de llenado 3\"",                   brand:"" },
  { no:"3",  pn:"CAT-03", qty:1, desc:"Valvula No Retroceso 3\"",                 brand:"" },
  { no:"4",  pn:"CAT-04", qty:1, desc:"Valvula Para Carburacion 3/4\"",           brand:"" },
  { no:"5",  pn:"CAT-05", qty:1, desc:"Valvula de Retorno de Vapores 1/4\"",      brand:"" },
  { no:"6",  pn:"CAT-06", qty:1, desc:"Valvula No Retroceso 1 1/4\"",             brand:"" },
  { no:"7",  pn:"CAT-07", qty:1, desc:"Valvula de Maximo llenado (1)",            brand:"" },
  { no:"8",  pn:"CAT-08", qty:1, desc:"Valvula de Maximo llenado (2)",            brand:"" },
  { no:"9",  pn:"CAT-09", qty:1, desc:"Valvula de Maximo llenado (3)",            brand:"" },
  { no:"10", pn:"CAT-10", qty:1, desc:"Valvula Interna de 2\"",                   brand:"" },
  { no:"11", pn:"CAT-11", qty:1, desc:"Valvula de Seguridad 3\"",                 brand:"" },
  { no:"12", pn:"CAT-12", qty:1, desc:"Manguera De Sumistro Modelo 20BHB",        brand:"" },
  { no:"13", pn:"CAT-13", qty:1, desc:"Sello de Seguridad",                       brand:"" },
];

const STATION_PARTS = [
  { no:"1",  pn:"SERIE 8530", qty:1, corresponde:"Tanques de almacenamiento", desc:"DELTAPORTT", brand:"" },
  { no:"2",  pn:"SERIE A8560 / A8570", qty:1, corresponde:"Tanques de almacenamiento", desc:"MULTIPORTTM", brand:"" },
  { no:"3",  pn:"SERIE A3186", qty:1, corresponde:"Tanques de almacenamiento", desc:"VALVULA BACK CHECK", brand:"" },
  { no:"4",  pn:"SERIE A7500", qty:1, corresponde:"Tanques de almacenamiento", desc:"VALVULA DE ANGULO", brand:"" },
  { no:"5",  pn:"SERIE 3181", qty:1, corresponde:"Tanques de almacenamiento", desc:"CONECTOR ACME HEMBRA", brand:"" },
  { no:"6",  pn:"SERIE A7500", qty:1, corresponde:"Tanques de almacenamiento", desc:"VALVULA DE GLOBO", brand:"" },
  { no:"7",  pn:"SERIE A7790", qty:1, corresponde:"Tanques de almacenamiento", desc:"MIRILLA DE FLUJO", brand:"" },
  { no:"8",  pn:"SERIE 3127 / SS8000", qty:1, corresponde:"Tanques de almacenamiento", desc:"VALVULA DE ALIVIO HIDROSTATICO", brand:"" },
  { no:"9",  pn:"SERIE A7500", qty:1, corresponde:"Tanques de almacenamiento", desc:"VALVULA DE GLOBO", brand:"" },
  { no:"10", pn:"SERIE A7500", qty:1, corresponde:"Tanques de almacenamiento", desc:"VALVULA DE GLOBO", brand:"" },
  { no:"11", pn:"SERIE A3186", qty:1, corresponde:"Tanques de almacenamiento", desc:"VALVULA BACK CHECK", brand:"" },
  { no:"12", pn:"SERIE A9090", qty:1, corresponde:"Tanques de almacenamiento", desc:"INDICADOR ROTOGAGE", brand:"" },
  { no:"13", pn:"SERIE 7534", qty:1, corresponde:"Tanques de almacenamiento", desc:"VALVULA DE SEGURIDAD DE ALIVIO DE PRESION", brand:"" },
  { no:"14", pn:"SERIE 3127", qty:1, corresponde:"Gabinete", desc:"VALVULA DE ALIVIO HIDROSTATICO", brand:"" },
  { no:"15", pn:"3272H", qty:1, corresponde:"Gabinete", desc:"VALVULA DE EXCESO DE FLUJO", brand:"" },
  { no:"16", pn:"SERIE 7590", qty:1, corresponde:"Gabinete", desc:"VALVULA CHECK-LOK", brand:"" }
];

function stationPartSignature(part) {
  const desc = String(part?.desc || '').trim().toLowerCase();
  const pn = String(part?.pn || '').trim().toLowerCase();
  const corresponde = String(part?.corresponde || '').trim().toLowerCase();
  return `${desc}|${pn}|${corresponde}`;
}

function buildUniqueStationParts(parts) {
  const map = new Map();
  (Array.isArray(parts) ? parts : []).forEach(part => {
    const key = stationPartSignature(part);
    if (!map.has(key)) {
      map.set(key, {
        ...part,
        allNos: [String(part.no || '').trim()]
      });
      return;
    }
    const current = map.get(key);
    const currentNo = String(part.no || '').trim();
    if (currentNo && !current.allNos.includes(currentNo)) current.allNos.push(currentNo);
  });
  return Array.from(map.values()).map(p => ({
    ...p,
    allNos: p.allNos.sort((a, b) => compareTextNatural(a, b)),
    no: p.allNos[0],
    repeats: p.allNos.length
  }));
}

const STATION_PARTS_UNIQUE = buildUniqueStationParts(STATION_PARTS);

const ESTACIONES_CARBURACION = [
  {
    planta: 'QUERETARO',
    estaciones: [
      { nombre: 'QUERO', bombas: ['9001-B018', '9002-B017', '9003-B019', '9004-B020'] },
      { nombre: 'BANCHIS', bombas: ['9008 B021', '9009-B022', '9010-B023', '9011 B024', '9015 B040', '9016 B041'] },
      { nombre: 'SOL', bombas: ['9005 B002', '9006-B003', '9007-B004', '9012-B027'] }
    ]
  },
  {
    planta: 'BALVANERA',
    estaciones: [
      { nombre: 'EJIDO LOS ANGELES', bombas: ['BOMBA 1'] },
      { nombre: 'PEÑA FLOR', bombas: ['BP05', 'BP06', 'BP09', 'BP08'] }
    ]
  },
  {
    planta: 'GALERAS',
    estaciones: [
      { nombre: 'GALERAS', bombas: ['BOMBA 1'] },
      { nombre: 'EL MARQUEZ', bombas: ['BOMBA 1'] },
      { nombre: 'EL SAUZ', bombas: ['BOMBA 1'] },
      { nombre: 'SAN IGNACIO', bombas: ['BOMBA 1'] }
    ]
  },
  {
    planta: 'PEDRO ESCOBEDO',
    estaciones: [
      { nombre: 'PEDRO ESCOBEDO', bombas: ['BOMBA 1'] }
    ]
  }
];
const STATIONS_STORAGE_KEY = 'at_estaciones';
const STATION_RECORDS_STORAGE_KEY = 'at_station_records';
const MAINTENANCE_STORAGE_KEY = 'at_maintenance_schedule';
const CHANGE_HISTORY_STORAGE_KEY = 'at_change_history';
const CHANGE_HISTORY_PENDING_STORAGE_KEY = 'at_change_history_pending';
const CHANGE_HISTORY_LIMIT = 5000;

// ── STATE ─────────────────────────────────────────────────────────────
let autotanques = JSON.parse(localStorage.getItem('at_units') || '[]');
let records     = JSON.parse(localStorage.getItem('at_records') || '[]');
let partImages  = JSON.parse(localStorage.getItem('at_part_images') || '{}');
let estaciones  = loadEstaciones();
let stationRecords = loadStationRecords();
let maintenanceSchedule = loadMaintenanceSchedule();
let selectedPart = null;
let editingATId  = null;
let currentDraftATId = null;
let editingRecordId = null;
let editingEstacionId = null;
let maintenanceEditingAtId = null;
let maintenanceEditingEntryId = null;
let maintenanceFullCalendar = null;
let expandedAutotanquePlantKey = null;
let expandedPlantGroupKey = null;
let expandedStationGroupKey = null;
let expandedReplGroups = new Set();
let draftExpedienteDocs = [];
let pendingExpedienteDeletePaths = [];
let originalExpedientePaths = new Set();
let appBootstrapped = false;
let authIdleTimer = null;
let authActivityBound = false;
let authLogoutInProgress = false;
let replMatrixColorFilter = '';
let changeHistory = loadChangeHistory();
let changeAuditReady = false;
let lastAuditSnapshot = null;
let activeAuditUser = 'Sistema';

const EXPEDIENTE_CATEGORIES = {
  seguro: 'Seguro de la unidad',
  permiso: 'Permisos',
  pago: 'Pagos',
  verificacion: 'Verificacion / inspeccion',
  otro: 'Otro'
};
const COMPONENT_GUIDE_IMAGE = 'assets/img/diagrama_componentes_autotanque.png';
const STATION_COMPONENT_GUIDE_IMAGE = 'assets/img/diagrama_componentes_autotanque.png';
const PLANTAS_ACTUALES = ['QUERETARO', 'BALVANERA', 'GALERAS', 'PEDRO ESCOBEDO'];
const MAX_DOC_SIZE_LOCAL_BYTES = 1.5 * 1024 * 1024;
const MAX_DOC_SIZE_SUPABASE_BYTES = 10 * 1024 * 1024;
const MAX_PART_IMAGE_SIZE_BYTES = 3 * 1024 * 1024;
const AUTH_IDLE_TIMEOUT_MINUTES = 15;
const AUTH_IDLE_TIMEOUT_MS = AUTH_IDLE_TIMEOUT_MINUTES * 60 * 1000;
const REPLACEMENT_YEARS = 10;
const LEGACY_REPLACEMENT_YEARS = 5;
const HOSE_20BHB_MAX_YEARS = 5;
const HOSE_20BHB_PREVIOUS_YEARS = 7;
const MATRIX_WARNING_DAYS = 60;
const UNIT_META_MARKER = '[GEN_UNIT_META]';
const SB_URL_KEY = 'sb_url';
const SB_ANON_KEY = 'sb_anon_key';
const APP_SUPABASE_CONFIG = window.SUPABASE_CONFIG || {};
const FILE_SUPABASE_URL = String(APP_SUPABASE_CONFIG.url || '').trim();
const FILE_SUPABASE_ANON = String(APP_SUPABASE_CONFIG.anonKey || '').trim();
const HAS_FILE_SUPABASE_CONFIG = Boolean(FILE_SUPABASE_URL && FILE_SUPABASE_ANON);
const SUPABASE_URL = HAS_FILE_SUPABASE_CONFIG ? FILE_SUPABASE_URL : (localStorage.getItem(SB_URL_KEY) || '').trim();
const SUPABASE_ANON = HAS_FILE_SUPABASE_CONFIG ? FILE_SUPABASE_ANON : (localStorage.getItem(SB_ANON_KEY) || '').trim();
const SUPABASE_TABLE_UNITS = String(APP_SUPABASE_CONFIG.tableUnits || 'at_units').trim() || 'at_units';
const SUPABASE_TABLE_RECORDS = String(APP_SUPABASE_CONFIG.tableRecords || 'at_records').trim() || 'at_records';
const SUPABASE_TABLE_PART_IMAGES = String(APP_SUPABASE_CONFIG.tablePartImages || 'at_part_images').trim() || 'at_part_images';
const SUPABASE_TABLE_STATION_RECORDS = String(APP_SUPABASE_CONFIG.tableStationRecords || 'at_station_records').trim() || 'at_station_records';
const SUPABASE_TABLE_CHANGE_HISTORY = String(APP_SUPABASE_CONFIG.tableChangeHistory || 'at_change_history').trim() || 'at_change_history';
const SUPABASE_BUCKET_EXPEDIENTE = String(APP_SUPABASE_CONFIG.bucketExpediente || 'at_expediente').trim() || 'at_expediente';
const SUPABASE_ENABLED = Boolean(SUPABASE_URL && SUPABASE_ANON && window.supabase?.createClient);
const SUPABASE_CONFIG_SOURCE = HAS_FILE_SUPABASE_CONFIG
  ? 'archivo (assets/config/supabase.config.js)'
  : (SUPABASE_ENABLED ? 'localStorage' : 'sin configurar');
const supabaseClient = SUPABASE_ENABLED ? window.supabase.createClient(SUPABASE_URL, SUPABASE_ANON) : null;
let runtimeUseSupabase = SUPABASE_ENABLED;
let runtimeUseSupabaseStationRecords = SUPABASE_ENABLED;
let runtimeUseSupabaseChangeHistory = SUPABASE_ENABLED;
let changeHistorySyncInFlight = false;
let pendingChangeHistoryRemote = loadPendingChangeHistoryRemote();
const SUPABASE_SETUP_SQL = `
create table if not exists public.at_units (
  id text primary key,
  econ text not null,
  placa text not null,
  planta_actual text,
  serie_unidad text,
  serie_tanque text,
  capacidad text,
  anio text,
  notas text,
  expediente jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now()
);

create table if not exists public.at_records (
  id text primary key,
  at_id text not null references public.at_units(id) on delete cascade,
  part_no text not null,
  part_pn text,
  part_desc text,
  part_brand text,
  fab_date date,
  inst_date date,
  repl_date date,
  serial text,
  brand text,
  notes text,
  created_at timestamptz not null default now()
);

create table if not exists public.at_part_images (
  part_no text primary key,
  file_name text,
  mime_type text,
  size_bytes integer,
  data_url text not null,
  updated_at timestamptz not null default now()
);

create table if not exists public.at_station_records (
  id text primary key,
  station_id text not null,
  part_no text not null,
  part_pn text,
  part_desc text,
  part_brand text,
  fab_date date,
  inst_date date,
  repl_date date,
  serial text,
  brand text,
  notes text,
  created_at timestamptz not null default now()
);

create table if not exists public.at_change_history (
  id text primary key,
  event_at timestamptz not null,
  action text,
  movement text not null,
  user_name text,
  created_at timestamptz not null default now()
);

alter table public.at_units add column if not exists planta_actual text;

alter table public.at_units enable row level security;
alter table public.at_records enable row level security;
alter table public.at_part_images enable row level security;
alter table public.at_station_records enable row level security;
alter table public.at_change_history enable row level security;

drop policy if exists "at_units_all" on public.at_units;
create policy "at_units_all" on public.at_units
for all
using (auth.role() = 'authenticated')
with check (auth.role() = 'authenticated');

drop policy if exists "at_records_all" on public.at_records;
create policy "at_records_all" on public.at_records
for all
using (auth.role() = 'authenticated')
with check (auth.role() = 'authenticated');

drop policy if exists "at_part_images_all" on public.at_part_images;
create policy "at_part_images_all" on public.at_part_images
for all
using (auth.role() = 'authenticated')
with check (auth.role() = 'authenticated');

drop policy if exists "at_station_records_all" on public.at_station_records;
create policy "at_station_records_all" on public.at_station_records
for all
using (auth.role() = 'authenticated')
with check (auth.role() = 'authenticated');

drop policy if exists "at_change_history_all" on public.at_change_history;
create policy "at_change_history_all" on public.at_change_history
for all
using (auth.role() = 'authenticated')
with check (auth.role() = 'authenticated');

insert into storage.buckets (id, name, public)
values ('${SUPABASE_BUCKET_EXPEDIENTE}', '${SUPABASE_BUCKET_EXPEDIENTE}', false)
on conflict (id) do nothing;

drop policy if exists "at_expediente_rw" on storage.objects;
create policy "at_expediente_rw"
on storage.objects
for all
using (bucket_id = '${SUPABASE_BUCKET_EXPEDIENTE}' and auth.role() = 'authenticated')
with check (bucket_id = '${SUPABASE_BUCKET_EXPEDIENTE}' and auth.role() = 'authenticated');
`.trim();

autotanques = normalizeAutotanques(autotanques);
partImages = normalizePartImages(partImages);
lastAuditSnapshot = buildAuditSnapshot();

function save() {
  const nextSnapshot = buildAuditSnapshot();
  const pendingHistoryEntries = changeAuditReady
    ? buildChangeHistoryEntries(lastAuditSnapshot, nextSnapshot)
    : [];

  if (runtimeUseSupabase) {
    // Siempre mantenemos cache local de imagenes por seguridad, aun en modo Supabase.
    try { localStorage.setItem('at_part_images', JSON.stringify(partImages)); } catch {}
    try { localStorage.setItem(STATIONS_STORAGE_KEY, JSON.stringify(estaciones)); } catch {}
    try { localStorage.setItem(STATION_RECORDS_STORAGE_KEY, JSON.stringify(stationRecords)); } catch {}
    if (pendingHistoryEntries.length) {
      appendChangeHistoryEntries(pendingHistoryEntries);
      renderChangeHistory();
    }
    lastAuditSnapshot = nextSnapshot;
    return true;
  }
  try {
    localStorage.setItem('at_units', JSON.stringify(autotanques));
    localStorage.setItem('at_records', JSON.stringify(records));
    localStorage.setItem('at_part_images', JSON.stringify(partImages));
    localStorage.setItem(STATIONS_STORAGE_KEY, JSON.stringify(estaciones));
    localStorage.setItem(STATION_RECORDS_STORAGE_KEY, JSON.stringify(stationRecords));
    if (pendingHistoryEntries.length) {
      appendChangeHistoryEntries(pendingHistoryEntries);
      renderChangeHistory();
    }
    lastAuditSnapshot = nextSnapshot;
    return true;
  } catch (err) {
    alert('No se pudo guardar en almacenamiento local. Reduce tamano de archivos del expediente o imagenes e intenta de nuevo.');
    return false;
  }
}

function normalizeMaintenanceEntry(entry) {
  return {
    id: String(entry?.id || genId()),
    atId: String(entry?.atId || '').trim(),
    maintDate: String(entry?.maintDate || '').trim(),
    notes: String(entry?.notes || '').trim(),
    createdAt: String(entry?.createdAt || new Date().toISOString()),
    updatedAt: String(entry?.updatedAt || entry?.createdAt || new Date().toISOString())
  };
}

function loadMaintenanceSchedule() {
  const raw = localStorage.getItem(MAINTENANCE_STORAGE_KEY);
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed
      .map(normalizeMaintenanceEntry)
      .filter(item => item.atId && item.maintDate);
  } catch {
    return [];
  }
}

function persistMaintenanceSchedule() {
  try {
    localStorage.setItem(MAINTENANCE_STORAGE_KEY, JSON.stringify(maintenanceSchedule));
  } catch {}
}

function getMaintenanceEntriesByAtId(atId) {
  return maintenanceSchedule
    .filter(item => item.atId === atId)
    .sort((a, b) => String(a.maintDate || '').localeCompare(String(b.maintDate || '')));
}

function getMaintenanceStatusByDate(dateStr) {
  const days = daysUntil(dateStr);
  if (days === null) return { key: 'sin-fecha', label: 'Sin fecha' };
  if (days < 0) return { key: 'finalizado', label: 'Finalizado' };
  if (days === 0) return { key: 'en-proceso', label: 'En proceso' };
  return { key: 'programado', label: 'Programado' };
}

function getTodayDateInputValue() {
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, '0');
  const day = String(now.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

function resetMaintenanceForm(presetDate = '') {
  const dateInput = document.getElementById('maintenanceDate');
  const notesInput = document.getElementById('maintenanceNotes');
  const saveBtn = document.getElementById('maintenanceSaveBtn');
  if (dateInput) dateInput.value = presetDate || getTodayDateInputValue();
  if (notesInput) notesInput.value = '';
  maintenanceEditingEntryId = null;
  if (saveBtn) saveBtn.textContent = 'GUARDAR';
}

function populateMaintenanceAtSelect(selectedAtId = '') {
  const select = document.getElementById('maintenanceAtSelect');
  if (!select) return;
  const options = autotanques
    .map(unit => ({
      id: String(unit?.id || ''),
      econ: String(unit?.econ || '—'),
      placa: String(unit?.placa || '—'),
      planta: String(unit?.plantaActual || 'SIN PLANTA')
    }))
    .sort((a, b) =>
      compareTextNatural(a.planta, b.planta) ||
      compareTextNatural(a.econ, b.econ) ||
      compareTextNatural(a.placa, b.placa)
    );

  select.innerHTML = `
    <option value="">— Seleccionar autotanque —</option>
    ${options.map(item => `<option value="${escapeHtml(item.id)}">${escapeHtml(item.econ)} | ${escapeHtml(item.placa)} | ${escapeHtml(item.planta)}</option>`).join('')}
  `;
  if (selectedAtId && options.some(item => item.id === selectedAtId)) {
    select.value = selectedAtId;
  }
}

function setMaintenanceTargetAt(atId) {
  const selectedAtId = String(atId || '').trim();
  const at = autotanques.find(item => item.id === selectedAtId);
  const labelEl = document.getElementById('maintenanceAtLabel');
  const plantEl = document.getElementById('maintenanceAtPlant');
  const select = document.getElementById('maintenanceAtSelect');

  if (!at) {
    maintenanceEditingAtId = null;
    if (labelEl) labelEl.textContent = 'Selecciona un autotanque';
    if (plantEl) plantEl.textContent = 'Planta: —';
    if (select) select.value = '';
    const wrap = document.getElementById('maintenanceHistory');
    if (wrap) wrap.innerHTML = '<p class="text-muted">Selecciona un autotanque para ver su historial.</p>';
    return;
  }

  maintenanceEditingAtId = at.id;
  if (labelEl) labelEl.textContent = `${at.econ || '—'} | ${at.placa || '—'}`;
  if (plantEl) plantEl.textContent = `Planta: ${at.plantaActual || 'SIN PLANTA'}`;
  if (select && select.value !== at.id) select.value = at.id;
  renderMaintenanceHistory(at.id);
}

function onMaintenanceAtSelectChange() {
  const select = document.getElementById('maintenanceAtSelect');
  const atId = String(select?.value || '').trim();
  maintenanceEditingEntryId = null;
  const saveBtn = document.getElementById('maintenanceSaveBtn');
  if (saveBtn) saveBtn.textContent = 'GUARDAR';
  setMaintenanceTargetAt(atId);
}

function renderMaintenanceHistory(atId) {
  const wrap = document.getElementById('maintenanceHistory');
  if (!wrap) return;
  const entries = getMaintenanceEntriesByAtId(atId);
  if (!entries.length) {
    wrap.innerHTML = '<p class="text-muted">Sin mantenimientos registrados.</p>';
    return;
  }
  wrap.innerHTML = `<table><thead><tr><th>FECHA</th><th>ESTADO</th><th>NOTAS</th><th>ACCIONES</th></tr></thead><tbody>
    ${entries.map(item => {
      const status = getMaintenanceStatusByDate(item.maintDate);
      return `<tr>
        <td>${formatDate(item.maintDate)}</td>
        <td><span class="badge ${status.key === 'programado' ? 'badge-warn' : status.key === 'en-proceso' ? 'badge-danger' : 'badge-ok'}">${status.label.toUpperCase()}</span></td>
        <td style="white-space:pre-wrap;overflow-wrap:anywhere">${escapeHtml(item.notes || '—')}</td>
        <td>
          <div class="flex-gap">
            <button class="btn btn-secondary" style="padding:4px 8px;font-size:10px" onclick="editMaintenanceEntry('${item.id}')">✏️ EDITAR</button>
            <button class="btn btn-danger" style="padding:4px 8px;font-size:10px" onclick="deleteMaintenanceEntry('${item.id}')">🗑 ELIMINAR</button>
          </div>
        </td>
      </tr>`;
    }).join('')}
  </tbody></table>`;
}

function editMaintenanceEntry(entryId) {
  const entry = maintenanceSchedule.find(item => item.id === entryId && item.atId === maintenanceEditingAtId);
  if (!entry) return alert('No se encontró el registro de mantenimiento.');
  maintenanceEditingEntryId = entry.id;
  const dateInput = document.getElementById('maintenanceDate');
  const notesInput = document.getElementById('maintenanceNotes');
  const saveBtn = document.getElementById('maintenanceSaveBtn');
  if (dateInput) dateInput.value = entry.maintDate || getTodayDateInputValue();
  if (notesInput) notesInput.value = entry.notes || '';
  if (saveBtn) saveBtn.textContent = 'ACTUALIZAR';
}

function deleteMaintenanceEntry(entryId) {
  const entry = maintenanceSchedule.find(item => item.id === entryId && item.atId === maintenanceEditingAtId);
  if (!entry) return alert('No se encontró el registro de mantenimiento.');
  if (!confirm(`¿Eliminar mantenimiento del ${formatDate(entry.maintDate)}?`)) return;

  maintenanceSchedule = maintenanceSchedule.filter(item => item.id !== entry.id);
  persistMaintenanceSchedule();
  if (maintenanceEditingEntryId === entry.id) {
    resetMaintenanceForm();
  }
  renderMaintenanceHistory(maintenanceEditingAtId);
  renderDashboard();
  renderReemplazosExpiryMatrix();
}

function openMaintenanceModal(atId = '', presetDate = '') {
  const selectedAtId = String(atId || '').trim();
  const at = autotanques.find(item => item.id === selectedAtId);
  const selectWrap = document.getElementById('maintenanceAtSelectWrap');

  document.getElementById('modalMaintenanceTitle').textContent = 'MANTENIMIENTO AUTOTANQUE';
  populateMaintenanceAtSelect(at?.id || '');
  if (selectWrap) {
    selectWrap.style.display = at ? 'none' : 'block';
  }
  resetMaintenanceForm(presetDate);
  setMaintenanceTargetAt(at?.id || '');
  document.getElementById('modalMaintenance').classList.add('open');
}

function saveMaintenanceEntry() {
  if (!maintenanceEditingAtId) return alert('No hay autotanque seleccionado para mantenimiento.');
  const maintDate = document.getElementById('maintenanceDate')?.value || '';
  if (!maintDate) return alert('Selecciona la fecha de mantenimiento.');
  const notes = document.getElementById('maintenanceNotes')?.value || '';

  if (maintenanceEditingEntryId) {
    const idx = maintenanceSchedule.findIndex(item => item.id === maintenanceEditingEntryId && item.atId === maintenanceEditingAtId);
    if (idx < 0) return alert('No se encontró el registro de mantenimiento a editar.');
    const current = maintenanceSchedule[idx];
    maintenanceSchedule[idx] = normalizeMaintenanceEntry({
      ...current,
      maintDate,
      notes,
      updatedAt: new Date().toISOString()
    });
  } else {
    maintenanceSchedule.push(normalizeMaintenanceEntry({
      id: genId(),
      atId: maintenanceEditingAtId,
      maintDate,
      notes,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString()
    }));
  }
  maintenanceSchedule.sort((a, b) => String(a.maintDate || '').localeCompare(String(b.maintDate || '')));
  persistMaintenanceSchedule();
  renderMaintenanceHistory(maintenanceEditingAtId);
  renderDashboard();
  renderReemplazosExpiryMatrix();
  resetMaintenanceForm();
}

function renderDashboardMaintenanceCalendar() {
  const wrap = document.getElementById('dashboardMaintenanceCalendar');
  if (!wrap) return;

  const rows = maintenanceSchedule
    .map(item => {
      const at = autotanques.find(unit => unit.id === item.atId);
      if (!at) return null;
      return {
        ...item,
        econ: at.econ || '—',
        placa: at.placa || '—',
        planta: at.plantaActual || 'SIN PLANTA',
        status: getMaintenanceStatusByDate(item.maintDate)
      };
    })
    .filter(Boolean)
    .sort((a, b) => String(a.maintDate || '').localeCompare(String(b.maintDate || '')));

  if (!rows.length) {
    wrap.innerHTML = '<p class="text-muted">Sin mantenimientos programados.</p>';
    return;
  }

  wrap.innerHTML = `<table><thead><tr><th>FECHA</th><th>AUTOTANQUE</th><th>PLANTA</th><th>ESTADO</th><th>NOTAS</th></tr></thead><tbody>
    ${rows.map(row => {
      const statusClass = row.status.key === 'programado'
        ? 'badge-warn'
        : (row.status.key === 'en-proceso' ? 'badge-danger' : 'badge-ok');
      return `<tr>
        <td style="font-family:monospace">${formatDate(row.maintDate)}</td>
        <td><b style="color:var(--accent)">${escapeHtml(row.econ)}</b> | ${escapeHtml(row.placa)}</td>
        <td>${escapeHtml(row.planta)}</td>
        <td><span class="badge ${statusClass}">${row.status.label.toUpperCase()}</span></td>
        <td style="white-space:pre-wrap;overflow-wrap:anywhere">${escapeHtml(row.notes || '—')}</td>
      </tr>`;
    }).join('')}
  </tbody></table>`;
}

function getMaintenanceCalendarColor(statusKey) {
  if (statusKey === 'en-proceso') return '#dc2626';
  if (statusKey === 'finalizado') return '#16a34a';
  if (statusKey === 'programado') return '#0ea5e9';
  return '#64748b';
}

function buildReplMaintenanceCalendarEvents() {
  return maintenanceSchedule
    .map(item => {
      const at = autotanques.find(unit => unit.id === item.atId);
      if (!at) return null;
      const status = getMaintenanceStatusByDate(item.maintDate);
      const color = getMaintenanceCalendarColor(status.key);
      const title = `${at.econ || '—'} | ${at.placa || '—'}`;
      return {
        id: item.id,
        title,
        start: item.maintDate,
        allDay: true,
        backgroundColor: color,
        borderColor: color,
        textColor: '#ffffff',
        extendedProps: {
          atId: item.atId,
          planta: at.plantaActual || 'SIN PLANTA',
          notes: item.notes || '',
          statusLabel: status.label
        }
      };
    })
    .filter(Boolean);
}

function renderReplMaintenanceCalendar() {
  const calendarEl = document.getElementById('replMaintenanceCalendar');
  const emptyEl = document.getElementById('replMaintenanceCalendarEmpty');
  if (!calendarEl) return;

  const hasFullCalendar = Boolean(window.FullCalendar && window.FullCalendar.Calendar);
  if (!hasFullCalendar) {
    if (emptyEl) {
      emptyEl.style.display = 'block';
      emptyEl.textContent = 'No se pudo cargar FullCalendar.';
    }
    return;
  }

  const events = buildReplMaintenanceCalendarEvents();
  if (!maintenanceFullCalendar) {
    maintenanceFullCalendar = new window.FullCalendar.Calendar(calendarEl, {
      locale: 'es',
      initialView: 'dayGridMonth',
      height: 520,
      contentHeight: 440,
      expandRows: true,
      handleWindowResize: true,
      headerToolbar: {
        left: 'prev,next today',
        center: 'title',
        right: 'dayGridMonth,timeGridWeek,listMonth'
      },
      buttonText: {
        today: 'Hoy',
        month: 'Mes',
        week: 'Semana',
        list: 'Lista'
      },
      dayMaxEventRows: true,
      dateClick: (info) => {
        const selectedDate = String(info?.dateStr || '').trim();
        openMaintenanceModal('', selectedDate);
      },
      eventClick: (info) => {
        const atId = String(info?.event?.extendedProps?.atId || '').trim();
        const entryId = String(info?.event?.id || '').trim();
        if (!atId) return;
        openMaintenanceModal(atId);
        if (entryId) editMaintenanceEntry(entryId);
      },
      events
    });
    maintenanceFullCalendar.render();
    setTimeout(() => maintenanceFullCalendar?.updateSize?.(), 120);
  } else {
    maintenanceFullCalendar.removeAllEvents();
    maintenanceFullCalendar.addEventSource(events);
    maintenanceFullCalendar.updateSize();
    setTimeout(() => maintenanceFullCalendar?.updateSize?.(), 80);
  }

  if (emptyEl) {
    emptyEl.style.display = events.length ? 'none' : 'block';
    emptyEl.textContent = events.length ? '' : 'Sin mantenimientos programados.';
  }
}

function loadChangeHistory() {
  const raw = localStorage.getItem(CHANGE_HISTORY_STORAGE_KEY);
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed
      .map(item => ({
        id: String(item?.id || genId()),
        timestamp: String(item?.timestamp || ''),
        action: String(item?.action || ''),
        movement: String(item?.movement || ''),
        user: String(item?.user || 'Sistema')
      }))
      .filter(item => item.timestamp && item.movement);
  } catch {
    return [];
  }
}

function persistChangeHistory() {
  try {
    localStorage.setItem(CHANGE_HISTORY_STORAGE_KEY, JSON.stringify(changeHistory));
  } catch {}
}

function normalizeChangeHistoryEntries(entries) {
  return (Array.isArray(entries) ? entries : [])
    .map(item => ({
      id: String(item?.id || genId()),
      timestamp: String(item?.timestamp || new Date().toISOString()),
      action: String(item?.action || '').trim(),
      movement: String(item?.movement || '').trim(),
      user: String(item?.user || 'Sistema').trim() || 'Sistema'
    }))
    .filter(item => item.timestamp && item.movement);
}

function sortChangeHistoryDesc(list) {
  return [...(Array.isArray(list) ? list : [])].sort((a, b) =>
    String(b?.timestamp || '').localeCompare(String(a?.timestamp || ''))
  );
}

function mergeChangeHistoryLists(...lists) {
  const map = new Map();
  lists.flat().forEach(item => {
    const id = String(item?.id || '').trim();
    if (!id) return;
    if (!map.has(id)) map.set(id, item);
  });
  return sortChangeHistoryDesc(Array.from(map.values()));
}

function loadPendingChangeHistoryRemote() {
  const raw = localStorage.getItem(CHANGE_HISTORY_PENDING_STORAGE_KEY);
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw);
    return normalizeChangeHistoryEntries(parsed);
  } catch {
    return [];
  }
}

function persistPendingChangeHistoryRemote() {
  try {
    localStorage.setItem(CHANGE_HISTORY_PENDING_STORAGE_KEY, JSON.stringify(pendingChangeHistoryRemote));
  } catch {}
}

function mapChangeHistoryToDb(entry) {
  return {
    id: entry.id,
    event_at: entry.timestamp || new Date().toISOString(),
    action: entry.action || null,
    movement: entry.movement || null,
    user_name: entry.user || null
  };
}

function mapChangeHistoryFromDb(row) {
  return {
    id: String(row?.id || genId()),
    timestamp: String(row?.event_at || row?.created_at || ''),
    action: String(row?.action || ''),
    movement: String(row?.movement || ''),
    user: String(row?.user_name || 'Sistema')
  };
}

function queueChangeHistoryForRemote(entries) {
  const normalized = normalizeChangeHistoryEntries(entries);
  if (!normalized.length) return;
  pendingChangeHistoryRemote = mergeChangeHistoryLists(pendingChangeHistoryRemote, normalized);
  if (pendingChangeHistoryRemote.length > CHANGE_HISTORY_LIMIT) {
    pendingChangeHistoryRemote = pendingChangeHistoryRemote.slice(0, CHANGE_HISTORY_LIMIT);
  }
  persistPendingChangeHistoryRemote();
  void flushPendingChangeHistoryRemote();
}

async function flushPendingChangeHistoryRemote() {
  if (!runtimeUseSupabase || !runtimeUseSupabaseChangeHistory || !supabaseClient) return false;
  if (changeHistorySyncInFlight) return false;
  if (!pendingChangeHistoryRemote.length) return true;

  changeHistorySyncInFlight = true;
  try {
    while (pendingChangeHistoryRemote.length) {
      const batch = pendingChangeHistoryRemote.slice(0, 200);
      const { error } = await supabaseClient
        .from(SUPABASE_TABLE_CHANGE_HISTORY)
        .upsert(batch.map(mapChangeHistoryToDb), { onConflict: 'id' });
      if (error) {
        if (isMissingChangeHistoryTableError(error)) {
          runtimeUseSupabaseChangeHistory = false;
          updateStorageModeLabel(`tabla ${SUPABASE_TABLE_CHANGE_HISTORY} no existe; historial solo local`);
        } else {
          console.warn('No se pudo sincronizar historial con Supabase:', error);
        }
        persistPendingChangeHistoryRemote();
        return false;
      }
      pendingChangeHistoryRemote = pendingChangeHistoryRemote.slice(batch.length);
      persistPendingChangeHistoryRemote();
    }
    return true;
  } finally {
    changeHistorySyncInFlight = false;
  }
}

function getAuditUserDisplayName() {
  const value = String(activeAuditUser || '').trim();
  return value || 'Sistema';
}

function buildAuditSnapshot() {
  const units = {};
  const recordsById = {};
  const stationsById = {};
  const stationRecordsById = {};
  const partImagesByKey = {};
  const unitLabelById = {};
  const stationLabelById = {};

  (Array.isArray(autotanques) ? autotanques : []).forEach(at => {
    const id = String(at?.id || '').trim();
    if (!id) return;
    const econ = String(at?.econ || '').trim();
    const placa = String(at?.placa || '').trim();
    const label = econ ? `${econ}${placa ? ` (${placa})` : ''}` : id;
    unitLabelById[id] = label;
    units[id] = {
      label,
      data: {
        econ,
        placa,
        plantaActual: String(at?.plantaActual || '').trim(),
        serieUnidad: String(at?.serieUnidad || '').trim(),
        serieTanque: String(at?.serieTanque || '').trim(),
        capacidad: String(at?.capacidad || '').trim(),
        anio: String(at?.anio || '').trim(),
        notas: String(at?.notas || '').trim(),
        activo: Boolean(at?.activo),
        enServicio: Boolean(at?.enServicio),
        marcaUnidad: String(at?.marcaUnidad || '').trim(),
        modeloUnidad: String(at?.modeloUnidad || '').trim(),
        dictamenNomMes: String(at?.dictamenNomMes || '').trim(),
        dictamenNomAnio: String(at?.dictamenNomAnio || '').trim(),
        nom013Mes: String(at?.nom013Mes || '').trim(),
        nom013Anio: String(at?.nom013Anio || '').trim(),
        nom007SeshMes: String(at?.nom007SeshMes || '').trim(),
        nom007SeshAnio: String(at?.nom007SeshAnio || '').trim(),
        registroSener: String(at?.registroSener || '').trim(),
        noRegTagSener: String(at?.noRegTagSener || '').trim(),
        expedienteCount: getVisibleExpedienteDocs(at?.expediente).length
      }
    };
  });

  (Array.isArray(estaciones) ? estaciones : []).forEach(st => {
    const id = String(st?.id || '').trim();
    if (!id) return;
    const planta = String(st?.planta || '').trim();
    const estacion = String(st?.estacion || '').trim();
    const bomba = String(st?.bomba || '').trim();
    const label = [planta, estacion, bomba].filter(Boolean).join(' / ') || id;
    stationLabelById[id] = label;
    stationsById[id] = {
      label,
      data: {
        planta,
        estacion,
        bomba,
        componentesCount: Array.isArray(st?.componentes) ? st.componentes.length : 0,
        expedienteCount: Array.isArray(st?.expediente) ? st.expediente.length : 0
      }
    };
  });

  (Array.isArray(records) ? records : []).forEach(rec => {
    const id = String(rec?.id || '').trim();
    if (!id) return;
    const atId = String(rec?.atId || '').trim();
    const atLabel = unitLabelById[atId] || atId || 'Sin unidad';
    const partLabel = String(rec?.partDesc || rec?.partNo || '').trim() || 'Componente';
    recordsById[id] = {
      label: `${partLabel} - ${atLabel}`,
      data: {
        atId,
        partNo: String(rec?.partNo || '').trim(),
        partPn: String(rec?.partPn || '').trim(),
        partDesc: String(rec?.partDesc || '').trim(),
        partBrand: String(rec?.partBrand || '').trim(),
        fabDate: String(rec?.fabDate || '').trim(),
        instDate: String(rec?.instDate || '').trim(),
        replDate: String(rec?.replDate || '').trim(),
        serial: String(rec?.serial || '').trim(),
        brand: String(rec?.brand || '').trim(),
        notes: String(rec?.notes || '').trim()
      }
    };
  });

  (Array.isArray(stationRecords) ? stationRecords : []).forEach(rec => {
    const id = String(rec?.id || '').trim();
    if (!id) return;
    const stationId = String(rec?.stationId || '').trim();
    const stationLabel = stationLabelById[stationId] || stationId || 'Sin estación';
    const partLabel = String(rec?.partDesc || rec?.partNo || '').trim() || 'Componente';
    stationRecordsById[id] = {
      label: `${partLabel} - ${stationLabel}`,
      data: {
        stationId,
        partNo: String(rec?.partNo || '').trim(),
        partPn: String(rec?.partPn || '').trim(),
        partDesc: String(rec?.partDesc || '').trim(),
        partBrand: String(rec?.partBrand || '').trim(),
        fabDate: String(rec?.fabDate || '').trim(),
        instDate: String(rec?.instDate || '').trim(),
        replDate: String(rec?.replDate || '').trim(),
        serial: String(rec?.serial || '').trim(),
        brand: String(rec?.brand || '').trim(),
        notes: String(rec?.notes || '').trim()
      }
    };
  });

  Object.entries(partImages || {}).forEach(([partNo, img]) => {
    const key = String(partNo || '').trim();
    if (!key) return;
    const safeImg = img && typeof img === 'object' ? img : {};
    partImagesByKey[key] = {
      label: `Imagen de pieza ${key}`,
      data: {
        fileName: String(safeImg.fileName || '').trim(),
        mimeType: String(safeImg.mimeType || '').trim(),
        sizeBytes: Number(safeImg.sizeBytes || 0) || 0,
        updatedAt: String(safeImg.updatedAt || '').trim(),
        dataUrlLength: String(safeImg.dataUrl || '').length
      }
    };
  });

  return {
    units,
    records: recordsById,
    stations: stationsById,
    stationRecords: stationRecordsById,
    partImages: partImagesByKey
  };
}

function stableStringify(value) {
  if (value === null || value === undefined) return String(value);
  if (typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(',')}]`;
  const keys = Object.keys(value).sort();
  return `{${keys.map(k => `${JSON.stringify(k)}:${stableStringify(value[k])}`).join(',')}}`;
}

function getChangedFieldList(prevData, nextData) {
  const keys = Array.from(new Set([
    ...Object.keys(prevData || {}),
    ...Object.keys(nextData || {})
  ])).sort(compareTextNatural);
  return keys.filter(key => stableStringify(prevData?.[key]) !== stableStringify(nextData?.[key]));
}

function diffSnapshotCollection(previousCollection, nextCollection, entityName) {
  const previous = previousCollection || {};
  const next = nextCollection || {};
  const prevKeys = Object.keys(previous);
  const nextKeys = Object.keys(next);
  const allKeys = Array.from(new Set([...prevKeys, ...nextKeys])).sort(compareTextNatural);
  const rows = [];

  allKeys.forEach(key => {
    const prevItem = previous[key];
    const nextItem = next[key];
    if (!prevItem && nextItem) {
      rows.push({
        id: genId(),
        action: 'ALTA',
        movement: `ALTA: ${entityName} "${nextItem.label}".`
      });
      return;
    }
    if (prevItem && !nextItem) {
      rows.push({
        id: genId(),
        action: 'ELIMINACIÓN',
        movement: `ELIMINACIÓN: ${entityName} "${prevItem.label}".`
      });
      return;
    }
    if (!prevItem || !nextItem) return;

    const prevHash = stableStringify(prevItem.data);
    const nextHash = stableStringify(nextItem.data);
    if (prevHash === nextHash) return;

    const changedFields = getChangedFieldList(prevItem.data, nextItem.data);
    const changedText = changedFields.length ? ` Campos: ${changedFields.join(', ')}.` : '';
    rows.push({
      id: genId(),
      action: 'MODIFICACIÓN',
      movement: `MODIFICACIÓN: ${entityName} "${nextItem.label}".${changedText}`
    });
  });

  return rows;
}

function buildChangeHistoryEntries(previousSnapshot, nextSnapshot) {
  if (!previousSnapshot || !nextSnapshot) return [];
  const nowIso = new Date().toISOString();
  const user = getAuditUserDisplayName();
  const out = [];
  const append = entries => entries.forEach(entry => out.push({ ...entry, timestamp: nowIso, user }));

  append(diffSnapshotCollection(previousSnapshot.units, nextSnapshot.units, 'Autotanque'));
  append(diffSnapshotCollection(previousSnapshot.records, nextSnapshot.records, 'Registro de componente'));
  append(diffSnapshotCollection(previousSnapshot.stations, nextSnapshot.stations, 'Estación'));
  append(diffSnapshotCollection(previousSnapshot.stationRecords, nextSnapshot.stationRecords, 'Registro de estación'));
  append(diffSnapshotCollection(previousSnapshot.partImages, nextSnapshot.partImages, 'Imagen de pieza'));
  return out;
}

function appendChangeHistoryEntries(entries) {
  const normalized = normalizeChangeHistoryEntries(entries);
  if (!normalized.length) return;
  changeHistory = mergeChangeHistoryLists(normalized, changeHistory).slice(0, CHANGE_HISTORY_LIMIT);
  persistChangeHistory();
  if (runtimeUseSupabase) queueChangeHistoryForRemote(normalized);
}

function renderChangeHistory() {
  const tbody = document.getElementById('changeHistoryBody');
  const summary = document.getElementById('changeHistorySummary');
  if (!tbody || !summary) return;

  if (!changeHistory.length) {
    summary.textContent = 'Sin cambios registrados.';
    tbody.innerHTML = '<tr><td colspan="3" style="text-align:center;color:var(--muted);padding:20px">Sin cambios registrados.</td></tr>';
    return;
  }

  const visibleRows = changeHistory.slice(0, 300);
  summary.textContent = `Total de movimientos: ${changeHistory.length}. Mostrando los últimos ${visibleRows.length}.`;
  tbody.innerHTML = visibleRows.map(item => `
    <tr>
      <td>${escapeHtml(formatDateTime(item.timestamp))}</td>
      <td>${escapeHtml(item.movement)}</td>
      <td>${escapeHtml(item.user || 'Sistema')}</td>
    </tr>
  `).join('');
}

// ── UTILS ─────────────────────────────────────────────────────────────
function genId() { return Date.now().toString(36) + Math.random().toString(36).slice(2,6); }

function buildDefaultEstaciones() {
  const out = [];
  ESTACIONES_CARBURACION.forEach(block => {
    const planta = String(block.planta || '').trim();
    (block.estaciones || []).forEach(est => {
      const estacion = String(est.nombre || '').trim();
      (est.bombas || []).forEach(b => {
        const bomba = String(b || '').trim();
        const key = `${planta}|${estacion}|${bomba}`;
        out.push({
          id: normalizeCsvHeader(key) || genId(),
          planta,
          estacion,
          bomba,
          expediente: [],
          componentes: []
        });
      });
    });
  });
  return out;
}

function normalizeEstaciones(list) {
  if (!Array.isArray(list)) return [];
  return list.map(row => ({
    id: String(row?.id || genId()),
    planta: String(row?.planta || '').trim(),
    estacion: String(row?.estacion || '').trim(),
    bomba: String(row?.bomba || '').trim(),
    expediente: Array.isArray(row?.expediente) ? row.expediente : [],
    componentes: Array.isArray(row?.componentes) ? row.componentes.map(x => String(x || '').trim()).filter(Boolean) : []
  })).filter(row => row.planta && row.estacion && row.bomba);
}

function loadEstaciones() {
  const raw = localStorage.getItem(STATIONS_STORAGE_KEY);
  if (!raw) return buildDefaultEstaciones();
  try {
    const parsed = JSON.parse(raw);
    const normalized = normalizeEstaciones(parsed);
    return normalized.length ? normalized : buildDefaultEstaciones();
  } catch {
    return buildDefaultEstaciones();
  }
}

function normalizeStationRecords(list) {
  if (!Array.isArray(list)) return [];
  return list.map(row => ({
    id: String(row?.id || genId()),
    stationId: String(row?.stationId || '').trim(),
    partNo: String(row?.partNo || '').trim(),
    partPn: String(row?.partPn || '').trim(),
    partDesc: String(row?.partDesc || '').trim(),
    partBrand: String(row?.partBrand || '').trim(),
    fabDate: String(row?.fabDate || '').trim(),
    instDate: String(row?.instDate || '').trim(),
    replDate: String(row?.replDate || '').trim(),
    serial: String(row?.serial || '').trim(),
    brand: String(row?.brand || '').trim(),
    notes: String(row?.notes || '').trim(),
    createdAt: String(row?.createdAt || new Date().toISOString())
  })).filter(row => row.stationId && row.partNo);
}

function loadStationRecords() {
  const raw = localStorage.getItem(STATION_RECORDS_STORAGE_KEY);
  if (!raw) return [];
  try {
    return normalizeStationRecords(JSON.parse(raw));
  } catch {
    return [];
  }
}

function compareTextNatural(a, b) {
  return String(a || '').localeCompare(String(b || ''), 'es', { numeric: true, sensitivity: 'base' });
}

const NO_VALVE_NOTE_TAG = 'SIN VALVULA';

function ensureNoValveTagInNotes(notes, hasFabDate) {
  const raw = String(notes || '').trim();
  if (hasFabDate) return raw;
  const hasTag = new RegExp(`\\b${NO_VALVE_NOTE_TAG.replace(' ', '\\s+')}\\b`, 'i').test(raw);
  if (hasTag) return raw;
  return raw ? `${NO_VALVE_NOTE_TAG} | ${raw}` : NO_VALVE_NOTE_TAG;
}

function sortUnitsByMode(list, mode = 'econ-asc') {
  const sorted = [...(Array.isArray(list) ? list : [])];
  sorted.sort((a, b) => {
    const econCmp = compareTextNatural(a?.econ, b?.econ);
    switch (mode) {
      case 'econ-desc': return -econCmp;
      case 'placa-asc': {
        const cmp = compareTextNatural(a?.placa, b?.placa);
        return cmp || econCmp;
      }
      case 'placa-desc': {
        const cmp = compareTextNatural(a?.placa, b?.placa);
        return cmp ? -cmp : -econCmp;
      }
      case 'planta-asc': {
        const cmp = compareTextNatural(a?.plantaActual, b?.plantaActual);
        return cmp || econCmp;
      }
      case 'planta-desc': {
        const cmp = compareTextNatural(a?.plantaActual, b?.plantaActual);
        return cmp ? -cmp : -econCmp;
      }
      case 'econ-asc':
      default:
        return econCmp;
    }
  });
  return sorted;
}

const MONTH_NAMES = [
  '', 'enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio',
  'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre'
];

const FAB_WEEK_MAP = {
  A: 1,
  B: 2,
  C: 3,
  D: 4,
  E: 5
};

function parseFabCode(rawCode) {
  const raw = String(rawCode || '').trim();
  if (!raw) return null;

  const normalized = raw.replace(/^\-+/, '').trim().toUpperCase();

  // Formato fecha normal: DD/MM/YYYY o DD-MM-YYYY
  const dateMatch = normalized.match(/^(\d{1,2})[\/-](\d{1,2})[\/-](\d{4})$/);
  if (dateMatch) {
    const day = Number(dateMatch[1]);
    const month = Number(dateMatch[2]);
    const year = Number(dateMatch[3]);
    if (month < 1 || month > 12 || day < 1) return null;
    const lastDay = new Date(year, month, 0).getDate();
    if (day > lastDay) return null;
    const iso = `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
    return {
      type: 'date',
      iso,
      input: normalized,
      day,
      month,
      year
    };
  }

  // Formato ISO: YYYY-MM-DD (por compatibilidad)
  const isoMatch = normalized.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (isoMatch) {
    const year = Number(isoMatch[1]);
    const month = Number(isoMatch[2]);
    const day = Number(isoMatch[3]);
    const lastDay = new Date(year, month, 0).getDate();
    if (month < 1 || month > 12 || day < 1 || day > lastDay) return null;
    return {
      type: 'date',
      iso: normalized,
      input: normalized,
      day,
      month,
      year
    };
  }

  // Formato codigo tipo imagen: Mes-Digito/Semana-Letra/Año-2dig
  // Ejemplos: 6A92, 9C22, 09C22
  const cleaned = normalized.replace(/[\s/_-]/g, '');
  const match = cleaned.match(/^(\d{1,2})([A-E])(\d{2})$/);
  if (!match) return null;

  const month = Number(match[1]);
  const weekLetter = match[2];
  const week = FAB_WEEK_MAP[weekLetter];
  const year2 = Number(match[3]);
  if (!week || month < 1 || month > 12) return null;

  // Regla solicitada: desde 1990 (90 -> 1990, 91 -> 1991 ... 00 -> 2000 ...)
  const year = year2 >= 90 ? 1900 + year2 : 2000 + year2;
  const lastDay = new Date(year, month, 0).getDate();
  const baseDay = 1 + ((week - 1) * 7);
  const day = Math.min(baseDay, lastDay);
  const iso = `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
  return {
    type: 'code',
    code: cleaned,
    iso,
    month,
    monthName: MONTH_NAMES[month] || '',
    weekLetter,
    week,
    year
  };
}

function updateReplacementFromFabInput() {
  const fabInputEl = document.getElementById('formFabDate');
  const replInputEl = document.getElementById('formReplDate');
  const hintEl = document.getElementById('formFabHint');
  if (!fabInputEl || !replInputEl) return null;

  const parsed = parseFabCode(fabInputEl.value);
  if (!parsed) {
    replInputEl.value = '';
    if (hintEl) hintEl.innerHTML = 'Formatos válidos: <b>6A92</b>, <b>9C22</b>, <b>09C22</b> o <b>27/04/2026</b>.';
    return null;
  }

  const years = getReplacementYearsForPart(selectedPart);
  replInputEl.value = addYears(parsed.iso, years);
  if (hintEl) {
    const yearsNote = years === HOSE_20BHB_MAX_YEARS
      ? `Regla aplicada: <b>${years} años</b> máximo para Manguera Modelo 20BHB.`
      : `Regla aplicada: <b>${years} años</b>.`;
    if (parsed.type === 'date') {
      hintEl.innerHTML = `Fecha capturada: <b>${parsed.iso}</b>. ${yearsNote}`;
    } else {
      hintEl.innerHTML = `Codigo <b>${parsed.code}</b>: mes ${parsed.month} (${parsed.monthName}), semana ${parsed.weekLetter} (${parsed.week}ra), año ${parsed.year}. Fecha base: <b>${parsed.iso}</b>. ${yearsNote}`;
    }
  }
  return parsed;
}

function addYears(dateStr, years) {
  if (!dateStr) return '';
  const raw = String(dateStr).trim();
  const isoMatch = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (isoMatch) {
    const year = Number(isoMatch[1]);
    const month = Number(isoMatch[2]);
    const day = Number(isoMatch[3]);
    const nextYear = year + Number(years || 0);
    const maxDay = new Date(nextYear, month, 0).getDate();
    const nextDay = Math.min(day, maxDay);
    return `${nextYear}-${String(month).padStart(2, '0')}-${String(nextDay).padStart(2, '0')}`;
  }
  const d = new Date(raw);
  if (Number.isNaN(d.getTime())) return '';
  d.setFullYear(d.getFullYear() + years);
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
}

function isHose20BHBPart(part = {}) {
  const desc = normalizeMatchText(part?.desc || part?.partDesc || '');
  const pn = normalizeMatchText(part?.pn || part?.partPn || '');
  return desc.includes('manguera modelo 20bhb') || pn.includes('20bhb');
}

function getReplacementYearsForPart(part = {}) {
  return isHose20BHBPart(part) ? HOSE_20BHB_MAX_YEARS : REPLACEMENT_YEARS;
}

function applyReplacementPolicyToRecord(rec) {
  const safeRec = { ...(rec || {}) };
  const fabDate = String(safeRec.fabDate || '').trim();
  if (!fabDate) return { record: safeRec, changed: false };

  const targetYears = getReplacementYearsForPart({
    partNo: safeRec.partNo,
    partPn: safeRec.partPn,
    partDesc: safeRec.partDesc
  });
  const replTarget = addYears(fabDate, targetYears);
  if (!replTarget) return { record: safeRec, changed: false };

  const currentRepl = String(safeRec.replDate || '').trim();
  if (!currentRepl) {
    safeRec.replDate = replTarget;
    return { record: safeRec, changed: true };
  }
  if (currentRepl === replTarget) {
    return { record: safeRec, changed: false };
  }

  const repl5 = addYears(fabDate, LEGACY_REPLACEMENT_YEARS);
  const repl10 = addYears(fabDate, REPLACEMENT_YEARS);
  const replPrevHose = addYears(fabDate, HOSE_20BHB_PREVIOUS_YEARS);
  const isHose = isHose20BHBPart({
    partNo: safeRec.partNo,
    partPn: safeRec.partPn,
    partDesc: safeRec.partDesc
  });
  if (
    (repl5 && currentRepl === repl5) ||
    (targetYears !== REPLACEMENT_YEARS && repl10 && currentRepl === repl10) ||
    (isHose && replPrevHose && currentRepl === replPrevHose)
  ) {
    safeRec.replDate = replTarget;
    return { record: safeRec, changed: true };
  }

  return { record: safeRec, changed: false };
}

function applyReplacementPolicyToRecords(list) {
  const changed = [];
  const normalized = (Array.isArray(list) ? list : []).map(rec => {
    const result = applyReplacementPolicyToRecord(rec);
    if (result.changed) changed.push(result.record);
    return result.record;
  });
  return { records: normalized, changed };
}

function daysUntil(dateStr) {
  if (!dateStr) return null;
  const now = new Date(); now.setHours(0,0,0,0);
  const d = new Date(dateStr);
  return Math.round((d - now) / 86400000);
}

function getNormExpeditionDate(unit, normKey = 'legacy') {
  let monthRaw = '';
  let yearRaw = '';
  if (normKey === 'nom013') {
    monthRaw = String(unit?.nom013Mes || '').trim();
    yearRaw = String(unit?.nom013Anio || '').trim();
  } else if (normKey === 'nom007sesh') {
    monthRaw = String(unit?.nom007SeshMes || '').trim();
    yearRaw = String(unit?.nom007SeshAnio || '').trim();
  } else {
    monthRaw = String(unit?.dictamenNomMes || '').trim();
    yearRaw = String(unit?.dictamenNomAnio || '').trim();
  }
  // Compatibilidad: si no hay campos nuevos, usar fecha legacy.
  if ((!monthRaw || !yearRaw) && normKey !== 'legacy') {
    monthRaw = String(unit?.dictamenNomMes || '').trim();
    yearRaw = String(unit?.dictamenNomAnio || '').trim();
  }
  const month = Number.parseInt(monthRaw, 10);
  const year = Number.parseInt(yearRaw, 10);
  if (!Number.isInteger(month) || !Number.isInteger(year)) return '';
  if (month < 1 || month > 12) return '';
  if (year < 1900 || year > 2100) return '';
  return `${year}-${String(month).padStart(2, '0')}-01`;
}

function getNormExpiryDate(unit, yearsToAdd, normKey = 'legacy') {
  const expeditionDate = getNormExpeditionDate(unit, normKey);
  if (!expeditionDate) return '';
  return addYears(expeditionDate, yearsToAdd);
}

function buildMatrixDateCell(dateValue, extraClass = '') {
  const cls = extraClass ? ` ${extraClass}` : '';
  if (!dateValue) return `<td class="matrix-empty${cls}">—</td>`;
  const d = daysUntil(dateValue);
  const txt = formatDate(dateValue);
  if (d !== null && d < 0) return `<td class="matrix-expired${cls}">${txt}</td>`;
  if (d !== null && d <= MATRIX_WARNING_DAYS) return `<td class="matrix-critical${cls}">${txt}</td>`;
  return `<td class="matrix-ok${cls}">${txt}</td>`;
}

function formatDate(d) {
  if (!d) return '—';
  const [y,m,dia] = d.split('-');
  return `${dia}/${m}/${y}`;
}

function statusBadge(days) {
  if (days === null) return '<span class="badge badge-none">SIN FECHA</span>';
  if (days < 0)   return '<span class="badge badge-danger">VENCIDO</span>';
  if (days <= 90)  return '<span class="badge badge-danger">CRÍTICO</span>';
  if (days <= 180) return '<span class="badge badge-warn">PRÓXIMO</span>';
  return '<span class="badge badge-ok">VIGENTE</span>';
}

function statusKey(days) {
  if (days === null) return 'sin-fecha';
  if (days < 0)   return 'vencido';
  if (days <= 90)  return 'critico';
  if (days <= 180) return 'proximo';
  return 'vigente';
}

function parseUnitNotesWithMeta(rawNotes) {
  const text = String(rawNotes || '');
  const inlineMarkerIndex = text.lastIndexOf(UNIT_META_MARKER);
  if (inlineMarkerIndex < 0) return { notes: text, meta: {} };

  const notesPart = text.slice(0, inlineMarkerIndex).replace(/\s+$/, '');
  const metaRaw = text.slice(inlineMarkerIndex + UNIT_META_MARKER.length).trim();
  if (!metaRaw) return { notes: notesPart, meta: {} };

  try {
    const parsed = JSON.parse(metaRaw);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      return { notes: notesPart, meta: {} };
    }
    return { notes: notesPart, meta: parsed };
  } catch {
    return { notes: text, meta: {} };
  }
}

function buildUnitMetaPayload(at) {
  return {
    activo: Boolean(at?.activo),
    enServicio: Boolean(at?.enServicio),
    marcaUnidad: String(at?.marcaUnidad || '').trim(),
    modeloUnidad: String(at?.modeloUnidad || '').trim(),
    dictamenNomMes: String(at?.dictamenNomMes || '').trim(),
    dictamenNomAnio: String(at?.dictamenNomAnio || '').trim(),
    nom013Mes: String(at?.nom013Mes || '').trim(),
    nom013Anio: String(at?.nom013Anio || '').trim(),
    nom007SeshMes: String(at?.nom007SeshMes || '').trim(),
    nom007SeshAnio: String(at?.nom007SeshAnio || '').trim(),
    registroSener: String(at?.registroSener || '').trim(),
    noRegTagSener: String(at?.noRegTagSener || '').trim()
  };
}

function stringifyUnitNotesWithMeta(notes, at) {
  const cleanNotes = String(notes || '').trim();
  const metaText = JSON.stringify(buildUnitMetaPayload(at));
  return `${cleanNotes}${cleanNotes ? '\n' : ''}${UNIT_META_MARKER}${metaText}`;
}

function normalizeAutotanques(list) {
  return (Array.isArray(list) ? list : []).map(at => {
    const parsed = parseUnitNotesWithMeta(at?.notas || '');
    const meta = parsed.meta || {};
    return {
      ...at,
      plantaActual: normalizePlantName(at?.plantaActual || at?.planta_actual || ''),
      activo: typeof at?.activo === 'boolean' ? at.activo : (typeof meta?.activo === 'boolean' ? meta.activo : true),
      enServicio: typeof at?.enServicio === 'boolean' ? at.enServicio : (typeof meta?.enServicio === 'boolean' ? meta.enServicio : true),
      marcaUnidad: String(at?.marcaUnidad || meta?.marcaUnidad || '').trim(),
      modeloUnidad: String(at?.modeloUnidad || meta?.modeloUnidad || '').trim(),
      dictamenNomMes: String(at?.dictamenNomMes || meta?.dictamenNomMes || '').trim(),
      dictamenNomAnio: String(at?.dictamenNomAnio || meta?.dictamenNomAnio || '').trim(),
      nom013Mes: String(at?.nom013Mes || meta?.nom013Mes || '').trim(),
      nom013Anio: String(at?.nom013Anio || meta?.nom013Anio || '').trim(),
      nom007SeshMes: String(at?.nom007SeshMes || meta?.nom007SeshMes || '').trim(),
      nom007SeshAnio: String(at?.nom007SeshAnio || meta?.nom007SeshAnio || '').trim(),
      registroSener: String(at?.registroSener || meta?.registroSener || '').trim(),
      noRegTagSener: String(at?.noRegTagSener || meta?.noRegTagSener || '').trim(),
      notas: parsed.notes,
      expediente: normalizeExpediente(at?.expediente)
    };
  });
}

function normalizePartImages(raw) {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return {};
  const out = {};
  for (const [partNo, img] of Object.entries(raw)) {
    const key = String(partNo || '').trim();
    if (!key || !img || typeof img !== 'object') continue;
    out[key] = {
      partNo: key,
      fileName: String(img.fileName || ''),
      mimeType: String(img.mimeType || ''),
      sizeBytes: Number(img.sizeBytes || 0) || 0,
      dataUrl: String(img.dataUrl || ''),
      updatedAt: String(img.updatedAt || '')
    };
  }
  return out;
}

function mapPartImageToDb(img) {
  return {
    part_no: img.partNo,
    file_name: img.fileName || null,
    mime_type: img.mimeType || null,
    size_bytes: img.sizeBytes || null,
    data_url: img.dataUrl,
    updated_at: img.updatedAt || new Date().toISOString()
  };
}

function mapPartImageFromDb(row) {
  return {
    partNo: row.part_no || '',
    fileName: row.file_name || '',
    mimeType: row.mime_type || '',
    sizeBytes: Number(row.size_bytes || 0) || 0,
    dataUrl: row.data_url || '',
    updatedAt: row.updated_at || ''
  };
}

function getPartImage(partNo) {
  const targetType = getRegistroTargetType();
  const partNoRaw = String(partNo || '').trim();
  const key = targetType === 'estacion' ? `estacion:${partNoRaw}` : partNoRaw;
  if (!key) return null;
  let img = partImages[key];
  // Compatibilidad hacia atras para imagenes antiguas de autotanque guardadas con la llave simple.
  if (!img && targetType !== 'estacion') img = partImages[partNoRaw];
  if (!img || typeof img !== 'object' || !img.dataUrl) return null;
  return img;
}

function getPartImageSrc(partNo) {
  const img = getPartImage(partNo);
  const targetType = getRegistroTargetType();
  const baseImg = targetType === 'estacion' ? STATION_COMPONENT_GUIDE_IMAGE : COMPONENT_GUIDE_IMAGE;
  return img?.dataUrl || baseImg;
}

function setImageWithFallback(imgEl, src, fallbackTextEl) {
  if (!imgEl) return;
  const fallbackEl = fallbackTextEl || null;
  imgEl.onerror = () => {
    imgEl.style.display = 'none';
    if (fallbackEl) fallbackEl.style.display = 'block';
  };
  imgEl.onload = () => {
    imgEl.style.display = 'block';
    if (fallbackEl) fallbackEl.style.display = 'none';
  };
  imgEl.src = src;
}

function refreshSelectedPartImageUI() {
  const statusEl = document.getElementById('partImageStatus');
  const guideImg = document.getElementById('componentGuideImage');
  const guideFallback = document.getElementById('componentGuideMissing');
  const diagramImg = document.getElementById('diagramImg');

  const partNo = selectedPart?.no || '';
  const src = getPartImageSrc(partNo);

  setImageWithFallback(guideImg, src, guideFallback);
  if (diagramImg && document.getElementById('partPreviewCard')?.style.display !== 'none') {
    diagramImg.src = src;
  }

  if (!statusEl) return;
  if (!partNo) {
    statusEl.textContent = 'Selecciona una pieza para administrar su imagen.';
    return;
  }
  const targetType = getRegistroTargetType();
  const img = getPartImage(partNo);
  if (!img) {
    statusEl.textContent = targetType === 'estacion'
      ? 'Esta pieza de estación usa la imagen guía general.'
      : 'Esta pieza usa la imagen guía general.';
    return;
  }
  const updatedAtTxt = img.updatedAt ? formatDateTime(img.updatedAt) : 'Sin fecha';
  statusEl.textContent = `Imagen guardada: ${img.fileName || 'archivo'} | ${formatBytes(img.sizeBytes)} | ${updatedAtTxt}`;
}

function normalizeExpedienteDoc(doc) {
  if (!doc || typeof doc !== 'object') return null;
  const out = {
    id: String(doc.id || '').trim() || genId(),
    category: String(doc.category || 'otro').trim() || 'otro',
    name: String(doc.name || doc.fileName || 'Documento'),
    fileName: String(doc.fileName || doc.name || 'archivo'),
    mimeType: String(doc.mimeType || 'application/octet-stream'),
    size: Number(doc.size || 0) || 0,
    uploadedAt: String(doc.uploadedAt || new Date().toISOString())
  };

  const storagePath = String(doc.storagePath || '').trim();
  if (storagePath) {
    out.storagePath = storagePath;
    out.storageBucket = String(doc.storageBucket || SUPABASE_BUCKET_EXPEDIENTE).trim() || SUPABASE_BUCKET_EXPEDIENTE;
    return out;
  }

  const dataUrl = String(doc.dataUrl || '').trim();
  if (dataUrl) out.dataUrl = dataUrl;
  return out;
}

function normalizeExpediente(raw) {
  if (Array.isArray(raw)) return raw.map(normalizeExpedienteDoc).filter(Boolean);
  if (!raw) return [];
  if (typeof raw === 'object') {
    if (Array.isArray(raw.data)) return raw.data.map(normalizeExpedienteDoc).filter(Boolean);
    if (Array.isArray(raw.expediente)) return raw.expediente.map(normalizeExpedienteDoc).filter(Boolean);
    return [];
  }
  if (typeof raw === 'string') {
    try {
      const parsed = JSON.parse(raw);
      return Array.isArray(parsed) ? parsed.map(normalizeExpedienteDoc).filter(Boolean) : [];
    } catch {
      return [];
    }
  }
  return [];
}

function isSecuritySealEvidenceDoc(doc) {
  const text = normalizeMatchText([
    doc?.name,
    doc?.fileName
  ].filter(Boolean).join(' '));
  return text.includes('evidencia sello seguridad') || text.includes('evidencia de sellos');
}

function getVisibleExpedienteDocs(expedienteDocs) {
  return normalizeExpediente(expedienteDocs).filter(doc => !isSecuritySealEvidenceDoc(doc));
}

function sanitizeStorageFileName(fileName) {
  const ext = (String(fileName || '').match(/\.[a-z0-9]{1,8}$/i) || [''])[0].toLowerCase();
  const base = String(fileName || 'archivo')
    .replace(/\.[^.]+$/, '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9_-]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 80) || 'archivo';
  return `${base}${ext}`;
}

function getExpedienteStoragePath(atId, docId, fileName) {
  const safeAtId = String(atId || '').trim() || 'sin-unidad';
  const safeDocId = String(docId || genId()).trim();
  const safeName = sanitizeStorageFileName(fileName);
  return `autotanques/${safeAtId}/${safeDocId}-${safeName}`;
}

function escapeHtml(v) {
  return String(v ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function formatBytes(bytes) {
  const num = Number(bytes) || 0;
  if (!num) return '0 B';
  if (num < 1024) return `${num} B`;
  if (num < 1024 * 1024) return `${(num / 1024).toFixed(1)} KB`;
  return `${(num / (1024 * 1024)).toFixed(2)} MB`;
}

function formatDateTime(iso) {
  if (!iso) return 'Sin fecha';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return 'Sin fecha';
  return d.toLocaleString('es-MX');
}

function expedienteCategoryLabel(key) {
  return EXPEDIENTE_CATEGORIES[key] || EXPEDIENTE_CATEGORIES.otro;
}

function mapUnitToDb(at) {
  return {
    id: at.id,
    econ: at.econ,
    placa: at.placa,
    planta_actual: normalizePlantName(at.plantaActual) || null,
    serie_unidad: at.serieUnidad || null,
    serie_tanque: at.serieTanque || null,
    capacidad: at.capacidad || null,
    anio: at.anio || null,
    notas: stringifyUnitNotesWithMeta(at.notas, at) || null,
    expediente: normalizeExpediente(at.expediente)
  };
}

function mapUnitFromDb(row) {
  const parsed = parseUnitNotesWithMeta(row.notas || '');
  const meta = parsed.meta || {};
  return {
    id: row.id,
    econ: row.econ || '',
    placa: row.placa || '',
    plantaActual: normalizePlantName(row.planta_actual || ''),
    serieUnidad: row.serie_unidad || '',
    serieTanque: row.serie_tanque || '',
    capacidad: row.capacidad || '',
    anio: row.anio || '',
    activo: typeof meta.activo === 'boolean' ? meta.activo : true,
    enServicio: typeof meta.enServicio === 'boolean' ? meta.enServicio : true,
    marcaUnidad: String(meta.marcaUnidad || '').trim(),
    modeloUnidad: String(meta.modeloUnidad || '').trim(),
    dictamenNomMes: String(meta.dictamenNomMes || '').trim(),
    dictamenNomAnio: String(meta.dictamenNomAnio || '').trim(),
    nom013Mes: String(meta.nom013Mes || '').trim(),
    nom013Anio: String(meta.nom013Anio || '').trim(),
    nom007SeshMes: String(meta.nom007SeshMes || '').trim(),
    nom007SeshAnio: String(meta.nom007SeshAnio || '').trim(),
    registroSener: String(meta.registroSener || '').trim(),
    noRegTagSener: String(meta.noRegTagSener || '').trim(),
    notas: parsed.notes,
    expediente: normalizeExpediente(row.expediente)
  };
}

function mapRecordToDb(rec) {
  return {
    id: rec.id,
    at_id: rec.atId,
    part_no: rec.partNo,
    part_pn: rec.partPn || null,
    part_desc: rec.partDesc || null,
    part_brand: rec.partBrand || null,
    fab_date: rec.fabDate || null,
    inst_date: rec.instDate || null,
    repl_date: rec.replDate || null,
    serial: rec.serial || null,
    brand: rec.brand || null,
    notes: rec.notes || null,
    created_at: rec.createdAt || new Date().toISOString()
  };
}

function mapRecordFromDb(row) {
  return {
    id: row.id,
    atId: row.at_id,
    partNo: row.part_no || '',
    partPn: row.part_pn || '',
    partDesc: row.part_desc || '',
    partBrand: row.part_brand || '',
    fabDate: row.fab_date || '',
    instDate: row.inst_date || '',
    replDate: row.repl_date || '',
    serial: row.serial || '',
    brand: row.brand || '',
    notes: row.notes || '',
    createdAt: row.created_at || new Date().toISOString()
  };
}

function mapStationRecordToDb(rec) {
  return {
    id: rec.id,
    station_id: rec.stationId,
    part_no: rec.partNo,
    part_pn: rec.partPn || null,
    part_desc: rec.partDesc || null,
    part_brand: rec.partBrand || null,
    fab_date: rec.fabDate || null,
    inst_date: rec.instDate || null,
    repl_date: rec.replDate || null,
    serial: rec.serial || null,
    brand: rec.brand || null,
    notes: rec.notes || null,
    created_at: rec.createdAt || new Date().toISOString()
  };
}

function mapStationRecordFromDb(row) {
  return {
    id: row.id,
    stationId: row.station_id || '',
    partNo: row.part_no || '',
    partPn: row.part_pn || '',
    partDesc: row.part_desc || '',
    partBrand: row.part_brand || '',
    fabDate: row.fab_date || '',
    instDate: row.inst_date || '',
    replDate: row.repl_date || '',
    serial: row.serial || '',
    brand: row.brand || '',
    notes: row.notes || '',
    createdAt: row.created_at || new Date().toISOString()
  };
}

function chunkArray(arr, size = 200) {
  const chunks = [];
  for (let i = 0; i < arr.length; i += size) chunks.push(arr.slice(i, i + size));
  return chunks;
}

function syncConfigInputs() {
  const urlEl = document.getElementById('sbUrl');
  const keyEl = document.getElementById('sbAnonKey');
  if (urlEl) {
    urlEl.value = SUPABASE_URL;
    urlEl.readOnly = HAS_FILE_SUPABASE_CONFIG;
  }
  if (keyEl) {
    keyEl.value = SUPABASE_ANON;
    keyEl.readOnly = HAS_FILE_SUPABASE_CONFIG;
  }
}

function updateStorageModeLabel(extra = '') {
  const el = document.getElementById('storageMode');
  if (!el) {
    updateExpedienteLimitNote();
    return;
  }
  const mode = runtimeUseSupabase ? 'Modo de almacenamiento: Supabase (en linea)' : 'Modo de almacenamiento: Local (localStorage)';
  el.textContent = `${mode} | Fuente de config: ${SUPABASE_CONFIG_SOURCE}${extra ? ' | ' + extra : ''}`;
  updateExpedienteLimitNote();
}

function getMaxDocSizeBytes() {
  return runtimeUseSupabase ? MAX_DOC_SIZE_SUPABASE_BYTES : MAX_DOC_SIZE_LOCAL_BYTES;
}

function updateExpedienteLimitNote() {
  const noteEl = document.getElementById('expedienteLimitNote');
  if (!noteEl) return;
  const maxMb = (getMaxDocSizeBytes() / (1024 * 1024)).toFixed(1).replace('.0', '');
  if (runtimeUseSupabase) {
    noteEl.textContent = `Nota: modo Supabase activo. Puedes subir archivos de hasta ${maxMb} MB por archivo.`;
    return;
  }
  noteEl.textContent = `Nota: por limite de almacenamiento local, usa archivos ligeros (recomendado max ${maxMb} MB por archivo).`;
}

function showSupabaseSetupSQL() {
  alert(
    'Ejecuta este SQL en Supabase (SQL Editor):\n\n' +
    SUPABASE_SETUP_SQL +
    '\n\nLa conexion se administra desde assets/config/supabase.config.js. Recarga la pagina despues de actualizar ese archivo.'
  );
}

function saveSupabaseConfig() {
  if (HAS_FILE_SUPABASE_CONFIG) {
    return alert('La conexion esta administrada por assets/config/supabase.config.js. Edita ese archivo y recarga la pagina.');
  }
  const url = (document.getElementById('sbUrl')?.value || '').trim();
  const anon = (document.getElementById('sbAnonKey')?.value || '').trim();
  if (!url || !anon) return alert('Ingresa SUPABASE URL y SUPABASE ANON KEY.');
  localStorage.setItem(SB_URL_KEY, url);
  localStorage.setItem(SB_ANON_KEY, anon);
  alert('Configuracion guardada. Se recargara la pagina para activar Supabase.');
  location.reload();
}

function clearSupabaseConfig() {
  if (HAS_FILE_SUPABASE_CONFIG) {
    return alert('La conexion activa viene de assets/config/supabase.config.js. Vacia ese archivo si quieres volver al modo local.');
  }
  if (!confirm('Se quitara la configuracion de Supabase y se usara almacenamiento local. Continuar?')) return;
  localStorage.removeItem(SB_URL_KEY);
  localStorage.removeItem(SB_ANON_KEY);
  alert('Configuracion eliminada. Se recargara la pagina.');
  location.reload();
}

function isMissingPlantaColumnError(error) {
  const detail = `${error?.message || ''} ${error?.details || ''} ${error?.hint || ''}`.toLowerCase();
  return detail.includes('planta_actual') && detail.includes('at_units') && detail.includes('column');
}

function isMissingPartImagesTableError(error) {
  const detail = `${error?.message || ''} ${error?.details || ''} ${error?.hint || ''}`.toLowerCase();
  const tableName = String(SUPABASE_TABLE_PART_IMAGES || 'at_part_images').toLowerCase();
  return (detail.includes(tableName) || detail.includes('at_part_images')) && (detail.includes('does not exist') || detail.includes('schema cache'));
}

function isMissingStationRecordsTableError(error) {
  const detail = `${error?.message || ''} ${error?.details || ''} ${error?.hint || ''}`.toLowerCase();
  const tableName = String(SUPABASE_TABLE_STATION_RECORDS || 'at_station_records').toLowerCase();
  return (detail.includes(tableName) || detail.includes('at_station_records')) && (detail.includes('does not exist') || detail.includes('schema cache'));
}

function isMissingChangeHistoryTableError(error) {
  const detail = `${error?.message || ''} ${error?.details || ''} ${error?.hint || ''}`.toLowerCase();
  const tableName = String(SUPABASE_TABLE_CHANGE_HISTORY || 'at_change_history').toLowerCase();
  return (detail.includes(tableName) || detail.includes('at_change_history')) && (detail.includes('does not exist') || detail.includes('schema cache'));
}

function isStorageBucketError(error) {
  const detail = `${error?.message || ''} ${error?.details || ''} ${error?.hint || ''}`.toLowerCase();
  const bucket = String(SUPABASE_BUCKET_EXPEDIENTE || 'at_expediente').toLowerCase();
  return detail.includes(bucket) && (
    detail.includes('bucket') ||
    detail.includes('not found') ||
    detail.includes('does not exist') ||
    detail.includes('policy') ||
    detail.includes('permission') ||
    detail.includes('row-level')
  );
}

function showSupabaseError(context, error) {
  const detail = error?.message || error?.details || String(error || 'Error desconocido');
  if (isMissingPlantaColumnError(error)) {
    return alert(
      'Error en Supabase: falta la columna planta_actual en at_units.\n\n' +
      'Ejecuta en SQL Editor:\n' +
      'alter table public.at_units add column if not exists planta_actual text;\n\n' +
      'Despues espera unos segundos y recarga la pagina.'
    );
  }
  if (isMissingPartImagesTableError(error)) {
    return alert(
      'Error en Supabase: falta la tabla at_part_images.\n\n' +
      'Ejecuta el SQL de configuracion para crear la tabla y politicas, recarga y vuelve a intentar.'
    );
  }
  if (isMissingStationRecordsTableError(error)) {
    return alert(
      `Error en Supabase: falta la tabla ${SUPABASE_TABLE_STATION_RECORDS}.\n\n` +
      'Ejecuta el SQL de configuracion para crearla y recarga la pagina.'
    );
  }
  if (isMissingChangeHistoryTableError(error)) {
    return alert(
      `Error en Supabase: falta la tabla ${SUPABASE_TABLE_CHANGE_HISTORY}.\n\n` +
      'Ejecuta el SQL de configuracion para crearla y recarga la pagina.'
    );
  }
  if (isStorageBucketError(error)) {
    return alert(
      `Error en Supabase Storage (${SUPABASE_BUCKET_EXPEDIENTE}).\n\n` +
      'Verifica que el bucket exista y que las politicas permitan subir/leer/eliminar.\n' +
      'Puedes usar el SQL de configuracion para crearlo.'
    );
  }
  alert(`Error en Supabase (${context}): ${detail}`);
}

async function loadFromSupabase() {
  if (!runtimeUseSupabase) return true;
  const localPartImagesCache = normalizePartImages(JSON.parse(localStorage.getItem('at_part_images') || '{}'));
  const localStationRecordsCache = normalizeStationRecords(JSON.parse(localStorage.getItem(STATION_RECORDS_STORAGE_KEY) || '[]'));
  const localChangeHistoryCache = loadChangeHistory();
  const { data: unitRows, error: unitsError } = await supabaseClient
    .from(SUPABASE_TABLE_UNITS)
    .select('*')
    .order('econ', { ascending: true });
  if (unitsError) {
    runtimeUseSupabase = false;
    showSupabaseError('carga de autotanques', unitsError);
    return false;
  }

  const { data: recordRows, error: recordsError } = await supabaseClient
    .from(SUPABASE_TABLE_RECORDS)
    .select('*')
    .order('created_at', { ascending: false });
  if (recordsError) {
    runtimeUseSupabase = false;
    showSupabaseError('carga de componentes', recordsError);
    return false;
  }

  const { data: partImageRows, error: partImagesError } = await supabaseClient
    .from(SUPABASE_TABLE_PART_IMAGES)
    .select('*');
  if (partImagesError) {
    showSupabaseError('carga de imagenes de piezas', partImagesError);
  }

  let stationRecordRows = [];
  if (runtimeUseSupabaseStationRecords) {
    const { data, error } = await supabaseClient
      .from(SUPABASE_TABLE_STATION_RECORDS)
      .select('*')
      .order('created_at', { ascending: false });
    if (error) {
      if (isMissingStationRecordsTableError(error)) {
        runtimeUseSupabaseStationRecords = false;
        updateStorageModeLabel(`tabla ${SUPABASE_TABLE_STATION_RECORDS} no existe; registros de estaciones solo local`);
      } else {
        runtimeUseSupabaseStationRecords = false;
        showSupabaseError('carga de registros de estaciones', error);
      }
      stationRecords = localStationRecordsCache;
    } else {
      stationRecordRows = data || [];
      stationRecords = normalizeStationRecords(stationRecordRows.map(mapStationRecordFromDb));
    }
  } else {
    stationRecords = localStationRecordsCache;
  }

  let changeHistoryRows = [];
  if (runtimeUseSupabaseChangeHistory) {
    const { data, error } = await supabaseClient
      .from(SUPABASE_TABLE_CHANGE_HISTORY)
      .select('*')
      .order('event_at', { ascending: false })
      .limit(CHANGE_HISTORY_LIMIT);
    if (error) {
      if (isMissingChangeHistoryTableError(error)) {
        runtimeUseSupabaseChangeHistory = false;
        updateStorageModeLabel(`tabla ${SUPABASE_TABLE_CHANGE_HISTORY} no existe; historial solo local`);
      } else {
        runtimeUseSupabaseChangeHistory = false;
        console.warn('No se pudo cargar historial desde Supabase:', error);
      }
      changeHistory = localChangeHistoryCache;
    } else {
      changeHistoryRows = data || [];
      const remoteChangeHistory = normalizeChangeHistoryEntries(changeHistoryRows.map(mapChangeHistoryFromDb));
      changeHistory = mergeChangeHistoryLists(remoteChangeHistory, localChangeHistoryCache).slice(0, CHANGE_HISTORY_LIMIT);
      persistChangeHistory();
      const missingRemote = changeHistory.filter(item => !remoteChangeHistory.some(r => r.id === item.id));
      if (missingRemote.length) queueChangeHistoryForRemote(missingRemote);
    }
  } else {
    changeHistory = localChangeHistoryCache;
  }

  autotanques = normalizeAutotanques((unitRows || []).map(mapUnitFromDb));
  records = (recordRows || []).map(mapRecordFromDb);
  if (!partImagesError) {
    const remotePartImages = {};
    (partImageRows || []).forEach(row => {
      const img = mapPartImageFromDb(row);
      if (img.partNo && img.dataUrl) remotePartImages[img.partNo] = img;
    });
    const remoteCount = Object.keys(remotePartImages).length;
    const localCount = Object.keys(localPartImagesCache).length;
    if (!remoteCount && localCount) {
      partImages = localPartImagesCache;
      const synced = await upsertPartImagesRemote(Object.values(localPartImagesCache));
      updateStorageModeLabel(
        synced
          ? `imagenes de piezas restauradas desde cache local (${localCount})`
          : `imagenes de piezas restauradas solo local (${localCount})`
      );
    } else {
      // Preferimos lo remoto, pero mantenemos cualquier imagen local faltante.
      partImages = normalizePartImages({ ...localPartImagesCache, ...remotePartImages });
    }
  }
  if (!runtimeUseSupabaseStationRecords) {
    stationRecords = localStationRecordsCache;
  }
  if (runtimeUseSupabaseChangeHistory) {
    void flushPendingChangeHistoryRemote();
  }
  save();
  return true;
}

async function upsertUnitRemote(at) {
  if (!runtimeUseSupabase) return true;
  const { error } = await supabaseClient
    .from(SUPABASE_TABLE_UNITS)
    .upsert([mapUnitToDb(at)], { onConflict: 'id' });
  if (error) {
    showSupabaseError('guardar autotanque', error);
    return false;
  }
  return true;
}

async function upsertUnitsRemote(units) {
  if (!runtimeUseSupabase) return true;
  const rows = normalizeAutotanques(units).map(mapUnitToDb);
  for (const chunk of chunkArray(rows)) {
    const { error } = await supabaseClient
      .from(SUPABASE_TABLE_UNITS)
      .upsert(chunk, { onConflict: 'id' });
    if (error) {
      showSupabaseError('importar autotanques CSV', error);
      return false;
    }
  }
  return true;
}

async function upsertPartImageRemote(img) {
  if (!runtimeUseSupabase) return true;
  const { error } = await supabaseClient
    .from(SUPABASE_TABLE_PART_IMAGES)
    .upsert([mapPartImageToDb(img)], { onConflict: 'part_no' });
  if (error) {
    showSupabaseError('guardar imagen de pieza', error);
    return false;
  }
  return true;
}

async function upsertPartImagesRemote(images) {
  if (!runtimeUseSupabase) return true;
  const rows = (Array.isArray(images) ? images : [])
    .filter(img => img?.partNo && img?.dataUrl)
    .map(mapPartImageToDb);
  if (!rows.length) return true;
  for (const chunk of chunkArray(rows)) {
    const { error } = await supabaseClient
      .from(SUPABASE_TABLE_PART_IMAGES)
      .upsert(chunk, { onConflict: 'part_no' });
    if (error) {
      showSupabaseError('sincronizar imagenes de piezas', error);
      return false;
    }
  }
  return true;
}

async function deletePartImageRemote(partNo) {
  if (!runtimeUseSupabase) return true;
  const { error } = await supabaseClient
    .from(SUPABASE_TABLE_PART_IMAGES)
    .delete()
    .eq('part_no', partNo);
  if (error) {
    showSupabaseError('eliminar imagen de pieza', error);
    return false;
  }
  return true;
}

async function uploadExpedienteFileRemote(file, atId, docId) {
  if (!runtimeUseSupabase) return null;
  const path = getExpedienteStoragePath(atId, docId, file?.name || 'archivo.bin');
  const contentType = file?.type || 'application/octet-stream';
  const { error } = await supabaseClient
    .storage
    .from(SUPABASE_BUCKET_EXPEDIENTE)
    .upload(path, file, {
      cacheControl: '3600',
      upsert: true,
      contentType
    });
  if (error) {
    showSupabaseError(`subir documento a bucket ${SUPABASE_BUCKET_EXPEDIENTE}`, error);
    return null;
  }
  return { path, bucket: SUPABASE_BUCKET_EXPEDIENTE };
}

async function deleteExpedienteFilesRemote(paths) {
  if (!runtimeUseSupabase) return true;
  const validPaths = [...new Set((Array.isArray(paths) ? paths : []).map(p => String(p || '').trim()).filter(Boolean))];
  if (!validPaths.length) return true;
  const { error } = await supabaseClient
    .storage
    .from(SUPABASE_BUCKET_EXPEDIENTE)
    .remove(validPaths);
  if (error) {
    showSupabaseError(`eliminar archivos del bucket ${SUPABASE_BUCKET_EXPEDIENTE}`, error);
    return false;
  }
  return true;
}

async function getExpedienteDocAccessUrl(doc) {
  if (!doc) return '';
  if (doc.dataUrl) return doc.dataUrl;
  const storagePath = String(doc.storagePath || '').trim();
  if (!storagePath) return '';
  const bucket = String(doc.storageBucket || SUPABASE_BUCKET_EXPEDIENTE).trim() || SUPABASE_BUCKET_EXPEDIENTE;
  if (!runtimeUseSupabase) return '';

  const { data, error } = await supabaseClient
    .storage
    .from(bucket)
    .createSignedUrl(storagePath, 60 * 5);
  if (error || !data?.signedUrl) {
    showSupabaseError(`abrir documento desde bucket ${bucket}`, error || 'No se pudo generar URL firmada.');
    return '';
  }
  return data.signedUrl;
}

async function insertRecordRemote(rec) {
  if (!runtimeUseSupabase) return true;
  const { error } = await supabaseClient
    .from(SUPABASE_TABLE_RECORDS)
    .insert([mapRecordToDb(rec)]);
  if (error) {
    showSupabaseError('guardar componente', error);
    return false;
  }
  return true;
}

async function upsertRecordRemote(rec) {
  if (!runtimeUseSupabase) return true;
  const { error } = await supabaseClient
    .from(SUPABASE_TABLE_RECORDS)
    .upsert([mapRecordToDb(rec)], { onConflict: 'id' });
  if (error) {
    showSupabaseError('actualizar registro', error);
    return false;
  }
  return true;
}

async function upsertRecordsRemote(recordsToUpsert, context = 'actualizar registros') {
  if (!runtimeUseSupabase) return true;
  const rows = (Array.isArray(recordsToUpsert) ? recordsToUpsert : []).map(mapRecordToDb);
  if (!rows.length) return true;
  for (const chunk of chunkArray(rows)) {
    const { error } = await supabaseClient
      .from(SUPABASE_TABLE_RECORDS)
      .upsert(chunk, { onConflict: 'id' });
    if (error) {
      showSupabaseError(context, error);
      return false;
    }
  }
  return true;
}

async function deleteAutotanqueRemote(atId) {
  if (!runtimeUseSupabase) return true;
  const currentAt = autotanques.find(a => a.id === atId);
  const pathsToDelete = normalizeExpediente(currentAt?.expediente).map(d => d.storagePath).filter(Boolean);
  if (pathsToDelete.length) await deleteExpedienteFilesRemote(pathsToDelete);
  const { error: recErr } = await supabaseClient
    .from(SUPABASE_TABLE_RECORDS)
    .delete()
    .eq('at_id', atId);
  if (recErr) {
    showSupabaseError('eliminar componentes del autotanque', recErr);
    return false;
  }

  const { error: atErr } = await supabaseClient
    .from(SUPABASE_TABLE_UNITS)
    .delete()
    .eq('id', atId);
  if (atErr) {
    showSupabaseError('eliminar autotanque', atErr);
    return false;
  }
  return true;
}

async function deleteRecordRemote(recordId) {
  if (!runtimeUseSupabase) return true;
  const { error } = await supabaseClient
    .from(SUPABASE_TABLE_RECORDS)
    .delete()
    .eq('id', recordId);
  if (error) {
    showSupabaseError('eliminar registro', error);
    return false;
  }
  return true;
}

async function insertStationRecordRemote(rec) {
  if (!runtimeUseSupabase || !runtimeUseSupabaseStationRecords) return true;
  const { error } = await supabaseClient
    .from(SUPABASE_TABLE_STATION_RECORDS)
    .insert([mapStationRecordToDb(rec)]);
  if (error) {
    if (isMissingStationRecordsTableError(error)) {
      runtimeUseSupabaseStationRecords = false;
      updateStorageModeLabel(`tabla ${SUPABASE_TABLE_STATION_RECORDS} no existe; registros de estaciones solo local`);
      return true;
    }
    showSupabaseError('guardar componente de estación', error);
    return false;
  }
  return true;
}

async function deleteStationRecordsByStationRemote(stationId) {
  if (!runtimeUseSupabase || !runtimeUseSupabaseStationRecords) return true;
  const { error } = await supabaseClient
    .from(SUPABASE_TABLE_STATION_RECORDS)
    .delete()
    .eq('station_id', stationId);
  if (error) {
    if (isMissingStationRecordsTableError(error)) {
      runtimeUseSupabaseStationRecords = false;
      return true;
    }
    showSupabaseError('eliminar registros de estación', error);
    return false;
  }
  return true;
}

async function replaceRemoteData(allUnits, allRecords, allPartImages = null, allStationRecords = null, allChangeHistory = null) {
  if (!runtimeUseSupabase) return true;

  const { error: delRecordsErr } = await supabaseClient
    .from(SUPABASE_TABLE_RECORDS)
    .delete()
    .not('id', 'is', null);
  if (delRecordsErr) {
    showSupabaseError('limpiar componentes en Supabase', delRecordsErr);
    return false;
  }

  const { error: delUnitsErr } = await supabaseClient
    .from(SUPABASE_TABLE_UNITS)
    .delete()
    .not('id', 'is', null);
  if (delUnitsErr) {
    showSupabaseError('limpiar autotanques en Supabase', delUnitsErr);
    return false;
  }

  if (allPartImages !== null) {
    const { error: delPartImagesErr } = await supabaseClient
      .from(SUPABASE_TABLE_PART_IMAGES)
      .delete()
      .not('part_no', 'is', null);
    if (delPartImagesErr) {
      showSupabaseError('limpiar imagenes de piezas en Supabase', delPartImagesErr);
      return false;
    }
  }

  if (allStationRecords !== null && runtimeUseSupabaseStationRecords) {
    const { error: delStationRecordsErr } = await supabaseClient
      .from(SUPABASE_TABLE_STATION_RECORDS)
      .delete()
      .not('id', 'is', null);
    if (delStationRecordsErr) {
      if (!isMissingStationRecordsTableError(delStationRecordsErr)) {
        showSupabaseError('limpiar registros de estaciones en Supabase', delStationRecordsErr);
        return false;
      }
      runtimeUseSupabaseStationRecords = false;
    }
  }

  if (allChangeHistory !== null && runtimeUseSupabaseChangeHistory) {
    const { error } = await supabaseClient
      .from(SUPABASE_TABLE_CHANGE_HISTORY)
      .delete()
      .neq('id', '');
    if (error) {
      if (isMissingChangeHistoryTableError(error)) {
        runtimeUseSupabaseChangeHistory = false;
        updateStorageModeLabel(`tabla ${SUPABASE_TABLE_CHANGE_HISTORY} no existe; historial solo local`);
      } else {
        console.warn('No se pudo limpiar historial remoto:', error);
      }
      return false;
    }
  }

  const unitRows = normalizeAutotanques(allUnits).map(mapUnitToDb);
  for (const chunk of chunkArray(unitRows)) {
    const { error } = await supabaseClient.from(SUPABASE_TABLE_UNITS).insert(chunk);
    if (error) {
      showSupabaseError('importar autotanques en Supabase', error);
      return false;
    }
  }

  const recordRows = (Array.isArray(allRecords) ? allRecords : []).map(mapRecordToDb);
  for (const chunk of chunkArray(recordRows)) {
    const { error } = await supabaseClient.from(SUPABASE_TABLE_RECORDS).insert(chunk);
    if (error) {
      showSupabaseError('importar componentes en Supabase', error);
      return false;
    }
  }

  if (allPartImages !== null) {
    const imageRows = Object.values(normalizePartImages(allPartImages))
      .filter(img => img?.partNo && img?.dataUrl)
      .map(mapPartImageToDb);
    for (const chunk of chunkArray(imageRows)) {
      const { error } = await supabaseClient.from(SUPABASE_TABLE_PART_IMAGES).insert(chunk);
      if (error) {
        showSupabaseError('importar imagenes de piezas en Supabase', error);
        return false;
      }
    }
  }

  if (allStationRecords !== null && runtimeUseSupabaseStationRecords) {
    const stationRows = normalizeStationRecords(allStationRecords).map(mapStationRecordToDb);
    for (const chunk of chunkArray(stationRows)) {
      const { error } = await supabaseClient.from(SUPABASE_TABLE_STATION_RECORDS).insert(chunk);
      if (error) {
        if (!isMissingStationRecordsTableError(error)) {
          showSupabaseError('importar registros de estaciones en Supabase', error);
          return false;
        }
        runtimeUseSupabaseStationRecords = false;
        break;
      }
    }
  }

  if (allChangeHistory !== null && runtimeUseSupabaseChangeHistory) {
    const historyRows = normalizeChangeHistoryEntries(allChangeHistory).map(mapChangeHistoryToDb);
    for (const chunk of chunkArray(historyRows)) {
      const { error } = await supabaseClient.from(SUPABASE_TABLE_CHANGE_HISTORY).insert(chunk);
      if (error) {
        if (isMissingChangeHistoryTableError(error)) {
          runtimeUseSupabaseChangeHistory = false;
          updateStorageModeLabel(`tabla ${SUPABASE_TABLE_CHANGE_HISTORY} no existe; historial solo local`);
          break;
        }
        console.warn('No se pudo importar historial remoto:', error);
        return false;
      }
    }
  }

  return true;
}

// ── TABS ──────────────────────────────────────────────────────────────
function switchTab(id) {
  document.querySelectorAll('.tab-btn').forEach((b,i) => {
    const tabs = ['dashboard','autotanques','estaciones','registro','reemplazos','exportar'];
    b.classList.toggle('active', tabs[i] === id);
  });
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  document.getElementById('tab-'+id).classList.add('active');
  if (id === 'dashboard')   renderDashboard();
  if (id === 'autotanques') renderAutotanques();
  if (id === 'estaciones')  renderEstaciones();
  if (id === 'registro')    renderPartList(); populateATSelect(); populateStationSelectForRegistro(); toggleRegistroTarget(); refreshSelectedPartImageUI();
  if (id === 'reemplazos')  {
    renderReemplazos();
    setTimeout(() => maintenanceFullCalendar?.updateSize?.(), 120);
  }
  if (id === 'exportar')    renderChangeHistory();
}

function getRegistroTargetType() {
  return document.getElementById('formTargetType')?.value || 'autotanque';
}

function getCurrentPartsCatalog() {
  return getRegistroTargetType() === 'estacion' ? STATION_PARTS_UNIQUE : PARTS;
}

function normalizeMatchText(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .trim();
}

function isSecuritySealPart(part) {
  const desc = normalizeMatchText(part?.desc || '');
  return desc.includes('sello de seguridad');
}

function shouldOfferSecuritySealEvidence() {
  return getRegistroTargetType() === 'autotanque' && isSecuritySealPart(selectedPart);
}

function getSecuritySealEvidenceFiles() {
  const input = document.getElementById('formSealEvidence');
  return Array.from(input?.files || []);
}

function updateSecuritySealEvidenceUI() {
  const wrap = document.getElementById('formSealEvidenceWrap');
  const hint = document.getElementById('formSealEvidenceHint');
  const input = document.getElementById('formSealEvidence');
  if (!wrap || !hint || !input) return;

  const enabled = shouldOfferSecuritySealEvidence();
  wrap.style.display = enabled ? 'block' : 'none';
  const filesCount = getSecuritySealEvidenceFiles().length;

  if (enabled) {
    hint.textContent = `Opcional para Sello de seguridad: adjunta las evidencias que necesites. Seleccionados: ${filesCount}.`;
    return;
  }

  input.value = '';
  hint.textContent = 'Disponible solo para el componente "Sello de seguridad" en autotanques.';
}

function getStationPartsCount() {
  return STATION_PARTS_UNIQUE.length;
}

function getStationRecordsByStationId(stationId) {
  return stationRecords.filter(rec => rec.stationId === stationId);
}

function getStationComponentList(stationId, fallbackComponents = []) {
  const fromRecords = getStationRecordsByStationId(stationId)
    .map(rec => rec.partDesc || (rec.partNo ? `Pieza ${rec.partNo}` : ''))
    .filter(Boolean);
  return [...new Set([...(Array.isArray(fallbackComponents) ? fallbackComponents : []), ...fromRecords])];
}

function renderEstaciones() {
  const tbody = document.getElementById('tableEstaciones');
  if (!tbody) return;

  const search = (document.getElementById('searchEst')?.value || '').trim().toLowerCase();
  const selectedPlant = String(document.getElementById('filterEstPlant')?.value || '').trim();

  let rows = normalizeEstaciones(estaciones).map(r => ({ ...r }));

  if (selectedPlant) rows = rows.filter(r => r.planta === selectedPlant);
  if (search) {
    rows = rows.filter(r =>
      r.planta.toLowerCase().includes(search) ||
      r.estacion.toLowerCase().includes(search) ||
      r.bomba.toLowerCase().includes(search)
    );
  }

  rows.sort((a, b) =>
    compareTextNatural(a.planta, b.planta) ||
    compareTextNatural(a.estacion, b.estacion) ||
    compareTextNatural(a.bomba, b.bomba)
  );

  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="6" style="text-align:center;color:var(--muted);padding:30px">Sin estaciones registradas para el filtro actual.</td></tr>';
    return;
  }

  const groupsMap = new Map();
  rows.forEach(r => {
    const key = encodeURIComponent(`${r.planta}||${r.estacion}`);
    if (!groupsMap.has(key)) groupsMap.set(key, { key, planta: r.planta, estacion: r.estacion, rows: [] });
    groupsMap.get(key).rows.push(r);
  });

  const groups = Array.from(groupsMap.values()).sort((a, b) =>
    compareTextNatural(a.planta, b.planta) ||
    compareTextNatural(a.estacion, b.estacion)
  );

  const plantsMap = new Map();
  groups.forEach(group => {
    if (!plantsMap.has(group.planta)) plantsMap.set(group.planta, []);
    plantsMap.get(group.planta).push(group);
  });

  const plantBlocks = Array.from(plantsMap.entries()).sort((a, b) => compareTextNatural(a[0], b[0]));

  tbody.innerHTML = plantBlocks.map(([planta, stationGroups]) => {
    const plantKey = encodeURIComponent(planta);
    const isPlantExpanded = expandedPlantGroupKey === plantKey;
    const stationGroupsSorted = [...stationGroups].sort((a, b) => compareTextNatural(a.estacion, b.estacion));
    const totalBombasPlant = stationGroupsSorted.reduce((acc, g) => acc + g.rows.length, 0);
    const totalCompPlant = stationGroupsSorted.reduce((acc, g) => acc + g.rows.reduce((sub, r) => sub + getStationComponentList(r.id, r.componentes).length, 0), 0);
    const plantRow = `
      <tr>
        <td colspan="6" class="plant-group-row bg-slate-50">
          <button class="station-group-toggle" type="button" onclick="togglePlantGroup('${plantKey}')">
            <span class="accordion-chevron ${isPlantExpanded ? 'expanded' : ''}">▶</span>
            <b style="color:var(--accent)">${escapeHtml(planta)}</b>
            <span style="color:var(--muted); font-size:11px">${stationGroupsSorted.length} estacion(es) | ${totalBombasPlant} bomba(s) | ${totalCompPlant} componente(s)</span>
          </button>
        </td>
      </tr>
    `;

    if (!isPlantExpanded) return plantRow;

    const stationRows = stationGroupsSorted.map(group => {
      const expanded = expandedStationGroupKey === group.key;
      const bombas = group.rows.length;
      const bombasActivas = group.rows.filter(r => getStationComponentList(r.id, r.componentes).length > 0).length;
      const totalComponentes = group.rows.reduce((acc, r) => acc + getStationComponentList(r.id, r.componentes).length, 0);
      const totalEsperado = bombas * getStationPartsCount();
      const pctGrupo = totalEsperado ? Math.min(100, Math.round((totalComponentes / totalEsperado) * 100)) : 0;
      const pctClassGrupo = pctGrupo < 50 ? 'danger' : pctGrupo < 85 ? 'warn' : '';

      const detailsHtml = expanded
        ? group.rows.map(r => {
          const comps = getStationComponentList(r.id, r.componentes);
          return `
          <tr>
            <td></td>
            <td style="color:var(--muted)">↳ ${escapeHtml(r.estacion)}</td>
            <td style="font-family:monospace">${escapeHtml(r.bomba)}</td>
            <td class="progress-cell">
              <div class="prog-bar"><div class="prog-fill ${comps.length < 6 ? 'danger' : comps.length < 10 ? 'warn' : ''}" style="width:${Math.min(100, Math.round((comps.length / getStationPartsCount()) * 100))}%"></div></div>
              <span style="font-size:11px">${comps.length}/${getStationPartsCount()}</span>
            </td>
            <td>${comps.length ? '<span class="badge badge-ok">ACTIVO</span>' : '<span class="badge badge-none">SIN REGISTROS</span>'}</td>
            <td class="table-actions-cell">
              <div class="table-actions">
                <button class="btn btn-secondary action-btn" onclick="openEstacionView('${r.id}')">VER</button>
                <button class="btn btn-secondary action-btn" onclick="openEstacionEdit('${r.id}')">✏️</button>
                <button class="btn btn-danger action-btn" onclick="deleteEstacion('${r.id}')">🗑</button>
              </div>
            </td>
          </tr>
        `;}).join('')
        : '';

      return `
        <tr>
          <td></td>
          <td style="background:var(--surface2); border-top:1px solid var(--border); border-bottom:1px solid var(--border);">
            <button class="station-group-toggle" type="button" onclick="toggleEstacionGroup('${plantKey}','${group.key}')">
              <span class="accordion-chevron ${expanded ? 'expanded' : ''}">▶</span>
              <b style="color:var(--accent)">${escapeHtml(group.estacion)}</b>
            </button>
          </td>
          <td style="background:var(--surface2); border-top:1px solid var(--border); border-bottom:1px solid var(--border); font-family:monospace">
            ${bombas} bomba(s)
          </td>
          <td class="progress-cell" style="background:var(--surface2); border-top:1px solid var(--border); border-bottom:1px solid var(--border);">
            <div class="prog-bar"><div class="prog-fill ${pctClassGrupo}" style="width:${pctGrupo}%"></div></div>
            <span style="font-size:11px">${totalComponentes}/${totalEsperado}</span>
          </td>
          <td style="background:var(--surface2); border-top:1px solid var(--border); border-bottom:1px solid var(--border);">
            <span class="text-muted">${bombasActivas}/${bombas} activas</span>
          </td>
          <td style="background:var(--surface2); border-top:1px solid var(--border); border-bottom:1px solid var(--border);">
            <button class="btn btn-secondary" style="padding:5px 10px;font-size:11px" onclick="toggleEstacionGroup('${plantKey}','${group.key}')">${expanded ? 'OCULTAR BOMBAS' : 'VER BOMBAS'}</button>
          </td>
        </tr>
        ${detailsHtml}
      `;
    }).join('');

    return `${plantRow}${stationRows}`;
  }).join('');
}

function togglePlantGroup(plantKey) {
  if (!plantKey) return;
  if (expandedPlantGroupKey === plantKey) {
    expandedPlantGroupKey = null;
    expandedStationGroupKey = null;
  } else {
    expandedPlantGroupKey = plantKey;
    expandedStationGroupKey = null;
  }
  renderEstaciones();
}

function toggleEstacionGroup(plantKey, stationKey) {
  if (!plantKey || !stationKey) return;
  if (expandedPlantGroupKey !== plantKey) expandedPlantGroupKey = plantKey;
  expandedStationGroupKey = expandedStationGroupKey === stationKey ? null : stationKey;
  renderEstaciones();
}

function openEstacionView(id) {
  const st = estaciones.find(x => x.id === id);
  if (!st) return alert('No se encontró la estación.');
  const componentes = getStationComponentList(st.id, st.componentes);
  const stationRecs = getStationRecordsByStationId(st.id).sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
  const body = document.getElementById('modalEstacionViewBody');
  if (!body) return;

  body.innerHTML = `
    <div class="grid-2 detail-grid-fixed" style="margin-bottom:16px">
      <div class="detail-row"><span class="detail-key">PLANTA:</span><span class="detail-val">${escapeHtml(st.planta)}</span></div>
      <div class="detail-row"><span class="detail-key">ESTACIÓN:</span><span class="detail-val">${escapeHtml(st.estacion)}</span></div>
      <div class="detail-row"><span class="detail-key">NÚMERO DE BOMBA:</span><span class="detail-val">${escapeHtml(st.bomba)}</span></div>
      <div class="detail-row"><span class="detail-key">COMPONENTES:</span><span class="detail-val">${componentes.length}/${getStationPartsCount()}</span></div>
    </div>
    <div class="section-sep"></div>
    <div class="card-title">COMPONENTES DE LA ESTACIÓN</div>
    <div class="table-wrap" style="max-height:300px;overflow-y:auto">
      <table>
        <thead><tr><th>#</th><th>DESCRIPCIÓN</th></tr></thead>
        <tbody>
          ${componentes.length
            ? componentes.map((c, i) => `<tr><td style="font-family:monospace">${i + 1}</td><td>${escapeHtml(c)}</td></tr>`).join('')
            : '<tr><td colspan="2" style="text-align:center;color:var(--muted);padding:20px">Sin componentes registrados</td></tr>'
          }
        </tbody>
      </table>
    </div>
    <div class="section-sep"></div>
    <div class="card-title">REGISTROS CAPTURADOS (${stationRecs.length})</div>
    <div class="table-wrap" style="max-height:260px;overflow-y:auto">
      <table>
        <thead><tr><th>PIEZA</th><th>DESCRIPCIÓN</th><th>F.FAB</th><th>F.REEMPLAZO</th><th>NOTAS</th></tr></thead>
        <tbody>
          ${stationRecs.length
            ? stationRecs.map(rec => `
              <tr>
                <td style="font-family:monospace;color:var(--accent)">${escapeHtml(rec.partNo || '—')}</td>
                <td>${escapeHtml(rec.partDesc || '—')}</td>
                <td>${formatDate(rec.fabDate)}</td>
                <td>${formatDate(rec.replDate)}</td>
                <td style="font-size:11px; white-space:pre-wrap; overflow-wrap:anywhere">${escapeHtml(rec.notes || '—')}</td>
              </tr>
            `).join('')
            : '<tr><td colspan="5" style="text-align:center;color:var(--muted);padding:20px">Sin registros capturados</td></tr>'
          }
        </tbody>
      </table>
    </div>
  `;
  document.getElementById('modalEstacionView').classList.add('open');
}

function openEstacionEdit(id) {
  const st = estaciones.find(x => x.id === id);
  if (!st) return alert('No se encontró la estación.');
  editingEstacionId = id;
  document.getElementById('editEstPlanta').value = st.planta || '';
  document.getElementById('editEstNombre').value = st.estacion || '';
  document.getElementById('editEstBomba').value = st.bomba || '';
  document.getElementById('editEstComponentes').value = (st.componentes || []).join('\n');
  document.getElementById('modalEstacionEdit').classList.add('open');
}

function saveEstacionEdit() {
  if (!editingEstacionId) return alert('No hay estación seleccionada para editar.');
  const idx = estaciones.findIndex(x => x.id === editingEstacionId);
  if (idx < 0) return alert('No se encontró la estación a actualizar.');

  const planta = String(document.getElementById('editEstPlanta').value || '').trim();
  const estacion = String(document.getElementById('editEstNombre').value || '').trim();
  const bomba = String(document.getElementById('editEstBomba').value || '').trim();
  const componentesRaw = String(document.getElementById('editEstComponentes').value || '');
  const componentes = componentesRaw
    .split('\n')
    .map(x => x.trim())
    .filter(Boolean);

  if (!planta || !estacion || !bomba) return alert('Planta, estación y número de bomba son obligatorios.');

  estaciones[idx] = {
    ...estaciones[idx],
    planta,
    estacion,
    bomba,
    componentes
  };
  save();
  closeModal('modalEstacionEdit');
  renderEstaciones();
  renderDashboard();
}

async function deleteEstacion(id) {
  const st = estaciones.find(x => x.id === id);
  if (!st) return alert('No se encontró la estación.');
  if (!confirm(`¿Eliminar la estación ${st.estacion} (${st.bomba}) de ${st.planta}?`)) return;
  if (!(await deleteStationRecordsByStationRemote(id))) return;
  stationRecords = stationRecords.filter(rec => rec.stationId !== id);
  estaciones = estaciones.filter(x => x.id !== id);
  save();
  renderEstaciones();
  renderDashboard();
}

function getPlantCatalog() {
  const fromData = autotanques
    .map(a => normalizePlantName(a?.plantaActual) || String(a?.plantaActual || '').trim())
    .filter(Boolean);
  const merged = [...new Set([...PLANTAS_ACTUALES, ...fromData])];
  return merged.sort((a, b) => compareTextNatural(a, b));
}

function fillPlantSelect(selectId, placeholder, plants) {
  const el = document.getElementById(selectId);
  if (!el) return;
  const prev = normalizePlantName(el.value) || String(el.value || '').trim();
  el.disabled = false;
  el.innerHTML = `<option value="">${placeholder}</option>` +
    plants.map(p => `<option value="${p}">${p}</option>`).join('');
  if (prev && plants.includes(prev)) el.value = prev;
}

function populatePlantSelectors() {
  const plants = getPlantCatalog();
  fillPlantSelect('atPlantaActual', '— Seleccionar planta —', plants);
  fillPlantSelect('filterATPlant', 'Todas las plantas', plants);
  fillPlantSelect('filterEstPlant', 'Todas las plantas', plants);
  fillPlantSelect('dashPlantFilter', 'Todas las plantas', plants);
  fillPlantSelect('normPlantSelect', '— Seleccionar planta —', plants);
}

// ── PART LIST ─────────────────────────────────────────────────────────
function renderPartList() {
  const q = (document.getElementById('searchPart')?.value || '').toLowerCase();
  const list = document.getElementById('partList');
  const catalog = getCurrentPartsCatalog();
  list.innerHTML = catalog.filter(p =>
    p.desc.toLowerCase().includes(q) ||
    p.no.includes(q) ||
    String(p.pn || '').toLowerCase().includes(q) ||
    String(p.corresponde || '').toLowerCase().includes(q) ||
    String((p.allNos || []).join(',')).toLowerCase().includes(q)
  ).map(p => `
    <div class="part-item ${selectedPart?.no===p.no?'selected':''}" onclick="selectPart('${p.no}')">
      <span class="part-no">${p.no}</span>
      <div>
        <div class="part-desc">${p.desc}</div>
        <div class="part-pn">${p.pn}${p.corresponde ? ` | ${p.corresponde}` : ''}${p.repeats > 1 ? ` | x${p.repeats}` : ''}</div>
      </div>
    </div>
  `).join('');
}

function filterParts() { renderPartList(); }

function selectPart(no) {
  selectedPart = getCurrentPartsCatalog().find(p => p.no === no);
  if (!selectedPart) return;
  renderPartList();

  const targetType = getRegistroTargetType();
  // Show preview card for both catalogs
  const card = document.getElementById('partPreviewCard');
  card.style.display = 'block';
  const highlight = document.getElementById('diagramHighlight');
  if (highlight) highlight.style.display = 'none';

  // Diagram image — we embed the uploaded diagram as the base
  const img = document.getElementById('diagramImg');
  img.src = getPartImageSrc(no);
  img.alt = `Diagrama - Pieza ${no}`;

  // Show detail info
  document.getElementById('partDetailInfo').innerHTML = `
    <div class="detail-row"><span class="detail-key">PIEZA No.:</span><span class="detail-val">${selectedPart.no}</span></div>
    <div class="detail-row"><span class="detail-key">NÚMERO DE PARTE:</span><span class="detail-val">${selectedPart.pn}</span></div>
    <div class="detail-row"><span class="detail-key">DESCRIPCIÓN:</span><span class="detail-val">${selectedPart.desc}</span></div>
    <div class="detail-row"><span class="detail-key">CORRESPONDE:</span><span class="detail-val">${selectedPart.corresponde || 'Autotanque'}</span></div>
    <div class="detail-row"><span class="detail-key">PIEZAS RELACIONADAS:</span><span class="detail-val">${selectedPart.repeats > 1 ? selectedPart.allNos.join(', ') : 'Sin repetición'}</span></div>
    <div class="detail-row"><span class="detail-key">MARCA:</span><span class="detail-val">${selectedPart.brand||'—'}</span></div>
    <div class="detail-row"><span class="detail-key">CANTIDAD POR UNIDAD:</span><span class="detail-val">${selectedPart.qty}</span></div>
  `;

  // Selected part info in form
  document.getElementById('selectedPartInfo').style.display = 'block';
  document.getElementById('selectedPartText').innerHTML = `
    <b style="color:var(--accent)">Pieza ${selectedPart.no}</b> — ${selectedPart.desc}<br>
    <span class="part-pn">${selectedPart.pn}${selectedPart.corresponde ? ` | ${selectedPart.corresponde}` : ''}${selectedPart.repeats > 1 ? ` | x${selectedPart.repeats}` : ''}</span>
  `;
  updateSecuritySealEvidenceUI();
  refreshSelectedPartImageUI();
}

// ── AUTOTANQUE SELECT ─────────────────────────────────────────────────
function populateATSelect() {
  const sortedUnits = sortUnitsByMode(autotanques, 'econ-asc');

  const sel = document.getElementById('formAT');
  sel.innerHTML = '<option value="">— Seleccionar autotanque —</option>' +
    sortedUnits.map(a => `<option value="${a.id}">${a.econ} | ${a.placa}</option>`).join('');

  ['filterReplAT'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.innerHTML = '<option value="">Todos los autotanques</option>' +
      sortedUnits.map(a => `<option value="${a.id}">${a.econ}</option>`).join('');
  });
}

function populateStationSelectForRegistro() {
  const sel = document.getElementById('formStation');
  if (!sel) return;
  const sorted = normalizeEstaciones(estaciones)
    .sort((a, b) =>
      compareTextNatural(a.planta, b.planta) ||
      compareTextNatural(a.estacion, b.estacion) ||
      compareTextNatural(a.bomba, b.bomba)
    );
  sel.innerHTML = '<option value="">— Seleccionar estación —</option>' +
    sorted.map(s => `<option value="${s.id}">${s.planta} | ${s.estacion} | ${s.bomba}</option>`).join('');
}

function toggleRegistroTarget() {
  const type = document.getElementById('formTargetType')?.value || 'autotanque';
  const labelEl = document.getElementById('formTargetLabel');
  const atWrap = document.getElementById('formATWrap');
  const stWrap = document.getElementById('formStationWrap');
  const previewCard = document.getElementById('partPreviewCard');
  if (!labelEl || !atWrap || !stWrap) return;

  selectedPart = null;
  const selectedInfo = document.getElementById('selectedPartInfo');
  if (selectedInfo) selectedInfo.style.display = 'none';

  if (type === 'estacion') {
    labelEl.textContent = 'ESTACIÓN DE CARBURACIÓN';
    atWrap.style.display = 'none';
    stWrap.style.display = 'block';
    if (previewCard) previewCard.style.display = 'none';
    populateStationSelectForRegistro();
  } else {
    labelEl.textContent = 'AUTOTANQUE';
    atWrap.style.display = 'block';
    stWrap.style.display = 'none';
    if (previewCard) previewCard.style.display = 'none';
  }
  renderPartList();
  updateSecuritySealEvidenceUI();
  refreshSelectedPartImageUI();
}

// ── SAVE COMPONENT RECORD ─────────────────────────────────────────────
async function saveComponentRecord() {
  if (!selectedPart) return alert('Selecciona un componente de la lista.');
  const targetType = document.getElementById('formTargetType')?.value || 'autotanque';
  const notesText = document.getElementById('formNotes').value || '';
  const serialText = document.getElementById('formSerial').value || '';
  const fabRawCode = document.getElementById('formFabDate').value || '';
  const parsedFab = parseFabCode(fabRawCode);
  if (!parsedFab && !notesText.trim() && !serialText.trim()) {
    return alert('Ingresa un código/fecha válida, un número de serie o agrega una nota cuando no se cuenta con el componente.');
  }
  const fabDate = parsedFab ? parsedFab.iso : '';
  const replYears = getReplacementYearsForPart(selectedPart || {});
  const replDateAuto = fabDate ? addYears(fabDate, replYears) : '';
  const notesFinal = ensureNoValveTagInNotes(notesText, Boolean(fabDate));

  if (targetType === 'estacion') {
    const stationId = document.getElementById('formStation')?.value || '';
    if (!stationId) return alert('Selecciona una estación.');
    const station = estaciones.find(s => s.id === stationId);
    if (!station) return alert('No se encontró la estación seleccionada.');
    const stationRec = {
      id: genId(),
      stationId,
      partNo: selectedPart.repeats > 1 ? selectedPart.allNos.join('/') : selectedPart.no,
      partPn: selectedPart.pn,
      partDesc: selectedPart.desc,
      partBrand: selectedPart.brand,
      fabDate,
      instDate: document.getElementById('formInstDate').value,
      replDate: document.getElementById('formReplDate').value || replDateAuto,
      serial: document.getElementById('formSerial').value,
      brand: document.getElementById('formBrand').value || selectedPart.brand,
      notes: notesFinal,
      createdAt: new Date().toISOString(),
    };
    if (!(await insertStationRecordRemote(stationRec))) return;
    stationRecords.push(stationRec);
    const idx = estaciones.findIndex(s => s.id === stationId);
    if (idx >= 0) {
      const set = new Set(Array.isArray(estaciones[idx].componentes) ? estaciones[idx].componentes : []);
      set.add(selectedPart.desc);
      estaciones[idx].componentes = [...set];
    }
    save();
    clearForm();
    alert(`✅ Registro guardado para estación ${station.estacion} (${station.bomba}) — Pieza ${stationRec.partNo}`);
    renderEstaciones();
    renderDashboard();
    return;
  }
  const atId = document.getElementById('formAT').value;
  if (!atId) return alert('Selecciona un autotanque.');
  const sealEvidenceEnabled = shouldOfferSecuritySealEvidence();
  const sealEvidenceFiles = sealEvidenceEnabled ? getSecuritySealEvidenceFiles() : [];

  const recId = genId();
  let notesWithEvidence = notesFinal;
  if (sealEvidenceFiles.length) {
    const evidenceResult = await attachSecuritySealEvidenceToAutotanque(atId, recId, sealEvidenceFiles);
    if (!evidenceResult.ok) return alert(evidenceResult.message || 'No se pudo guardar la evidencia de sellos.');
    const extraNote = `EVIDENCIA DE SELLOS: ${sealEvidenceFiles.length} archivo(s) adjuntos en expediente del autotanque.`;
    notesWithEvidence = notesFinal ? `${notesFinal} | ${extraNote}` : extraNote;
  }

  const rec = {
    id:       recId,
    atId,
    partNo:   selectedPart.no,
    partPn:   selectedPart.pn,
    partDesc: selectedPart.desc,
    partBrand:selectedPart.brand,
    fabDate,
    instDate:  document.getElementById('formInstDate').value,
    replDate:  document.getElementById('formReplDate').value || replDateAuto,
    serial:    document.getElementById('formSerial').value,
    brand:     document.getElementById('formBrand').value || selectedPart.brand,
    notes:     notesWithEvidence,
    createdAt: new Date().toISOString(),
  };

  if (!(await insertRecordRemote(rec))) return;
  records.push(rec);
  save();
  clearForm();
  alert(`✅ Registro guardado para Pieza ${rec.partNo} — ${rec.partDesc}`);
  renderDashboard();
}

function clearForm() {
  ['formAT','formStation','formFabDate','formInstDate','formReplDate','formSerial','formBrand','formNotes']
    .forEach(id => document.getElementById(id).value = '');
  document.getElementById('formTargetType').value = 'autotanque';
  toggleRegistroTarget();
  const hintEl = document.getElementById('formFabHint');
  if (hintEl) hintEl.innerHTML = `Formatos válidos: <b>6A92</b>, <b>9C22</b>, <b>09C22</b> o <b>27/04/2026</b>. Regla general: <b>${REPLACEMENT_YEARS} años</b> (Manguera 20BHB: <b>${HOSE_20BHB_MAX_YEARS} años máx.</b>).`;
  document.getElementById('partImageFile').value = '';
  document.getElementById('formSealEvidence').value = '';
  selectedPart = null;
  document.getElementById('selectedPartInfo').style.display = 'none';
  document.getElementById('partPreviewCard').style.display = 'none';
  updateSecuritySealEvidenceUI();
  refreshSelectedPartImageUI();
  renderPartList();
}

function renderDraftExpedienteList() {
  const box = document.getElementById('expedienteDraftList');
  if (!box) return;
  const visibleDocs = getVisibleExpedienteDocs(draftExpedienteDocs);
  if (!visibleDocs.length) {
    box.innerHTML = '<p class="text-muted" style="padding:10px 12px">Sin documentos en el expediente.</p>';
    return;
  }

  box.innerHTML = visibleDocs.map(doc => `
    <div class="expediente-item">
      <div class="expediente-meta">
        <b>${escapeHtml(doc.name || doc.fileName || 'Documento')}</b>
        <div class="small">${escapeHtml(expedienteCategoryLabel(doc.category))} | ${escapeHtml(doc.fileName || 'Sin nombre')} | ${formatBytes(doc.size)}</div>
      </div>
      <button class="btn btn-danger" style="padding:4px 8px;font-size:10px" type="button" onclick="removeDraftExpDoc('${doc.id}')">ELIMINAR</button>
    </div>
  `).join('');
}

function readFileAsDataUrl(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = ev => resolve(ev.target.result);
    reader.onerror = () => reject(new Error('No se pudo leer el archivo seleccionado.'));
    reader.readAsDataURL(file);
  });
}

async function saveSelectedPartImage() {
  if (!selectedPart) return alert('Selecciona una pieza para cargar su imagen.');
  const fileInput = document.getElementById('partImageFile');
  const file = fileInput?.files?.[0];
  if (!file) return alert('Selecciona un archivo de imagen.');
  if (file.size > MAX_PART_IMAGE_SIZE_BYTES) {
    return alert('Imagen demasiado grande. Usa archivos de hasta 3 MB.');
  }

  try {
    const targetType = getRegistroTargetType();
    const imageKey = targetType === 'estacion' ? `estacion:${selectedPart.no}` : selectedPart.no;
    const payload = {
      partNo: imageKey,
      fileName: file.name || `${selectedPart.no}.png`,
      mimeType: file.type || 'image/png',
      sizeBytes: file.size || 0,
      dataUrl: await readFileAsDataUrl(file),
      updatedAt: new Date().toISOString()
    };

    if (!(await upsertPartImageRemote(payload))) return;
    partImages[payload.partNo] = payload;
    if (!save()) return;
    fileInput.value = '';
    refreshSelectedPartImageUI();
    alert(`✅ Imagen guardada para la pieza ${selectedPart.no}.`);
  } catch {
    alert('No se pudo leer o guardar la imagen seleccionada.');
  }
}

async function removeSelectedPartImage() {
  if (!selectedPart) return alert('Selecciona una pieza.');
  const targetType = getRegistroTargetType();
  const partNo = targetType === 'estacion' ? `estacion:${selectedPart.no}` : selectedPart.no;
  if (!partImages[partNo]) return alert('La pieza seleccionada no tiene imagen personalizada.');
  if (!confirm(`¿Quitar imagen personalizada de la pieza ${selectedPart.no}?`)) return;

  if (!(await deletePartImageRemote(partNo))) return;
  delete partImages[partNo];
  if (!save()) return;
  document.getElementById('partImageFile').value = '';
  refreshSelectedPartImageUI();
}

async function createExpedienteDocFromFile(file, category, customName) {
  if (!file) throw new Error('Selecciona un archivo para agregar al expediente.');
  const maxSize = getMaxDocSizeBytes();
  if (file.size > maxSize) {
    const maxMb = (maxSize / (1024 * 1024)).toFixed(1).replace('.0', '');
    throw new Error(`Archivo demasiado grande. Usa archivos de hasta ${maxMb} MB.`);
  }

  const docId = genId();
  const baseDoc = {
    id: docId,
    category,
    name: customName || file.name,
    fileName: file.name,
    mimeType: file.type || 'application/octet-stream',
    size: file.size,
    uploadedAt: new Date().toISOString()
  };

  if (runtimeUseSupabase) {
    const atId = currentDraftATId || editingATId || genId();
    currentDraftATId = atId;
    const uploaded = await uploadExpedienteFileRemote(file, atId, docId);
    if (!uploaded) throw new Error('No se pudo subir el documento a Supabase Storage.');
    return {
      ...baseDoc,
      storagePath: uploaded.path,
      storageBucket: uploaded.bucket
    };
  }

  return {
    ...baseDoc,
    dataUrl: await readFileAsDataUrl(file)
  };
}

async function attachSecuritySealEvidenceToAutotanque(atId, recordId, files = []) {
  const atIndex = autotanques.findIndex(a => a.id === atId);
  if (atIndex < 0) {
    return { ok: false, message: 'No se encontró el autotanque para guardar la evidencia de sellos.' };
  }

  const at = autotanques[atIndex];
  const nextExpediente = normalizeExpediente(at.expediente).map(d => ({ ...d }));
  const uploadedDocs = [];
  const uploadedPaths = [];
  const prevDraftAtId = currentDraftATId;
  currentDraftATId = atId;

  try {
    for (let i = 0; i < files.length; i++) {
      const file = files[i];
      const customName = `Evidencia sello seguridad ${i + 1}/${files.length} | Pieza ${selectedPart?.no || ''} | Registro ${recordId}`;
      const doc = await createExpedienteDocFromFile(file, 'otro', customName);
      uploadedDocs.push(doc);
      if (doc?.storagePath) uploadedPaths.push(doc.storagePath);
    }
  } catch (err) {
    if (uploadedPaths.length) await deleteExpedienteFilesRemote(uploadedPaths);
    currentDraftATId = prevDraftAtId;
    return { ok: false, message: err?.message || 'No se pudo cargar la evidencia de sellos.' };
  }

  currentDraftATId = prevDraftAtId;
  const updatedAt = {
    ...at,
    expediente: [...nextExpediente, ...uploadedDocs]
  };
  if (!(await upsertUnitRemote(updatedAt))) {
    if (uploadedPaths.length) await deleteExpedienteFilesRemote(uploadedPaths);
    return { ok: false, message: 'No se pudo guardar la evidencia de sellos en el expediente del autotanque.' };
  }

  autotanques[atIndex] = updatedAt;
  return { ok: true, uploadedDocs };
}

async function addExpedienteDoc() {
  const fileInput = document.getElementById('expDocFile');
  const category = document.getElementById('expDocCategory').value || 'otro';
  const customName = document.getElementById('expDocName').value.trim();
  const file = fileInput?.files?.[0];

  try {
    const doc = await createExpedienteDocFromFile(file, category, customName);
    draftExpedienteDocs.push(doc);
    document.getElementById('expDocName').value = '';
    document.getElementById('expDocFile').value = '';
    renderDraftExpedienteList();
  } catch (err) {
    alert(err.message || 'No se pudo agregar el documento.');
  }
}

async function removeDraftExpDoc(docId) {
  const doc = draftExpedienteDocs.find(d => d.id === docId);
  if (doc?.storagePath) {
    if (originalExpedientePaths.has(doc.storagePath)) {
      if (!pendingExpedienteDeletePaths.includes(doc.storagePath)) {
        pendingExpedienteDeletePaths.push(doc.storagePath);
      }
    } else {
      await deleteExpedienteFilesRemote([doc.storagePath]);
    }
  }
  draftExpedienteDocs = draftExpedienteDocs.filter(d => d.id !== docId);
  renderDraftExpedienteList();
}

async function openExpedienteDoc(atId, docId) {
  const at = autotanques.find(a => a.id === atId);
  const doc = at?.expediente?.find(d => d.id === docId);
  if (!doc) return alert('No se encontro el documento.');
  const accessUrl = await getExpedienteDocAccessUrl(doc);
  if (!accessUrl) return alert('No se pudo abrir el documento.');
  const a = document.createElement('a');
  a.href = accessUrl;
  a.target = '_blank';
  a.rel = 'noopener';
  a.download = doc.fileName || `${doc.name || 'documento'}.bin`;
  a.click();
}

function renderExpedienteTable(expedienteDocs, atId) {
  const docs = getVisibleExpedienteDocs(expedienteDocs);
  if (!docs.length) {
    return '<p class="text-muted">Sin documentos en expediente.</p>';
  }

  return `
    <div class="table-wrap" style="max-height:240px;overflow-y:auto">
      <table>
        <thead><tr><th>TIPO</th><th>NOMBRE</th><th>ARCHIVO</th><th>CARGA</th><th>ACCION</th></tr></thead>
        <tbody>
          ${docs.map(doc => `
            <tr>
              <td>${escapeHtml(expedienteCategoryLabel(doc.category))}</td>
              <td>${escapeHtml(doc.name || 'Documento')}</td>
              <td style="font-family:monospace;font-size:11px">${escapeHtml(doc.fileName || 'Sin nombre')}<br><span class="text-muted">${formatBytes(doc.size)}</span></td>
              <td>${escapeHtml(formatDateTime(doc.uploadedAt))}</td>
              <td><button class="btn btn-secondary" style="padding:4px 8px;font-size:10px" onclick="openExpedienteDoc('${atId}','${doc.id}')">VER</button></td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function setAuthError(message = '') {
  const errEl = document.getElementById('loginError');
  if (errEl) errEl.textContent = message;
}

function clearAuthIdleTimer() {
  if (!authIdleTimer) return;
  clearTimeout(authIdleTimer);
  authIdleTimer = null;
}

async function forceLogoutByInactivity() {
  if (authLogoutInProgress) return;
  authLogoutInProgress = true;
  await logoutApp({ dueToInactivity: true });
  authLogoutInProgress = false;
}

function startAuthIdleTimer() {
  clearAuthIdleTimer();
  if (document.body.classList.contains('auth-locked')) return;
  authIdleTimer = setTimeout(forceLogoutByInactivity, AUTH_IDLE_TIMEOUT_MS);
}

function bindAuthActivityWatchers() {
  if (authActivityBound) return;
  const events = ['mousemove', 'mousedown', 'keydown', 'scroll', 'touchstart'];
  const onActivity = () => startAuthIdleTimer();
  events.forEach(evt => window.addEventListener(evt, onActivity, { passive: true }));
  authActivityBound = true;
}

function setAuthLocked(locked, displayName = '') {
  document.body.classList.toggle('auth-locked', locked);
  const gate = document.getElementById('authGate');
  if (gate) gate.classList.toggle('open', locked);
  activeAuditUser = locked ? 'Sin sesión' : (String(displayName || '').trim() || 'Usuario');

  const userEl = document.getElementById('authCurrentUser');
  if (userEl) userEl.textContent = locked ? '' : `Usuario: ${displayName || 'Activo'}`;

  const logoutBtn = document.getElementById('btnLogout');
  if (logoutBtn) logoutBtn.style.display = locked ? 'none' : 'inline-flex';

  if (locked) clearAuthIdleTimer();
  else startAuthIdleTimer();
  if (locked) setTimeout(() => document.getElementById('loginUser')?.focus(), 60);
  renderChangeHistory();
}

async function getAuthUserFromSupabase() {
  if (!supabaseClient) return null;
  const { data, error } = await supabaseClient.auth.getSession();
  if (error) return null;
  return data?.session?.user || null;
}

async function handleLoginSubmit(ev) {
  ev.preventDefault();
  if (!SUPABASE_ENABLED || !supabaseClient) {
    setAuthError('Supabase no está configurado. Revisa URL y ANON KEY.');
    return;
  }

  const userInput = String(document.getElementById('loginUser')?.value || '').trim();
  const passInput = document.getElementById('loginPass')?.value || '';
  if (!userInput || !passInput) {
    setAuthError('Ingresa usuario/correo y contraseña.');
    return;
  }

  setAuthError('');
  const { data, error } = await supabaseClient.auth.signInWithPassword({
    email: userInput,
    password: passInput
  });
  if (error || !data?.user) {
    setAuthError('Credenciales inválidas o usuario no confirmado.');
    return;
  }

  const displayName = data.user.user_metadata?.full_name || data.user.email || data.user.id;
  setAuthLocked(false, displayName);
  await bootstrapApp();
}

async function logoutApp(options = {}) {
  const dueToInactivity = Boolean(options?.dueToInactivity);
  clearAuthIdleTimer();
  if (supabaseClient) {
    await supabaseClient.auth.signOut();
  }
  document.querySelectorAll('.modal-overlay.open').forEach(el => el.classList.remove('open'));
  const userEl = document.getElementById('loginUser');
  if (userEl) userEl.value = '';
  const passEl = document.getElementById('loginPass');
  if (passEl) passEl.value = '';
  setAuthLocked(true);
  if (dueToInactivity) {
    setAuthError(`Sesión cerrada por inactividad (${AUTH_IDLE_TIMEOUT_MINUTES} min).`);
  } else {
    setAuthError('');
  }
}

function bindLoginForm() {
  const form = document.getElementById('loginForm');
  if (!form || form.dataset.bound === '1') return;
  form.addEventListener('submit', handleLoginSubmit);
  form.dataset.bound = '1';
}

// Auto-calc replacement date
async function bootstrapApp() {
  if (appBootstrapped) return;
  appBootstrapped = true;

  document.getElementById('formFabDate')?.addEventListener('input', updateReplacementFromFabInput);
  document.getElementById('formSealEvidence')?.addEventListener('change', updateSecuritySealEvidenceUI);
  document.getElementById('editRecFabDate')?.addEventListener('change', e => {
    const replEl = document.getElementById('editRecReplDate');
    if (!replEl) return;
    if (!replEl.value && e.target.value) {
      const current = records.find(r => r.id === editingRecordId) || {};
      const years = getReplacementYearsForPart({
        partNo: current.partNo,
        partPn: current.partPn,
        partDesc: current.partDesc
      });
      replEl.value = addYears(e.target.value, years);
    }
  });
  bindDashboardControls();
  syncConfigInputs();
  updateStorageModeLabel();
  if (runtimeUseSupabase) {
    const loaded = await loadFromSupabase();
    if (!loaded) updateStorageModeLabel('error de carga, revisa SQL/politicas');
  }

  const policySync = applyReplacementPolicyToRecords(records);
  if (policySync.changed.length) {
    records = policySync.records;
    if (runtimeUseSupabase) {
      const synced = await upsertRecordsRemote(
        policySync.changed,
        'migrar reemplazos automáticos según política por componente'
      );
      if (synced) {
        updateStorageModeLabel(`reemplazos recalculados por política (${policySync.changed.length} registros)`);
      }
    } else {
      save();
    }
  }

  populatePlantSelectors();
  updateSecuritySealEvidenceUI();
  refreshSelectedPartImageUI();
  renderPartList();
  renderEstaciones();
  populateATSelect();
  populateStationSelectForRegistro();
  toggleRegistroTarget();
  renderDashboard();
  renderAutotanques();
  renderDraftExpedienteList();
  renderChangeHistory();
  lastAuditSnapshot = buildAuditSnapshot();
  changeAuditReady = true;
}

document.addEventListener('DOMContentLoaded', async () => {
  bindLoginForm();
  bindAuthActivityWatchers();
  if (!SUPABASE_ENABLED || !supabaseClient) {
    setAuthLocked(true);
    setAuthError('Configura Supabase para usar login seguro.');
    return;
  }

  const user = await getAuthUserFromSupabase();
  if (!user) {
    setAuthLocked(true);
    return;
  }

  const displayName = user.user_metadata?.full_name || user.email || user.id;
  setAuthLocked(false, displayName);
  await bootstrapApp();
});

// ── AUTOTANQUES CRUD ─────────────────────────────────────────────────
function openModalAutotanque(id) {
  populatePlantSelectors();
  editingATId = id || null;
  currentDraftATId = id || genId();
  pendingExpedienteDeletePaths = [];
  originalExpedientePaths = new Set();
  document.getElementById('modalATTitle').textContent = id ? 'EDITAR AUTOTANQUE' : 'NUEVO AUTOTANQUE';
  document.getElementById('expDocCategory').value = 'seguro';
  document.getElementById('expDocName').value = '';
  document.getElementById('expDocFile').value = '';
  if (id) {
    const at = autotanques.find(a => a.id === id);
    if (!at) return alert('No se encontró el autotanque seleccionado.');
    document.getElementById('atEcon').value        = at.econ;
    document.getElementById('atPlaca').value       = at.placa;
    document.getElementById('atPlantaActual').value= at.plantaActual || '';
    document.getElementById('atSerieUnidad').value = at.serieUnidad;
    document.getElementById('atSerieTanque').value = at.serieTanque;
    document.getElementById('atCapacidad').value   = at.capacidad;
    document.getElementById('atAnio').value        = at.anio;
    document.getElementById('atNotas').value       = at.notas;
    document.getElementById('atActivo').checked    = at.activo !== false;
    document.getElementById('atEnServicio').checked= at.enServicio !== false;
    document.getElementById('atMarcaUnidad').value = at.marcaUnidad || '';
    document.getElementById('atModeloUnidad').value= at.modeloUnidad || '';
    document.getElementById('atNom013Mes').value = at.nom013Mes || '';
    document.getElementById('atNom013Anio').value = at.nom013Anio || '';
    document.getElementById('atNom007SeshMes').value = at.nom007SeshMes || '';
    document.getElementById('atNom007SeshAnio').value = at.nom007SeshAnio || '';
    document.getElementById('atRegistroSener').value = at.registroSener || '';
    document.getElementById('atNoRegTagSener').value = at.noRegTagSener || '';
    draftExpedienteDocs = (at.expediente || []).map(d => ({ ...d }));
    originalExpedientePaths = new Set(draftExpedienteDocs.map(d => d.storagePath).filter(Boolean));
  } else {
    ['atEcon','atPlaca','atPlantaActual','atSerieUnidad','atSerieTanque','atCapacidad','atAnio','atNotas','atMarcaUnidad','atModeloUnidad','atNom013Mes','atNom013Anio','atNom007SeshMes','atNom007SeshAnio','atRegistroSener','atNoRegTagSener']
      .forEach(id => document.getElementById(id).value = '');
    document.getElementById('atActivo').checked = true;
    document.getElementById('atEnServicio').checked = true;
    draftExpedienteDocs = [];
  }
  renderDraftExpedienteList();
  document.getElementById('modalAT').classList.add('open');
}

async function saveAutotanque() {
  const econ = document.getElementById('atEcon').value.trim();
  const placa = document.getElementById('atPlaca').value.trim();
  const plantaRaw = String(document.getElementById('atPlantaActual').value || '').trim();
  const plantaActual = normalizePlantName(plantaRaw) || plantaRaw;
  if (!econ || !placa) return alert('No. Económico y Placas son obligatorios.');
  if (!plantaActual) return alert('Selecciona la planta actual.');
  const unitId = editingATId || currentDraftATId || genId();
  const currentAt = editingATId ? autotanques.find(a => a.id === editingATId) : null;

  const pendingFile = document.getElementById('expDocFile')?.files?.[0];
  if (pendingFile) {
    const category = document.getElementById('expDocCategory').value || 'otro';
    const customName = document.getElementById('expDocName').value.trim();
    try {
      const doc = await createExpedienteDocFromFile(pendingFile, category, customName);
      draftExpedienteDocs.push(doc);
      document.getElementById('expDocName').value = '';
      document.getElementById('expDocFile').value = '';
      renderDraftExpedienteList();
    } catch (err) {
      return alert(err.message || 'No se pudo agregar el archivo pendiente del expediente.');
    }
  }

  const data = {
    id: unitId,
    econ, placa,
    plantaActual,
    serieUnidad: document.getElementById('atSerieUnidad').value,
    serieTanque: document.getElementById('atSerieTanque').value,
    capacidad:   document.getElementById('atCapacidad').value,
    anio:        document.getElementById('atAnio').value,
    notas:       document.getElementById('atNotas').value,
    activo:      document.getElementById('atActivo').checked,
    enServicio:  document.getElementById('atEnServicio').checked,
    marcaUnidad: document.getElementById('atMarcaUnidad').value,
    modeloUnidad: document.getElementById('atModeloUnidad').value,
    dictamenNomMes: currentAt?.dictamenNomMes || '',
    dictamenNomAnio: currentAt?.dictamenNomAnio || '',
    nom013Mes: document.getElementById('atNom013Mes').value,
    nom013Anio: document.getElementById('atNom013Anio').value,
    nom007SeshMes: document.getElementById('atNom007SeshMes').value,
    nom007SeshAnio: document.getElementById('atNom007SeshAnio').value,
    registroSener: document.getElementById('atRegistroSener').value,
    noRegTagSener: document.getElementById('atNoRegTagSener').value,
    expediente:  draftExpedienteDocs.map(d => ({ ...d })),
  };

  if (!(await upsertUnitRemote(data))) return;

  if (editingATId) {
    const i = autotanques.findIndex(a => a.id === editingATId);
    autotanques[i] = { ...autotanques[i], ...data };
  } else {
    autotanques.push(data);
  }
  if (pendingExpedienteDeletePaths.length) {
    await deleteExpedienteFilesRemote(pendingExpedienteDeletePaths);
  }
  save();
  draftExpedienteDocs = [];
  pendingExpedienteDeletePaths = [];
  originalExpedientePaths = new Set();
  currentDraftATId = null;
  await closeModal('modalAT');
  populatePlantSelectors();
  renderAutotanques();
  populateATSelect();
}

async function deleteAutotanque(id) {
  if (!confirm('¿Eliminar este autotanque y todos sus registros?')) return;
  if (!(await deleteAutotanqueRemote(id))) return;
  autotanques = autotanques.filter(a => a.id !== id);
  records     = records.filter(r => r.atId !== id);
  maintenanceSchedule = maintenanceSchedule.filter(item => item.atId !== id);
  save();
  persistMaintenanceSchedule();
  populatePlantSelectors();
  renderAutotanques();
  renderReemplazos();
  renderDashboard();
}

function renderAutotanques() {
  const q = (document.getElementById('searchAT')?.value || '').toLowerCase();
  const orderMode = document.getElementById('sortAT')?.value || 'econ-asc';
  const selectedPlantRaw = String(document.getElementById('filterATPlant')?.value || '').trim();
  const selectedPlant = normalizePlantName(selectedPlantRaw) || selectedPlantRaw;
  const tbody = document.getElementById('tableAT');
  if (!tbody) return;

  const filtered = autotanques.filter(a => {
    const plant = normalizePlantName(a.plantaActual || '');
    if (selectedPlant && plant !== selectedPlant) return false;
    return (
      a.econ.toLowerCase().includes(q) ||
      a.placa.toLowerCase().includes(q) ||
      (a.plantaActual||'').toLowerCase().includes(q) ||
      (a.serieUnidad||'').toLowerCase().includes(q) ||
      (a.serieTanque||'').toLowerCase().includes(q) ||
      (a.marcaUnidad||'').toLowerCase().includes(q) ||
      (a.modeloUnidad||'').toLowerCase().includes(q) ||
      (a.noRegTagSener||'').toLowerCase().includes(q)
    );
  });
  const sorted = sortUnitsByMode(filtered, orderMode);

  if (!sorted.length) {
    tbody.innerHTML = '<tr><td colspan="10" style="text-align:center;color:var(--muted);padding:30px">Sin autotanques registrados. Haz clic en "+ NUEVO AUTOTANQUE".</td></tr>';
    renderAutotanquesExpiryMatrix([]);
    return;
  }

  const plantsMap = new Map();
  sorted.forEach(at => {
    const plant = normalizePlantName(at.plantaActual || '') || String(at.plantaActual || '').trim() || 'SIN PLANTA';
    if (!plantsMap.has(plant)) plantsMap.set(plant, []);
    plantsMap.get(plant).push(at);
  });
  const plantEntries = Array.from(plantsMap.entries()).sort((a, b) => compareTextNatural(a[0], b[0]));

  if (selectedPlant) {
    const selectedPlantKey = encodeURIComponent(selectedPlant);
    if (expandedAutotanquePlantKey !== selectedPlantKey) expandedAutotanquePlantKey = selectedPlantKey;
  } else if (expandedAutotanquePlantKey) {
    const keyExists = plantEntries.some(([plant]) => encodeURIComponent(plant) === expandedAutotanquePlantKey);
    if (!keyExists) expandedAutotanquePlantKey = null;
  }

  tbody.innerHTML = plantEntries.map(([plant, units]) => {
    const plantKey = encodeURIComponent(plant);
    const expanded = expandedAutotanquePlantKey === plantKey;
    const unitsSorted = sortUnitsByMode(units, orderMode);

    const totalRecords = unitsSorted.reduce((acc, at) => acc + getLatestRecordsForUnit(at.id).length, 0);
    const plantSummary = `${unitsSorted.length} unidad(es) | ${totalRecords} componente(s)`;

    const unitRows = expanded ? unitsSorted.map(at => {
      const atRecs = getLatestRecordsForUnit(at.id);
      const expedienteDocs = getVisibleExpedienteDocs(at.expediente);
      const expedienteCount = expedienteDocs.length;
      const withDate = atRecs.filter(r => r.replDate);
      const vencidos = withDate.filter(r => daysUntil(r.replDate) < 0).length;
      const criticos = withDate.filter(r => { const d=daysUntil(r.replDate); return d>=0&&d<=90; }).length;

      let estado = '<span class="badge badge-ok">OK</span>';
      if (vencidos > 0) estado = `<span class="badge badge-danger">${vencidos} VENCIDO(S)</span>`;
      else if (criticos > 0) estado = `<span class="badge badge-warn">${criticos} CRÍTICO(S)</span>`;
      else if (!atRecs.length) estado = '<span class="badge badge-none">SIN REGISTROS</span>';

      const pct = atRecs.length ? Math.min(100, Math.round(atRecs.length/PARTS.length*100)) : 0;
      const pctClass = pct < 50 ? 'danger' : pct < 80 ? 'warn' : '';
      const expedienteEstado = expedienteCount
        ? `<span class="badge badge-ok">CAPTURADO (${expedienteCount})</span>`
        : '<span class="badge badge-none">SIN CAPTURAR</span>';

      return `<tr>
        <td><b style="color:var(--accent)">${escapeHtml(at.econ)}</b></td>
        <td>${escapeHtml(at.placa)}</td>
        <td style="font-family:monospace;font-size:11px">${escapeHtml(at.serieUnidad||'—')}</td>
        <td style="font-family:monospace;font-size:11px">${escapeHtml(at.serieTanque||'—')}</td>
        <td>${escapeHtml(at.capacidad?at.capacidad+' L':'—')}</td>
        <td>${escapeHtml(at.plantaActual || '—')}</td>
        <td>${expedienteEstado}</td>
        <td class="progress-cell">
          <div class="prog-bar"><div class="prog-fill ${pctClass}" style="width:${pct}%"></div></div>
          <span style="font-size:11px">${atRecs.length}/${PARTS.length}</span>
        </td>
        <td>${estado}</td>
        <td class="table-actions-cell">
          <div class="table-actions">
            <button class="btn btn-secondary action-btn" onclick="viewAutotanque('${at.id}')">VER</button>
            <button class="btn btn-secondary action-btn" onclick="openModalAutotanque('${at.id}')">✏️</button>
            <button class="btn btn-danger action-btn" onclick="deleteAutotanque('${at.id}')">🗑</button>
          </div>
        </td>
      </tr>`;
    }).join('') : '';

    return `
      <tr>
        <td colspan="10" class="plant-group-row bg-slate-50">
          <button class="station-group-toggle" type="button" onclick="toggleAutotanquesPlantGroup('${plantKey}')">
            <span class="accordion-chevron ${expanded ? 'expanded' : ''}">▶</span>
            <b style="color:var(--accent)">${escapeHtml(plant)}</b>
            <span style="color:var(--muted); font-size:11px">${plantSummary}</span>
          </button>
        </td>
      </tr>
      ${unitRows}
    `;
  }).join('');

  renderAutotanquesExpiryMatrix(sorted);
}

function getLatestRecordByPartForUnit(atId) {
  const byPart = new Map();
  records
    .filter(r => r.atId === atId)
    .forEach(rec => {
      const key = String(rec.partNo || '').trim();
      if (!key) return;
      const current = byPart.get(key);
      if (!current) {
        byPart.set(key, rec);
        return;
      }
      const curDate = new Date(current.createdAt || 0).getTime();
      const nextDate = new Date(rec.createdAt || 0).getTime();
      if (nextDate >= curDate) byPart.set(key, rec);
    });
  return byPart;
}

function getLatestRecordsForUnit(atId) {
  return Array.from(getLatestRecordByPartForUnit(atId).values())
    .sort((a, b) => compareTextNatural(a.partNo, b.partNo));
}

function renderAutotanquesExpiryMatrix(units = []) {
  const head = document.getElementById('tableATMatrixHead');
  const body = document.getElementById('tableATMatrixBody');
  const counter = document.getElementById('matrixCounter');
  if (!head || !body) return;

  const parts = [...PARTS].sort((a, b) => compareTextNatural(a.no, b.no));
  head.innerHTML = `
    <tr>
      <th class="matrix-sticky-col">Acciones</th>
      <th>Sucursal</th>
      <th>Tipo</th>
      <th>NoEco</th>
      <th>Placas</th>
      <th>Capacidad</th>
      <th>No. Serie Tanque</th>
      <th>Marca</th>
      <th>NOM-013<br>Vencimiento</th>
      <th>NOM-007-SESH-2010<br>Vencimiento</th>
      ${parts.map(p => `<th>Fecha Vencimiento<br>${escapeHtml(p.desc)}</th>`).join('')}
    </tr>
  `;
  if (counter) counter.textContent = `${units.length} Autotanque(s) encontrado(s)`;

  if (!units.length) {
    body.innerHTML = `<tr><td class="matrix-sticky-col" colspan="${10 + parts.length}" style="text-align:center;color:var(--muted);padding:20px">Sin autotanques para mostrar con el filtro actual.</td></tr>`;
    return;
  }

  body.innerHTML = units.map(unit => {
    const partMap = getLatestRecordByPartForUnit(unit.id);
    const brand = String(unit.marcaUnidad || '').trim();
    const partCells = parts.map(part => {
      const rec = partMap.get(String(part.no));
      return buildMatrixDateCell(rec?.replDate || '');
    }).join('');
    const nom013Expiry = getNormExpiryDate(unit, 5, 'nom013');
    const nom007SeshExpiry = getNormExpiryDate(unit, 1, 'nom007sesh');

    return `
      <tr>
        <td class="matrix-sticky-col table-actions-cell">
          <div class="matrix-actions table-actions">
            <div class="matrix-actions-icons">
              <button class="btn btn-secondary matrix-icon-btn action-btn" title="Ver" onclick="viewAutotanque('${unit.id}')">👁</button>
              <button class="btn btn-secondary matrix-icon-btn action-btn" title="Editar registro de reemplazo" onclick="openMatrixRecordEditor('${unit.id}')">✏️</button>
              <button class="btn btn-danger matrix-icon-btn action-btn" title="Eliminar registro de reemplazo" onclick="deleteMatrixRecord('${unit.id}')">🗑</button>
            </div>
            <button class="btn btn-secondary matrix-maint-btn action-btn" title="Registrar mantenimiento" onclick="openMaintenanceModal('${unit.id}')">Mant.</button>
          </div>
        </td>
        <td class="matrix-meta">${escapeHtml(unit.plantaActual || '—')}</td>
        <td class="matrix-meta">AT</td>
        <td class="matrix-meta">${escapeHtml(unit.econ || '—')}</td>
        <td class="matrix-meta">${escapeHtml(unit.placa || '—')}</td>
        <td class="matrix-meta">${escapeHtml(unit.capacidad ? `${unit.capacidad} L` : '—')}</td>
        <td class="matrix-meta">${escapeHtml(unit.serieTanque || '—')}</td>
        <td class="matrix-meta">${escapeHtml(brand || '—')}</td>
        ${buildMatrixDateCell(nom013Expiry)}
        ${buildMatrixDateCell(nom007SeshExpiry)}
        ${partCells}
      </tr>
    `;
  }).join('');

  syncAutotanquesMatrixScroll();
}

function syncAutotanquesMatrixScroll() {
  const top = document.getElementById('matrixTopScroll');
  const topInner = document.getElementById('matrixTopScrollInner');
  const wrap = document.querySelector('.matrix-wrap');
  const table = document.getElementById('tableATMatrix');
  if (!top || !topInner || !wrap || !table) return;

  topInner.style.width = `${Math.max(table.scrollWidth, wrap.scrollWidth)}px`;

  if (top.dataset.bound !== '1') {
    top.addEventListener('scroll', () => {
      if (wrap.scrollLeft !== top.scrollLeft) wrap.scrollLeft = top.scrollLeft;
    });
    top.dataset.bound = '1';
  }
  if (wrap.dataset.bound !== '1') {
    wrap.addEventListener('scroll', () => {
      if (top.scrollLeft !== wrap.scrollLeft) top.scrollLeft = wrap.scrollLeft;
    });
    wrap.dataset.bound = '1';
  }
}

function getUnitsForReplMatrix() {
  const filterAt = document.getElementById('filterReplAT')?.value || '';
  const filterStatus = document.getElementById('filterReplStatus')?.value || '';
  const searchTerm = normalizeMatchText(document.getElementById('searchRepl')?.value || '');
  let units = sortUnitsByMode(autotanques, 'econ-asc');
  if (filterAt) units = units.filter(u => u.id === filterAt);

  return units.filter(unit => {
    const partMap = getLatestRecordByPartForUnit(unit.id);
    const normExpiryDates = [
      getNormExpiryDate(unit, 5, 'nom013'), // NOM-013
      getNormExpiryDate(unit, 1, 'nom007sesh')  // NOM-007-SESH-2010
    ];

    if (filterStatus) {
      const hasStatus = PARTS.some(part => {
        const rec = partMap.get(String(part.no));
        if (!rec || !rec.replDate) return filterStatus === 'sin-fecha';
        return statusKey(daysUntil(rec.replDate)) === filterStatus;
      }) || normExpiryDates.some(dateValue => {
        if (!dateValue) return filterStatus === 'sin-fecha';
        return statusKey(daysUntil(dateValue)) === filterStatus;
      });
      if (!hasStatus) return false;
    }

    if (replMatrixColorFilter) {
      const hasColor = PARTS.some(part => {
        const rec = partMap.get(String(part.no));
        return recordMatchesMatrixColor(rec, replMatrixColorFilter);
      }) || normExpiryDates.some(dateValue => {
        const virtualRec = { replDate: dateValue };
        return recordMatchesMatrixColor(virtualRec, replMatrixColorFilter);
      });
      if (!hasColor) return false;
    }

    if (!searchTerm) return true;
    const recValues = Array.from(partMap.values()).flatMap(r => [
      r.partNo, r.partDesc, r.partPn, r.brand, r.serial, r.notes
    ]);
    const bag = [
      unit.econ, unit.placa, unit.plantaActual, unit.serieUnidad, unit.serieTanque, unit.capacidad, unit.notas,
      ...normExpiryDates,
      ...recValues
    ];
    return bag.some(v => normalizeMatchText(v).includes(searchTerm));
  });
}

function recordMatchesMatrixColor(rec, colorFilter) {
  if (!rec || !rec.replDate) return false;
  const d = daysUntil(rec.replDate);
  if (d === null) return false;
  if (colorFilter === 'expired') return d < 0;
  if (colorFilter === 'warning') return d >= 0 && d <= MATRIX_WARNING_DAYS;
  if (colorFilter === 'ok') return d > MATRIX_WARNING_DAYS;
  return true;
}

function updateReplMatrixLegendState() {
  const wrap = document.querySelector('.matrix-legend-inline');
  if (!wrap) return;
  wrap.querySelectorAll('[data-matrix-filter]').forEach(btn => {
    const filter = String(btn.getAttribute('data-matrix-filter') || '');
    const isActive = Boolean(replMatrixColorFilter) && filter === replMatrixColorFilter;
    btn.classList.toggle('active', isActive);
    btn.setAttribute('aria-pressed', isActive ? 'true' : 'false');
  });
}

function setReplMatrixColorFilter(colorFilter) {
  const next = replMatrixColorFilter === colorFilter ? '' : colorFilter;
  replMatrixColorFilter = next;
  updateReplMatrixLegendState();
  renderReemplazosExpiryMatrix();
}

function renderReemplazosExpiryMatrix() {
  const head = document.getElementById('tableReplMatrixHead');
  const body = document.getElementById('tableReplMatrixBody');
  const counter = document.getElementById('replMatrixCounter');
  if (!head || !body) return;
  updateReplMatrixLegendState();

  const units = getUnitsForReplMatrix();
  const parts = [...PARTS].sort((a, b) => compareTextNatural(a.no, b.no));
  head.innerHTML = `
    <tr>
      <th class="matrix-sticky-col matrix-fixed-col matrix-col-actions">Acciones</th>
      <th class="matrix-fixed-col">Planta</th>
      <th class="matrix-fixed-col matrix-col-type">Tipo</th>
      <th class="matrix-fixed-col matrix-col-eco">NoEco</th>
      <th class="matrix-fixed-col matrix-col-placas">Placas</th>
      <th class="matrix-fixed-col">Capacidad</th>
      <th class="matrix-fixed-col">No. Serie Tanque</th>
      <th class="matrix-fixed-col">Marca</th>
      <th class="matrix-part-col">
        <span class="matrix-head-title">F. Vencimiento</span>
        <span class="matrix-head-sub">NOM-013</span>
      </th>
      <th class="matrix-part-col">
        <span class="matrix-head-title">F. Vencimiento</span>
        <span class="matrix-head-sub">NOM-007-SESH-2010</span>
      </th>
      ${parts.map(p => `
        <th class="matrix-part-col">
          <span class="matrix-head-title">F. Vencimiento</span>
          <span class="matrix-head-sub">${escapeHtml(p.desc)}</span>
        </th>
      `).join('')}
    </tr>
  `;
  if (counter) counter.textContent = `${units.length} Autotanque(s) encontrado(s)`;

  if (!units.length) {
    body.innerHTML = `<tr><td class="matrix-sticky-col" colspan="${10 + parts.length}" style="text-align:center;color:var(--muted);padding:20px">Sin autotanques para mostrar con el filtro actual.</td></tr>`;
    syncReemplazosMatrixScroll();
    renderReplMaintenanceCalendar();
    requestAnimationFrame(() => maintenanceFullCalendar?.updateSize?.());
    return;
  }

  body.innerHTML = units.map(unit => {
    const partMap = getLatestRecordByPartForUnit(unit.id);
    const brand = String(unit.marcaUnidad || '').trim();
    const nom013Expiry = getNormExpiryDate(unit, 5, 'nom013');
    const nom007SeshExpiry = getNormExpiryDate(unit, 1, 'nom007sesh');

    const buildNormCell = dateValue => {
      const virtualRec = { replDate: dateValue };
      if (replMatrixColorFilter && !recordMatchesMatrixColor(virtualRec, replMatrixColorFilter)) {
        return '<td class="matrix-part-col matrix-empty">—</td>';
      }
      return buildMatrixDateCell(dateValue, 'matrix-part-col');
    };

    const partCells = parts.map(part => {
      const rec = partMap.get(String(part.no));
      if (replMatrixColorFilter && !recordMatchesMatrixColor(rec, replMatrixColorFilter)) {
        return '<td class="matrix-part-col matrix-empty">—</td>';
      }
      return buildMatrixDateCell(rec?.replDate || '', 'matrix-part-col');
    }).join('');

    return `
      <tr>
        <td class="matrix-sticky-col matrix-col-actions table-actions-cell">
          <div class="matrix-actions table-actions">
            <div class="matrix-actions-icons">
              <button class="btn btn-secondary matrix-icon-btn action-btn" title="Ver" onclick="viewAutotanque('${unit.id}')">👁</button>
              <button class="btn btn-secondary matrix-icon-btn action-btn" title="Editar registro de reemplazo" onclick="openMatrixRecordEditor('${unit.id}')">✏️</button>
              <button class="btn btn-danger matrix-icon-btn action-btn" title="Eliminar registro de reemplazo" onclick="deleteMatrixRecord('${unit.id}')">🗑</button>
            </div>
            <button class="btn btn-secondary matrix-maint-btn action-btn" title="Registrar mantenimiento" onclick="openMaintenanceModal('${unit.id}')">Mant.</button>
          </div>
        </td>
        <td class="matrix-meta">${escapeHtml(unit.plantaActual || '—')}</td>
        <td class="matrix-meta matrix-col-type">AT</td>
        <td class="matrix-meta matrix-col-eco">${escapeHtml(unit.econ || '—')}</td>
        <td class="matrix-meta matrix-col-placas">${escapeHtml(unit.placa || '—')}</td>
        <td class="matrix-meta">${escapeHtml(unit.capacidad || '—')}</td>
        <td class="matrix-meta">${escapeHtml(unit.serieTanque || '—')}</td>
        <td class="matrix-meta">${escapeHtml(brand || '—')}</td>
        ${buildNormCell(nom013Expiry)}
        ${buildNormCell(nom007SeshExpiry)}
        ${partCells}
      </tr>
    `;
  }).join('');

  syncReemplazosMatrixScroll();
  renderReplMaintenanceCalendar();
  requestAnimationFrame(() => maintenanceFullCalendar?.updateSize?.());
}

function getPreferredRecordForUnit(atId) {
  const unitRecords = getLatestRecordsForUnit(atId)
    .map(r => ({ ...r, days: daysUntil(r.replDate) }));
  if (!unitRecords.length) return null;

  unitRecords.sort((a, b) => {
    const da = a.days === null ? 999999 : a.days;
    const db = b.days === null ? 999999 : b.days;
    if (da !== db) return da - db;
    return compareTextNatural(a.partNo, b.partNo);
  });
  return unitRecords[0];
}

function openMatrixRecordEditor(atId) {
  const rec = getPreferredRecordForUnit(atId);
  if (!rec) return alert('Este autotanque no tiene registros de reemplazo para editar.');
  openRecordEditor(rec.id);
}

async function deleteMatrixRecord(atId) {
  const rec = getPreferredRecordForUnit(atId);
  if (!rec) return alert('Este autotanque no tiene registros de reemplazo para eliminar.');
  await deleteRecord(rec.id);
}

function syncReemplazosMatrixScroll() {
  const top = document.getElementById('matrixReplTopScroll');
  const topInner = document.getElementById('matrixReplTopScrollInner');
  const wrap = document.getElementById('replMatrixWrap');
  const table = document.getElementById('tableReplMatrix');
  if (!top || !topInner || !wrap || !table) return;

  topInner.style.width = `${Math.max(table.scrollWidth, wrap.scrollWidth)}px`;

  if (top.dataset.bound !== '1') {
    top.addEventListener('scroll', () => {
      if (wrap.scrollLeft !== top.scrollLeft) wrap.scrollLeft = top.scrollLeft;
    });
    top.dataset.bound = '1';
  }
  if (wrap.dataset.bound !== '1') {
    wrap.addEventListener('scroll', () => {
      if (top.scrollLeft !== wrap.scrollLeft) top.scrollLeft = wrap.scrollLeft;
    });
    wrap.dataset.bound = '1';
  }
}

function toggleAutotanquesPlantGroup(plantKey) {
  if (!plantKey) return;
  expandedAutotanquePlantKey = expandedAutotanquePlantKey === plantKey ? null : plantKey;
  renderAutotanques();
}

function partNoContains(partNoValue, targetPartNo) {
  const target = String(targetPartNo || '').trim();
  if (!target) return false;
  return String(partNoValue || '')
    .split('/')
    .map(x => String(x || '').trim())
    .filter(Boolean)
    .includes(target);
}

function isSecuritySealRecord(rec) {
  if (!rec) return false;
  if (partNoContains(rec.partNo, '13')) return true;
  return normalizeMatchText(rec.partDesc || '').includes('sello de seguridad');
}

function isImageDoc(doc) {
  const mime = String(doc?.mimeType || '').toLowerCase();
  const fileName = String(doc?.fileName || '').toLowerCase();
  return mime.startsWith('image/') || /\.(png|jpg|jpeg|webp|gif|bmp|svg)$/.test(fileName);
}

async function buildSealRegistryRowsForAutotanque(atId) {
  const at = autotanques.find(a => a.id === atId);
  if (!at) return [];
  const atRecs = records
    .filter(r => r.atId === atId)
    .sort((a, b) => compareTextNatural(a.partNo, b.partNo));
  const expedienteDocs = normalizeExpediente(at.expediente);
  const sealRecords = atRecs.filter(isSecuritySealRecord);

  return Promise.all(sealRecords.map(async rec => {
    const relatedDocs = expedienteDocs.filter(doc => {
      const docName = String(doc?.name || '');
      return docName.includes(`Registro ${rec.id}`) && normalizeMatchText(docName).includes('evidencia sello seguridad');
    });
    const docsWithUrl = await Promise.all(relatedDocs.map(async doc => ({
      ...doc,
      accessUrl: await getExpedienteDocAccessUrl(doc)
    })));
    return { rec, docs: docsWithUrl };
  }));
}

async function openSealRegistryForAutotanque(atId) {
  const at = autotanques.find(a => a.id === atId);
  if (!at) return alert('No se encontró el autotanque.');
  const rows = await buildSealRegistryRowsForAutotanque(atId);
  if (!rows.length) return alert('No hay sellos capturados para este autotanque.');

  const titleEl = document.getElementById('modalDashboardDrillTitle');
  const bodyEl = document.getElementById('modalDashboardDrillBody');
  if (!titleEl || !bodyEl) return;

  titleEl.textContent = `SELLOS COMPLETOS | ${at.econ} | ${at.placa}`;
  bodyEl.innerHTML = rows.map((row, index) => `
    <div class="card" style="margin-bottom:12px;padding:12px;background:var(--surface2)">
      <div class="detail-row"><span class="detail-key">SELLO ${index + 1}:</span><span class="detail-val">${escapeHtml(row.rec.partNo || '13')} — ${escapeHtml(row.rec.partDesc || 'Sello de seguridad')}</span></div>
      <div class="detail-row"><span class="detail-key">SERIE:</span><span class="detail-val" style="font-family:monospace">${escapeHtml(row.rec.serial || '—')}</span></div>
      <div class="detail-row"><span class="detail-key">F. FABRICACIÓN:</span><span class="detail-val">${formatDate(row.rec.fabDate)}</span></div>
      <div class="detail-row"><span class="detail-key">F. REEMPLAZO:</span><span class="detail-val">${formatDate(row.rec.replDate)}</span></div>
      <div class="section-sep" style="margin:8px 0"></div>
      <div style="font-size:12px;font-weight:700;margin-bottom:8px">IMÁGENES / EVIDENCIA (${row.docs.length})</div>
      ${
        row.docs.length
          ? `<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:10px">
              ${row.docs.map(doc => {
                const hasUrl = Boolean(doc.accessUrl);
                const imagePreview = hasUrl && isImageDoc(doc)
                  ? `<a href="${escapeHtml(doc.accessUrl)}" target="_blank" rel="noopener"><img src="${escapeHtml(doc.accessUrl)}" alt="${escapeHtml(doc.name || 'Evidencia')}" style="width:100%;height:120px;object-fit:cover;border:1px solid var(--border);border-radius:8px"></a>`
                  : `<div style="height:120px;display:flex;align-items:center;justify-content:center;border:1px dashed var(--border);border-radius:8px;color:var(--muted);font-size:12px;padding:6px;text-align:center">${escapeHtml(doc.fileName || doc.name || 'Archivo')}</div>`;
                const openBtn = hasUrl
                  ? `<a class="btn btn-secondary" style="padding:4px 8px;font-size:10px;margin-top:6px;display:inline-flex" href="${escapeHtml(doc.accessUrl)}" target="_blank" rel="noopener">ABRIR</a>`
                  : '<span class="text-muted" style="font-size:10px">Sin URL de acceso</span>';
                return `
                  <div>
                    ${imagePreview}
                    <div style="font-size:11px;margin-top:6px;white-space:pre-wrap;overflow-wrap:anywhere">${escapeHtml(doc.name || 'Evidencia')}</div>
                    ${openBtn}
                  </div>
                `;
              }).join('')}
            </div>`
          : '<p class="text-muted">No hay evidencia de sellos asociada a este registro.</p>'
      }
    </div>
  `).join('');

  document.getElementById('modalDashboardDrill').classList.add('open');
}

function viewAutotanque(id) {
  const at = autotanques.find(a => a.id === id);
  if (!at) return alert('No se encontró el autotanque.');
  const atRecs = records
    .filter(r => r.atId === id)
    .sort((a, b) => compareTextNatural(a.partNo, b.partNo));
  const expedienteDocs = getVisibleExpedienteDocs(at.expediente);
  const partsSorted = [...PARTS].sort((a, b) => compareTextNatural(a.no, b.no));
  const capturedPartCount = partsSorted.filter(part => atRecs.some(r => partNoContains(r.partNo, part.no))).length;

  document.getElementById('modalDetailTitle').textContent = `${at.econ} | ${at.placa}`;
  document.getElementById('modalDetailBody').innerHTML = `
    <div class="grid-2 detail-grid-fixed" style="margin-bottom:16px">
      <div class="detail-row"><span class="detail-key">ECONÓMICO:</span><span class="detail-val">${at.econ}</span></div>
      <div class="detail-row"><span class="detail-key">PLACA:</span><span class="detail-val">${at.placa}</span></div>
      <div class="detail-row"><span class="detail-key">SERIE UNIDAD:</span><span class="detail-val">${at.serieUnidad||'—'}</span></div>
      <div class="detail-row"><span class="detail-key">SERIE TANQUE:</span><span class="detail-val">${at.serieTanque||'—'}</span></div>
      <div class="detail-row"><span class="detail-key">CAPACIDAD:</span><span class="detail-val">${at.capacidad?at.capacidad+' L':'—'}</span></div>
      <div class="detail-row"><span class="detail-key">PLANTA ACTUAL:</span><span class="detail-val">${at.plantaActual || '—'}</span></div>
      <div class="detail-row"><span class="detail-key">ACTIVO:</span><span class="detail-val">${at.activo ? 'SI' : 'NO'}</span></div>
      <div class="detail-row"><span class="detail-key">EN SERVICIO:</span><span class="detail-val">${at.enServicio ? 'SI' : 'NO'}</span></div>
      <div class="detail-row"><span class="detail-key">MARCA:</span><span class="detail-val">${at.marcaUnidad || '—'}</span></div>
      <div class="detail-row"><span class="detail-key">MODELO:</span><span class="detail-val">${at.modeloUnidad || '—'}</span></div>
      <div class="detail-row"><span class="detail-key">EXPEDICIÓN NOM-013:</span><span class="detail-val">${(at.nom013Mes || at.nom013Anio) ? `${at.nom013Mes || '—'} / ${at.nom013Anio || '—'}` : '—'}</span></div>
      <div class="detail-row"><span class="detail-key">EXPEDICIÓN NOM-007-SESH-2010:</span><span class="detail-val">${(at.nom007SeshMes || at.nom007SeshAnio) ? `${at.nom007SeshMes || '—'} / ${at.nom007SeshAnio || '—'}` : '—'}</span></div>
      <div class="detail-row"><span class="detail-key">REGISTRO SENER:</span><span class="detail-val">${at.registroSener || '—'}</span></div>
      <div class="detail-row"><span class="detail-key">NO. REG. TAG SENER:</span><span class="detail-val">${at.noRegTagSener || '—'}</span></div>
      <div class="detail-row"><span class="detail-key">NOTAS AUTOTANQUE:</span><span class="detail-val">${escapeHtml(at.notas || '—')}</span></div>
    </div>
    <div class="section-sep"></div>
    <div class="card-title">DATOS DE REGISTRO (${capturedPartCount}/${partsSorted.length})</div>
    <div class="table-wrap" style="max-height:300px;overflow-y:auto">
      <table>
        <thead><tr><th>PIEZA</th><th>DESCRIPCIÓN</th><th>NÚM. SERIE</th><th>F.FAB</th><th>F.REEMPLAZO</th><th>ESTADO</th><th>NOTAS / OBS.</th><th>ACCIÓN</th></tr></thead>
        <tbody>
          ${partsSorted.map(part => {
            const recs = atRecs.filter(r => partNoContains(r.partNo, part.no));
            const latest = recs.slice().sort((a, b) => new Date(b.createdAt || 0) - new Date(a.createdAt || 0))[0] || null;
            const actionBtn = part.no === '13'
              ? `<button class="btn btn-secondary" style="padding:4px 8px;font-size:10px" onclick="openSealRegistryForAutotanque('${at.id}')">VER SELLOS</button>`
              : '<span class="text-muted">—</span>';
            return `
              <tr>
                <td style="font-family:monospace;color:var(--accent)">${escapeHtml(part.no || '—')}</td>
                <td>${escapeHtml(part.desc || latest?.partDesc || '—')}</td>
                <td style="font-family:monospace">${escapeHtml(latest?.serial || (part.no === '13' ? `${recs.length} sello(s)` : '—'))}</td>
                <td>${formatDate(latest?.fabDate || '')}</td>
                <td>${formatDate(latest?.replDate || '')}</td>
                <td>${latest ? statusBadge(daysUntil(latest.replDate)) : '<span class="badge badge-none">SIN REGISTRO</span>'}</td>
                <td style="font-size:11px; white-space:pre-wrap; overflow-wrap:anywhere">${escapeHtml(latest?.notes || '—')}</td>
                <td>${actionBtn}</td>
              </tr>
            `;
          }).join('')}
        </tbody>
      </table>
    </div>
    <div class="section-sep"></div>
    <div class="card-title">EXPEDIENTE DOCUMENTAL (${expedienteDocs.length})</div>
    ${renderExpedienteTable(expedienteDocs, at.id)}
  `;
  document.getElementById('modalDetailDelete').style.display = 'none';
  document.getElementById('modalDetail').classList.add('open');
}

// ── REEMPLAZOS ────────────────────────────────────────────────────────
function toggleReemplazoGroup(atId) {
  if (!atId) return;
  if (expandedReplGroups.has(atId)) expandedReplGroups.delete(atId);
  else expandedReplGroups.add(atId);
  renderReemplazos();
}

function renderReemplazos() {
  const tbody = document.getElementById('tableReemplazos');
  if (!tbody) {
    renderReemplazosExpiryMatrix();
    renderReplMaintenanceCalendar();
    return;
  }
  const filterAt     = document.getElementById('filterReplAT')?.value || '';
  const filterStatus = document.getElementById('filterReplStatus')?.value || '';
  const searchTerm   = (document.getElementById('searchRepl')?.value || '').trim().toLowerCase();

  let rows = autotanques.flatMap(at =>
    getLatestRecordsForUnit(at.id).map(r => ({
      ...r,
      at,
      days: daysUntil(r.replDate)
    }))
  );

  if (filterAt)     rows = rows.filter(r => r.atId === filterAt);
  if (filterStatus) rows = rows.filter(r => statusKey(r.days) === filterStatus);
  if (searchTerm) {
    rows = rows.filter(r => {
      const bag = [
        r.at?.econ,
        r.at?.placa,
        r.at?.plantaActual,
        r.partNo,
        r.partDesc,
        r.partPn,
        r.serial,
        r.brand,
        r.notes
      ].map(v => String(v || '').toLowerCase());
      return bag.some(v => v.includes(searchTerm));
    });
  }

  const sortRowsForReemplazos = (a, b) => {
    const da = a.days ?? 99999;
    const db = b.days ?? 99999;
    const atCmp = compareTextNatural(a.at?.econ, b.at?.econ) || compareTextNatural(a.at?.placa, b.at?.placa);
    const partCmp = compareTextNatural(a.partNo, b.partNo);
    // Mantener orden de autotanque y, dentro de cada uno, ordenar PIEZA No. ascendente.
    return atCmp || partCmp || (da - db);
  };
  rows.sort(sortRowsForReemplazos);

  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="10" style="text-align:center;color:var(--muted);padding:30px">Sin registros que mostrar.</td></tr>';
    renderReemplazosExpiryMatrix();
    return;
  }

  const groupsMap = new Map();
  rows.forEach(r => {
    const plant = String(r.at?.plantaActual || '').trim() || 'SIN PLANTA';
    const key = `plant::${plant.toUpperCase()}`;
    if (!groupsMap.has(key)) groupsMap.set(key, { key, plant, rows: [] });
    groupsMap.get(key).rows.push(r);
  });

  groupsMap.forEach(group => {
    group.rows.sort(sortRowsForReemplazos);
  });

  const groups = Array.from(groupsMap.values()).sort((a, b) => {
    const da = Math.min(...a.rows.map(x => x.days ?? 99999));
    const db = Math.min(...b.rows.map(x => x.days ?? 99999));
    if (da !== db) return da - db;
    return compareTextNatural(a.plant, b.plant);
  });

  if (filterAt) {
    const selected = rows.find(r => r.atId === filterAt);
    if (selected) {
      const plant = String(selected.at?.plantaActual || '').trim() || 'SIN PLANTA';
      expandedReplGroups.add(`plant::${plant.toUpperCase()}`);
    }
  }

  tbody.innerHTML = groups.map(group => {
    const plant = group.plant;
    const groupKey = group.key;
    const items = group.rows;
    const expanded = expandedReplGroups.has(groupKey);

    const vencidos = items.filter(x => x.days !== null && x.days < 0).length;
    const criticos = items.filter(x => x.days !== null && x.days >= 0 && x.days <= 90).length;
    const proximos = items.filter(x => x.days !== null && x.days > 90 && x.days <= 180).length;
    const sinFecha = items.filter(x => x.days === null).length;
    const unitsCount = new Set(items.map(x => x.atId)).size;

    let resumen = `${unitsCount} unidad(es) | ${items.length} registro(s)`;
    if (vencidos) resumen += ` | ${vencidos} vencido(s)`;
    else if (criticos) resumen += ` | ${criticos} crítico(s)`;
    else if (proximos) resumen += ` | ${proximos} próximo(s)`;
    if (sinFecha) resumen += ` | ${sinFecha} sin fecha`;

    const detailsHtml = expanded
      ? items.map(r => {
          const dStr = r.days === null ? '—' :
            r.days < 0 ? `<span style="color:var(--danger)">${Math.abs(r.days)} días VENCIDO</span>` :
            `${r.days} días`;

          return `<tr>
            <td style="color:var(--muted);font-size:11px">↳ ${r.at.econ} | ${r.at.placa}</td>
            <td style="font-family:monospace;color:var(--accent)">${r.partNo}</td>
            <td>${r.partDesc}</td>
            <td style="font-family:monospace;font-size:11px">${r.partPn}</td>
            <td>${formatDate(r.fabDate)}</td>
            <td>${formatDate(r.instDate)}</td>
            <td>${formatDate(r.replDate)}</td>
            <td>${dStr}</td>
            <td>${statusBadge(r.days)}</td>
            <td class="table-actions-cell">
              <div class="table-actions">
                <button class="btn btn-secondary action-btn" onclick="openRecordEditor('${r.id}')">✏️</button>
                <button class="btn btn-danger action-btn" onclick="deleteRecord('${r.id}')">🗑</button>
              </div>
            </td>
          </tr>`;
        }).join('')
      : '';

    return `
      <tr>
        <td colspan="10" style="background:var(--surface2); border-top:1px solid var(--border); border-bottom:1px solid var(--border);">
          <button class="btn btn-secondary" style="padding:4px 8px;font-size:10px; margin-right:8px" onclick="toggleReemplazoGroup('${groupKey}')">${expanded ? '▼' : '▶'}</button>
          <b style="color:var(--accent)">${plant}</b>
          <span style="color:var(--muted); font-size:11px; margin-left:8px">${resumen}</span>
        </td>
      </tr>
      ${detailsHtml}
    `;
  }).join('');
  renderReemplazosExpiryMatrix();
}

function openRecordEditor(id) {
  const rec = records.find(r => r.id === id);
  if (!rec) return alert('No se encontro el registro.');
  const at = autotanques.find(a => a.id === rec.atId);

  editingRecordId = id;
  document.getElementById('modalRecordEditTitle').textContent = 'EDITAR REGISTRO DE REEMPLAZO';
  document.getElementById('editRecAt').textContent = at ? `${at.econ} | ${at.placa}` : 'Autotanque no encontrado';
  document.getElementById('editRecPart').textContent = `Pieza ${rec.partNo} — ${rec.partDesc || 'Sin descripción'}`;

  document.getElementById('editRecFabDate').value = rec.fabDate || '';
  document.getElementById('editRecInstDate').value = rec.instDate || '';
  document.getElementById('editRecReplDate').value = rec.replDate || '';
  document.getElementById('editRecSerial').value = rec.serial || '';
  document.getElementById('editRecBrand').value = rec.brand || '';
  document.getElementById('editRecNotes').value = rec.notes || '';

  document.getElementById('modalRecordEdit').classList.add('open');
}

async function saveRecordEdit() {
  if (!editingRecordId) return alert('No hay registro seleccionado para editar.');
  const idx = records.findIndex(r => r.id === editingRecordId);
  if (idx < 0) return alert('No se encontro el registro a actualizar.');

  const current = records[idx];
  const fabDate = document.getElementById('editRecFabDate').value;
  if (!fabDate) return alert('La fecha de fabricación es obligatoria.');

  const instDate = document.getElementById('editRecInstDate').value || '';
  const replInput = document.getElementById('editRecReplDate').value || '';
  const replYears = getReplacementYearsForPart({
    partNo: current.partNo,
    partPn: current.partPn,
    partDesc: current.partDesc
  });
  const replDate = replInput || addYears(fabDate, replYears);

  const updated = {
    ...current,
    fabDate,
    instDate,
    replDate,
    serial: document.getElementById('editRecSerial').value || '',
    brand: document.getElementById('editRecBrand').value || '',
    notes: document.getElementById('editRecNotes').value || ''
  };

  if (!(await upsertRecordRemote(updated))) return;
  records[idx] = updated;
  save();
  await closeModal('modalRecordEdit');
  renderReemplazos();
  renderDashboard();
  renderAutotanques();
}

async function deleteRecord(id) {
  if (!confirm('¿Eliminar este registro?')) return;
  if (!(await deleteRecordRemote(id))) return;
  records = records.filter(r => r.id !== id);
  save();
  renderReemplazos();
  renderDashboard();
}

// ── DASHBOARD ─────────────────────────────────────────────────────────
function parseDateSafe(value) {
  if (!value) return null;
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? null : d;
}

function startOfDay(date) {
  const d = new Date(date);
  d.setHours(0, 0, 0, 0);
  return d;
}

function endOfDay(date) {
  const d = new Date(date);
  d.setHours(23, 59, 59, 999);
  return d;
}

function inferCreatedAt(entity) {
  const explicit = parseDateSafe(entity?.createdAt);
  if (explicit) return explicit;
  const id = String(entity?.id || '');
  if (id.length <= 4) return null;
  const prefix = id.slice(0, -4);
  const ts = Number.parseInt(prefix, 36);
  if (!Number.isFinite(ts)) return null;
  if (ts < 946684800000 || ts > Date.now() + (24 * 60 * 60 * 1000)) return null;
  const inferred = new Date(ts);
  return Number.isNaN(inferred.getTime()) ? null : inferred;
}

function daysUntilFrom(dateStr, refDate = new Date()) {
  if (!dateStr) return null;
  const target = new Date(`${dateStr}T00:00:00`);
  if (Number.isNaN(target.getTime())) return null;
  const ref = startOfDay(refDate);
  return Math.floor((target - ref) / (1000 * 60 * 60 * 24));
}

function dashboardStatusFromDays(days) {
  if (days === null) return 'sin-fecha';
  if (days < 0) return 'vencido';
  if (days <= 90) return 'critico';
  if (days <= 180) return 'proximo';
  return 'vigente';
}

function inRange(date, start, end) {
  if (!date) return false;
  if (start && date < start) return false;
  if (end && date > end) return false;
  return true;
}

function getDashboardFilters() {
  const plantRaw = String(document.getElementById('dashPlantFilter')?.value || '').trim();
  const fromDate = String(document.getElementById('dashFromDate')?.value || '').trim();
  const toDate = String(document.getElementById('dashToDate')?.value || '').trim();
  return {
    plant: normalizePlantName(plantRaw) || plantRaw,
    type: String(document.getElementById('dashTypeFilter')?.value || 'todos').trim(),
    fromDate,
    toDate,
    search: String(document.getElementById('dashSearch')?.value || '').trim(),
    searchNorm: normalizeMatchText(document.getElementById('dashSearch')?.value || '')
  };
}

function getDashboardDateBounds(filters) {
  const from = parseDateSafe(filters?.fromDate);
  const to = parseDateSafe(filters?.toDate);
  const start = from ? startOfDay(from) : null;
  const end = to ? endOfDay(to) : null;
  if (start && end && start > end) return { start: endOfDay(to), end: startOfDay(from) };
  return { start, end };
}

function getDashboardTrendWindow(filters) {
  const bounds = getDashboardDateBounds(filters);
  const now = new Date();
  const currentEnd = bounds.end || endOfDay(now);
  const currentStart = bounds.start || startOfDay(new Date(currentEnd.getTime() - (29 * 24 * 60 * 60 * 1000)));
  const windowMs = currentEnd.getTime() - currentStart.getTime();
  const prevEnd = endOfDay(new Date(currentStart.getTime() - 1));
  const prevStart = startOfDay(new Date(prevEnd.getTime() - windowMs));
  return { currentStart, currentEnd, prevStart, prevEnd };
}

function formatTrend(delta) {
  if (!Number.isFinite(delta) || delta === 0) return { text: '0', cls: 'trend-flat' };
  if (delta > 0) return { text: `+${delta}`, cls: 'trend-up' };
  return { text: `${delta}`, cls: 'trend-down' };
}

function buildDashboardContext() {
  const filters = getDashboardFilters();
  const bounds = getDashboardDateBounds(filters);
  const term = filters.searchNorm;
  const type = filters.type || 'todos';
  const stMap = new Map(normalizeEstaciones(estaciones).map(st => [st.id, st]));

  const matchTerm = (...values) => {
    if (!term) return true;
    return values.some(v => normalizeMatchText(v).includes(term));
  };

  const atEntities = autotanques.filter(at => {
    const plant = normalizePlantName(at.plantaActual || '');
    if (filters.plant && plant !== filters.plant) return false;
    return matchTerm(at.econ, at.placa, at.plantaActual, at.serieUnidad, at.serieTanque, at.notas);
  });

  const stEntities = normalizeEstaciones(estaciones).filter(st => {
    const plant = normalizePlantName(st.planta || '');
    if (filters.plant && plant !== filters.plant) return false;
    return matchTerm(st.planta, st.estacion, st.bomba, (st.componentes || []).join(' '));
  });

  const autoComponentRowsAll = autotanques
    .flatMap(at => getLatestRecordsForUnit(at.id).map(rec => {
      const plant = normalizePlantName(at.plantaActual || '') || 'SIN PLANTA';
      return {
        id: rec.id,
        entityType: 'autotanque',
        entityId: at.id,
        entityName: `${at.econ} | ${at.placa}`,
        plant,
        record: rec,
        createdAt: parseDateSafe(rec.createdAt),
        days: daysUntil(rec.replDate),
        status: statusKey(daysUntil(rec.replDate))
      };
    }))
    .filter(row => {
      if (filters.plant && row.plant !== filters.plant) return false;
      return matchTerm(
        row.entityName,
        row.plant,
        row.record.partNo,
        row.record.partDesc,
        row.record.partPn,
        row.record.serial,
        row.record.brand,
        row.record.notes
      );
    });

  const stationComponentRowsAll = stationRecords
    .map(rec => {
      const st = stMap.get(rec.stationId);
      if (!st) return null;
      const plant = normalizePlantName(st.planta || '') || 'SIN PLANTA';
      return {
        id: rec.id,
        entityType: 'estacion',
        entityId: st.id,
        entityName: `${st.estacion} | ${st.bomba}`,
        plant,
        station: st,
        record: rec,
        createdAt: parseDateSafe(rec.createdAt),
        days: daysUntil(rec.replDate),
        status: statusKey(daysUntil(rec.replDate))
      };
    })
    .filter(Boolean)
    .filter(row => {
      if (filters.plant && row.plant !== filters.plant) return false;
      return matchTerm(
        row.entityName,
        row.plant,
        row.record.partNo,
        row.record.partDesc,
        row.record.partPn,
        row.record.serial,
        row.record.brand,
        row.record.notes
      );
    });

  const byTypeAll = type === 'autotanque'
    ? autoComponentRowsAll
    : type === 'estacion'
      ? stationComponentRowsAll
      : [...autoComponentRowsAll, ...stationComponentRowsAll];

  const byTypePeriod = byTypeAll.filter(row => {
    if (!bounds.start && !bounds.end) return true;
    return inRange(row.createdAt, bounds.start, bounds.end);
  });

  return {
    filters,
    bounds,
    atEntities: type === 'estacion' ? [] : atEntities,
    stEntities: type === 'autotanque' ? [] : stEntities,
    componentRowsAll: byTypeAll,
    componentRowsPeriod: byTypePeriod
  };
}

function dashboardNavigate(metric) {
  const filters = getDashboardFilters();
  if (metric === 'autotanques') {
    switchTab('autotanques');
    const plantEl = document.getElementById('filterATPlant');
    if (plantEl) plantEl.value = filters.plant || '';
    renderAutotanques();
    return;
  }
  if (metric === 'estaciones') {
    switchTab('estaciones');
    const plantEl = document.getElementById('filterEstPlant');
    if (plantEl) plantEl.value = filters.plant || '';
    renderEstaciones();
    return;
  }
  switchTab('reemplazos');
  const statusEl = document.getElementById('filterReplStatus');
  if (statusEl) statusEl.value = metric === 'vencidos' ? 'vencido' : metric === 'criticos' ? 'critico' : '';
  const searchEl = document.getElementById('searchRepl');
  if (searchEl && filters.search) searchEl.value = filters.search;
  renderReemplazos();
}

function openDashboardDrill(title, bodyHtml) {
  const titleEl = document.getElementById('modalDashboardDrillTitle');
  const bodyEl = document.getElementById('modalDashboardDrillBody');
  if (!titleEl || !bodyEl) return;
  titleEl.textContent = title;
  bodyEl.innerHTML = bodyHtml;
  document.getElementById('modalDashboardDrill')?.classList.add('open');
}

function dashboardOpenEntity(entityType, entityId) {
  if (entityType === 'estacion') {
    switchTab('estaciones');
    openEstacionView(entityId);
    return;
  }
  switchTab('autotanques');
  viewAutotanque(entityId);
}

function dashboardOpenStatusDrill(status) {
  const ctx = buildDashboardContext();
  const rows = ctx.componentRowsAll.filter(r => r.status === status);
  const statusTitle = status.toUpperCase().replace('-', ' ');
  const html = rows.length
    ? `<div class="table-wrap"><table><thead><tr><th>TIPO</th><th>UNIDAD/ESTACIÓN</th><th>PIEZA</th><th>DÍAS</th><th>ACCIONES</th></tr></thead><tbody>
      ${rows.slice(0, 120).map(r => `<tr>
        <td>${r.entityType === 'estacion' ? 'ESTACIÓN' : 'AUTOTANQUE'}</td>
        <td>${escapeHtml(r.entityName)}</td>
        <td>${escapeHtml(r.record.partNo || '')} — ${escapeHtml(r.record.partDesc || '')}</td>
        <td>${r.days === null ? '—' : r.days}</td>
        <td><button class="btn btn-secondary" style="padding:4px 8px;font-size:10px" onclick="dashboardOpenEntity('${r.entityType}','${r.entityId}')">VER</button></td>
      </tr>`).join('')}
    </tbody></table></div>`
    : '<p class="text-muted">Sin registros para este estado.</p>';
  openDashboardDrill(`Detalle estado: ${statusTitle}`, html);
}

function dashboardOpenPlantDrill(plant) {
  const ctx = buildDashboardContext();
  const rows = ctx.componentRowsAll.filter(r => r.plant === plant);
  const html = rows.length
    ? `<div class="table-wrap"><table><thead><tr><th>TIPO</th><th>UNIDAD/ESTACIÓN</th><th>PIEZA</th><th>ESTADO</th><th>ACCIÓN</th></tr></thead><tbody>
      ${rows.slice(0, 150).map(r => `<tr>
        <td>${r.entityType === 'estacion' ? 'ESTACIÓN' : 'AUTOTANQUE'}</td>
        <td>${escapeHtml(r.entityName)}</td>
        <td>${escapeHtml(r.record.partNo || '')} — ${escapeHtml(r.record.partDesc || '')}</td>
        <td>${statusBadge(r.days)}</td>
        <td><button class="btn btn-secondary" style="padding:4px 8px;font-size:10px" onclick="dashboardOpenEntity('${r.entityType}','${r.entityId}')">ABRIR</button></td>
      </tr>`).join('')}
    </tbody></table></div>`
    : '<p class="text-muted">Sin registros en esta planta con los filtros actuales.</p>';
  openDashboardDrill(`Planta ${plant}: detalle de componentes`, html);
}

function dashboardOpenMonthDrill(monthKey) {
  const ctx = buildDashboardContext();
  const rows = ctx.componentRowsAll.filter(r => String(r.record.replDate || '').startsWith(`${monthKey}-`));
  const html = rows.length
    ? `<div class="table-wrap"><table><thead><tr><th>TIPO</th><th>UNIDAD/ESTACIÓN</th><th>PIEZA</th><th>F.REEMPLAZO</th><th>ESTADO</th></tr></thead><tbody>
      ${rows.slice(0, 120).map(r => `<tr>
        <td>${r.entityType === 'estacion' ? 'ESTACIÓN' : 'AUTOTANQUE'}</td>
        <td>${escapeHtml(r.entityName)}</td>
        <td>${escapeHtml(r.record.partNo || '')} — ${escapeHtml(r.record.partDesc || '')}</td>
        <td>${formatDate(r.record.replDate)}</td>
        <td>${statusBadge(r.days)}</td>
      </tr>`).join('')}
    </tbody></table></div>`
    : '<p class="text-muted">Sin vencimientos programados en este mes.</p>';
  openDashboardDrill(`Vencimientos programados en ${monthKey}`, html);
}

function resetDashboardFilters() {
  const defaults = {
    dashPlantFilter: '',
    dashTypeFilter: 'todos',
    dashFromDate: '',
    dashToDate: '',
    dashSearch: ''
  };
  Object.entries(defaults).forEach(([id, value]) => {
    const el = document.getElementById(id);
    if (el) el.value = value;
  });
  renderDashboard();
}

function bindDashboardControls() {
  const map = [
    ['dashPlantFilter', 'change'],
    ['dashTypeFilter', 'change'],
    ['dashFromDate', 'change'],
    ['dashToDate', 'change'],
    ['dashSearch', 'input']
  ];
  map.forEach(([id, evt]) => {
    const el = document.getElementById(id);
    if (!el || el.dataset.bound === '1') return;
    el.addEventListener(evt, renderDashboard);
    el.dataset.bound = '1';
  });
  const resetBtn = document.getElementById('dashResetBtn');
  if (resetBtn && resetBtn.dataset.bound !== '1') {
    resetBtn.addEventListener('click', resetDashboardFilters);
    resetBtn.dataset.bound = '1';
  }
}

function renderDashboard() {
  const ctx = buildDashboardContext();
  const period = getDashboardTrendWindow(ctx.filters);
  const rows = ctx.componentRowsPeriod;
  const rowsAll = ctx.componentRowsAll;

  const totalAT = ctx.atEntities.length;
  const totalStations = ctx.stEntities.length;
  const totalRec = rows.length;
  const vencidos = rows.filter(r => r.status === 'vencido').length;
  const criticos = rows.filter(r => r.status === 'critico').length;

  const atCur = ctx.atEntities.filter(at => inRange(inferCreatedAt(at), period.currentStart, period.currentEnd)).length;
  const atPrev = ctx.atEntities.filter(at => inRange(inferCreatedAt(at), period.prevStart, period.prevEnd)).length;
  const stCur = ctx.stEntities.filter(st => inRange(inferCreatedAt(st), period.currentStart, period.currentEnd)).length;
  const stPrev = ctx.stEntities.filter(st => inRange(inferCreatedAt(st), period.prevStart, period.prevEnd)).length;
  const recCur = rowsAll.filter(r => inRange(r.createdAt, period.currentStart, period.currentEnd)).length;
  const recPrev = rowsAll.filter(r => inRange(r.createdAt, period.prevStart, period.prevEnd)).length;
  const vencidosPrev = rowsAll.filter(r => dashboardStatusFromDays(daysUntilFrom(r.record.replDate, period.prevEnd)) === 'vencido').length;
  const criticosPrev = rowsAll.filter(r => dashboardStatusFromDays(daysUntilFrom(r.record.replDate, period.prevEnd)) === 'critico').length;

  const tAt = formatTrend(atCur - atPrev);
  const tSt = formatTrend(stCur - stPrev);
  const tRec = formatTrend(recCur - recPrev);
  const tVen = formatTrend(vencidos - vencidosPrev);
  const tCri = formatTrend(criticos - criticosPrev);

  document.getElementById('statsGrid').innerHTML = `
    <div class="stat-card stat-blue clickable" onclick="dashboardNavigate('autotanques')">
      <div class="stat-value">${totalAT}</div>
      <div class="stat-label">AUTOTANQUES</div>
      <div class="stat-trend ${tAt.cls}">Tendencia periodo: ${tAt.text}</div>
    </div>
    <div class="stat-card stat-blue clickable" onclick="dashboardNavigate('estaciones')">
      <div class="stat-value">${totalStations}</div>
      <div class="stat-label">ESTACIONES DE CARBURACIÓN</div>
      <div class="stat-trend ${tSt.cls}">Tendencia periodo: ${tSt.text}</div>
    </div>
    <div class="stat-card stat-green clickable" onclick="dashboardNavigate('componentes')">
      <div class="stat-value">${totalRec}</div>
      <div class="stat-label">COMPONENTES REGISTRADOS</div>
      <div class="stat-trend ${tRec.cls}">Capturas periodo: ${tRec.text}</div>
    </div>
    <div class="stat-card stat-red clickable" onclick="dashboardNavigate('vencidos')">
      <div class="stat-value">${vencidos}</div>
      <div class="stat-label">VENCIDOS</div>
      <div class="stat-trend ${tVen.cls}">Vs periodo previo: ${tVen.text}</div>
    </div>
    <div class="stat-card stat-amber clickable" onclick="dashboardNavigate('criticos')">
      <div class="stat-value">${criticos}</div>
      <div class="stat-label">CRÍTICOS (≤90 DÍAS)</div>
      <div class="stat-trend ${tCri.cls}">Vs periodo previo: ${tCri.text}</div>
    </div>
  `;

  const alertItems = [];
  if (vencidos > 0) {
    alertItems.push(`<div class="alert-item"><strong>${vencidos} componente(s) vencidos.</strong><div class="alert-actions"><button class="btn btn-danger" style="padding:4px 8px;font-size:10px" onclick="dashboardNavigate('vencidos')">VER VENCIDOS</button></div></div>`);
  }
  if (criticos > 0) {
    alertItems.push(`<div class="alert-item"><strong>${criticos} componente(s) críticos en 90 días.</strong><div class="alert-actions"><button class="btn btn-secondary" style="padding:4px 8px;font-size:10px" onclick="dashboardNavigate('criticos')">REVISAR CRÍTICOS</button></div></div>`);
  }
  const atWithoutComponents = ctx.atEntities.filter(at => !records.some(r => r.atId === at.id)).length;
  if (atWithoutComponents > 0 && ctx.filters.type !== 'estacion') {
    alertItems.push(`<div class="alert-item"><strong>${atWithoutComponents} autotanque(s) sin componentes capturados.</strong><div class="alert-actions"><button class="btn btn-secondary" style="padding:4px 8px;font-size:10px" onclick="switchTab('registro')">CAPTURAR AHORA</button></div></div>`);
  }
  const stWithoutComponents = ctx.stEntities.filter(st => !stationRecords.some(r => r.stationId === st.id)).length;
  if (stWithoutComponents > 0 && ctx.filters.type !== 'autotanque') {
    alertItems.push(`<div class="alert-item"><strong>${stWithoutComponents} estación(es) sin componentes capturados.</strong><div class="alert-actions"><button class="btn btn-secondary" style="padding:4px 8px;font-size:10px" onclick="switchTab('registro')">CAPTURAR EN ESTACIÓN</button></div></div>`);
  }
  document.getElementById('dashboardAlerts').innerHTML = alertItems.length
    ? `<div class="alert-list">${alertItems.join('')}</div>`
    : '<p class="text-muted">No hay alertas de riesgo con los filtros actuales.</p>';

  const smartBox = document.getElementById('dashboardSmartResults');
  if (smartBox) {
    if (!ctx.filters.searchNorm) {
      smartBox.innerHTML = '<p class="text-muted">Escribe un término para ver resultados de unidades, estaciones y componentes.</p>';
    } else {
      const smartMatches = [];
      ctx.atEntities.forEach(at => smartMatches.push({ kind: 'AUTOTANQUE', title: `${at.econ} | ${at.placa}`, subtitle: at.plantaActual || 'SIN PLANTA', action: `dashboardOpenEntity('autotanque','${at.id}')` }));
      ctx.stEntities.forEach(st => smartMatches.push({ kind: 'ESTACIÓN', title: `${st.estacion} | ${st.bomba}`, subtitle: st.planta, action: `dashboardOpenEntity('estacion','${st.id}')` }));
      rowsAll.slice(0, 80).forEach(r => smartMatches.push({
        kind: 'COMPONENTE',
        title: `${r.record.partNo || ''} — ${r.record.partDesc || ''}`,
        subtitle: `${r.entityName} | ${r.plant}`,
        action: `dashboardOpenEntity('${r.entityType}','${r.entityId}')`
      }));
      const dedup = [];
      const seen = new Set();
      smartMatches.forEach(m => {
        const key = `${m.kind}|${m.title}|${m.subtitle}`;
        if (seen.has(key)) return;
        seen.add(key);
        dedup.push(m);
      });
      smartBox.innerHTML = dedup.length
        ? `<div class="smart-results">${dedup.slice(0, 10).map(m => `
          <div class="smart-item">
            <div><b>${escapeHtml(m.kind)}</b> — ${escapeHtml(m.title)}</div>
            <div class="text-muted">${escapeHtml(m.subtitle || '')}</div>
            <div class="alert-actions"><button class="btn btn-secondary" style="padding:4px 8px;font-size:10px" onclick="${m.action}">ABRIR</button></div>
          </div>`).join('')}</div>`
        : '<p class="text-muted">Sin coincidencias.</p>';
    }
  }

  const plantCounts = new Map();
  rows.forEach(r => plantCounts.set(r.plant, (plantCounts.get(r.plant) || 0) + 1));
  const plantRows = Array.from(plantCounts.entries()).sort((a, b) => b[1] - a[1]);
  const maxPlant = plantRows.length ? plantRows[0][1] : 1;
  document.getElementById('dashboardPlantBars').innerHTML = plantRows.length
    ? `<div class="mini-bars">${plantRows.map(([plant, count]) => `
      <div class="mini-bar-row">
        <button class="mini-bar-label" onclick="dashboardOpenPlantDrill(decodeURIComponent('${encodeURIComponent(plant)}'))">${escapeHtml(plant)}</button>
        <div class="mini-bar-track"><div class="mini-bar-fill" style="width:${(count / maxPlant) * 100}%"></div></div>
        <span class="mini-bar-value">${count}</span>
      </div>`).join('')}</div>`
    : '<p class="text-muted">Sin datos por planta para el filtro actual.</p>';

  const statusPalette = {
    vencido: '#dc2626',
    critico: '#d97706',
    proximo: '#f59e0b',
    vigente: '#16a34a',
    'sin-fecha': '#64748b'
  };
  const statusCounts = {
    vencido: rows.filter(r => r.status === 'vencido').length,
    critico: rows.filter(r => r.status === 'critico').length,
    proximo: rows.filter(r => r.status === 'proximo').length,
    vigente: rows.filter(r => r.status === 'vigente').length,
    'sin-fecha': rows.filter(r => r.status === 'sin-fecha').length
  };
  const totalStatus = Object.values(statusCounts).reduce((acc, n) => acc + n, 0);
  if (!totalStatus) {
    document.getElementById('dashboardStatusDonut').innerHTML = '<p class="text-muted">Sin registros para construir estado global.</p>';
  } else {
    const slices = [];
    let cursor = 0;
    Object.entries(statusCounts).forEach(([k, v]) => {
      if (!v) return;
      const pct = (v / totalStatus) * 100;
      slices.push(`${statusPalette[k]} ${cursor}% ${cursor + pct}%`);
      cursor += pct;
    });
    const labelMap = { vencido: 'Vencido', critico: 'Crítico', proximo: 'Próximo', vigente: 'Vigente', 'sin-fecha': 'Sin fecha' };
    document.getElementById('dashboardStatusDonut').innerHTML = `
      <div class="donut-wrap">
        <div class="donut-chart" style="background:conic-gradient(${slices.join(', ')})">
          <div class="donut-center"><b>${totalStatus}</b><span class="kpi-subnote">TOTAL</span></div>
        </div>
        <div class="donut-legend">
          ${Object.entries(statusCounts).map(([k, v]) => `
            <div class="donut-item">
              <button onclick="dashboardOpenStatusDrill('${k}')"><span><span class="dot" style="background:${statusPalette[k]}"></span>${labelMap[k]}</span></button>
              <b>${v}</b>
            </div>`).join('')}
        </div>
      </div>
    `;
  }

  const monthMap = new Map();
  const baseMonth = new Date();
  for (let i = 0; i < 12; i++) {
    const d = new Date(baseMonth.getFullYear(), baseMonth.getMonth() + i, 1);
    const key = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
    monthMap.set(key, 0);
  }
  rowsAll.forEach(r => {
    if (!r.record.replDate) return;
    const monthKey = String(r.record.replDate).slice(0, 7);
    if (monthMap.has(monthKey)) monthMap.set(monthKey, monthMap.get(monthKey) + 1);
  });
  const monthEntries = Array.from(monthMap.entries());
  const maxMonth = Math.max(1, ...monthEntries.map(([, n]) => n));
  const w = Math.max(560, monthEntries.length * 56);
  const h = 220;
  const padX = 34;
  const padY = 30;
  const stepX = (w - (padX * 2)) / Math.max(1, monthEntries.length - 1);
  const points = monthEntries.map(([, n], idx) => {
    const x = padX + (idx * stepX);
    const y = (h - padY) - ((n / maxMonth) * (h - (padY * 2)));
    return { x, y, n };
  });
  const polyline = points.map(p => `${p.x},${p.y}`).join(' ');
  document.getElementById('dashboardMonthLine').innerHTML = `
    <div class="line-svg-wrap">
      <svg class="line-svg" width="${w}" height="${h}" viewBox="0 0 ${w} ${h}">
        <line x1="${padX}" y1="${h - padY}" x2="${w - padX}" y2="${h - padY}" stroke="#94a3b8" stroke-width="1"></line>
        <polyline fill="none" stroke="#0369a1" stroke-width="2.5" points="${polyline}"></polyline>
        ${points.map((p, i) => `<circle class="line-point" cx="${p.x}" cy="${p.y}" r="4" fill="#0ea5e9" onclick="dashboardOpenMonthDrill('${monthEntries[i][0]}')"><title>${monthEntries[i][0]}: ${p.n}</title></circle>`).join('')}
        ${monthEntries.map(([k], i) => `<text x="${points[i].x}" y="${h - 10}" text-anchor="middle">${k.slice(5)}</text>`).join('')}
      </svg>
    </div>
    <div class="kpi-subnote">Haz clic en un punto para abrir el detalle del mes.</div>
  `;

  const riskMap = new Map();
  rowsAll.forEach(r => {
    const key = `${r.entityType}|${r.entityId}`;
    if (!riskMap.has(key)) {
      riskMap.set(key, {
        entityType: r.entityType,
        entityId: r.entityId,
        name: r.entityName,
        plant: r.plant,
        total: 0,
        vencidos: 0,
        criticos: 0,
        proximos: 0
      });
    }
    const acc = riskMap.get(key);
    acc.total += 1;
    if (r.status === 'vencido') acc.vencidos += 1;
    if (r.status === 'critico') acc.criticos += 1;
    if (r.status === 'proximo') acc.proximos += 1;
  });
  const riskRows = Array.from(riskMap.values()).map(r => ({
    ...r,
    score: (r.vencidos * 3) + (r.criticos * 2) + r.proximos
  })).sort((a, b) => b.score - a.score || b.vencidos - a.vencidos || b.criticos - a.criticos);

  document.getElementById('dashboardRiskTable').innerHTML = riskRows.length
    ? `<table><thead><tr><th>TIPO</th><th>UNIDAD/ESTACIÓN</th><th>RIESGO</th><th>DETALLE</th><th>ACCIÓN</th></tr></thead><tbody>
      ${riskRows.slice(0, 10).map(r => `<tr>
        <td>${r.entityType === 'estacion' ? 'ESTACIÓN' : 'AUTOTANQUE'}</td>
        <td>${escapeHtml(r.name)}</td>
        <td><span class="risk-score">Score ${r.score}</span></td>
        <td>${r.vencidos} venc. | ${r.criticos} crít. | ${r.proximos} próx.</td>
        <td><button class="btn btn-secondary" style="padding:4px 8px;font-size:10px" onclick="dashboardOpenEntity('${r.entityType}','${r.entityId}')">ABRIR</button></td>
      </tr>`).join('')}
    </tbody></table>`
    : '<p class="text-muted">Sin datos de riesgo para el filtro actual.</p>';

  const critRecs = rows
    .filter(r => r.days !== null && r.days <= 90)
    .sort((a, b) => a.days - b.days)
    .slice(0, 10);

  document.getElementById('criticalList').innerHTML = critRecs.length
    ? `<table><thead><tr><th>TIPO</th><th>UNIDAD/ESTACIÓN</th><th>PIEZA</th><th>DÍAS</th><th>ESTADO</th></tr></thead><tbody>
      ${critRecs.map(r => `<tr>
        <td>${r.entityType === 'estacion' ? 'ESTACIÓN' : 'AUTOTANQUE'}</td>
        <td><b style="color:var(--accent)">${escapeHtml(r.entityName)}</b></td>
        <td>${escapeHtml(r.record.partNo || '')} — ${escapeHtml(String(r.record.partDesc || '').slice(0, 34))}</td>
        <td style="font-family:monospace">${r.days < 0 ? 'VENCIDO' : `${r.days} días`}</td>
        <td>${statusBadge(r.days)}</td>
      </tr>`).join('')}
    </tbody></table>`
    : '<p style="color:var(--ok);font-size:13px;padding:10px 0">Sin vencimientos críticos próximos.</p>';

  const recent = [...rowsAll]
    .sort((a, b) => new Date(b.record.createdAt || 0) - new Date(a.record.createdAt || 0))
    .slice(0, 8);
  document.getElementById('recentActivity').innerHTML = recent.length
    ? recent.map(r => {
        const d = parseDateSafe(r.record.createdAt);
        return `<div style="padding:8px 0;border-bottom:1px solid var(--border)">
          <div style="font-size:12px"><b style="color:var(--accent)">${r.entityType === 'estacion' ? 'EST.' : 'AT.'}</b> ${escapeHtml(r.entityName)} — Pieza ${escapeHtml(r.record.partNo || '')}: ${escapeHtml(String(r.record.partDesc || '').slice(0, 40))}</div>
          <div class="text-muted">${d ? d.toLocaleString('es-MX') : 'Sin fecha'}</div>
        </div>`;
      }).join('')
    : '<p class="text-muted">Sin registros aún.</p>';

  const semMap = new Map();
  rowsAll.forEach(r => {
    if (!semMap.has(r.plant)) semMap.set(r.plant, { total: 0, alertas: 0, vencidos: 0, criticos: 0 });
    const item = semMap.get(r.plant);
    item.total += 1;
    if (r.status === 'vencido') { item.alertas += 1; item.vencidos += 1; }
    if (r.status === 'critico') { item.alertas += 1; item.criticos += 1; }
  });
  const semRows = Array.from(semMap.entries()).map(([plant, s]) => {
    const pct = s.total ? Math.round((s.alertas / s.total) * 100) : 0;
    const level = pct >= 25 ? 'danger' : pct >= 10 ? 'warn' : 'ok';
    return { plant, ...s, pct, level };
  }).sort((a, b) => b.pct - a.pct);
  document.getElementById('dashboardPlantSemaforo').innerHTML = semRows.length
    ? `<div class="semaforo-grid">${semRows.map(s => `
      <div class="semaforo-card ${s.level}" onclick="dashboardOpenPlantDrill(decodeURIComponent('${encodeURIComponent(s.plant)}'))">
        <div class="title">${escapeHtml(s.plant)}</div>
        <div class="meta">${s.alertas}/${s.total} en alerta (${s.pct}%)</div>
        <div class="meta">${s.vencidos} vencidos | ${s.criticos} críticos</div>
      </div>`).join('')}</div>`
    : '<p class="text-muted">Sin datos para semáforo por planta.</p>';
}

// ── MODALS ────────────────────────────────────────────────────────────
async function closeModal(id) {
  if (id === 'modalAT') {
    const transientPaths = draftExpedienteDocs
      .map(d => d?.storagePath)
      .filter(path => path && !originalExpedientePaths.has(path));
    if (transientPaths.length) await deleteExpedienteFilesRemote(transientPaths);
    draftExpedienteDocs = [];
    pendingExpedienteDeletePaths = [];
    originalExpedientePaths = new Set();
    currentDraftATId = null;
    editingATId = null;
  }
  if (id === 'modalRecordEdit') {
    editingRecordId = null;
  }
  if (id === 'modalEstacionEdit') {
    editingEstacionId = null;
  }
  if (id === 'modalMaintenance') {
    maintenanceEditingAtId = null;
    maintenanceEditingEntryId = null;
  }
  document.getElementById(id).classList.remove('open');
}
window.addEventListener('click', e => {
  if (!e.target.classList.contains('modal-overlay')) return;
  if (e.target.id) {
    closeModal(e.target.id);
  } else {
    e.target.classList.remove('open');
  }
});
window.addEventListener('resize', () => {
  syncAutotanquesMatrixScroll();
  syncReemplazosMatrixScroll();
});

// ── EXPORT / IMPORT ───────────────────────────────────────────────────
function exportCSV() {
  const rows = [
    ['NO_ECONOMICO','PLACA','PLANTA_ACTUAL','SERIE_UNIDAD','SERIE_TANQUE','CAPACIDAD_L','PIEZA_NO','NP','DESCRIPCION','MARCA','SERIE_COMP','F_FABRICACION','F_INSTALACION','F_REEMPLAZO','DIAS_RESTANTES','ESTADO','NOTAS']
  ];
  records.forEach(r => {
    const at = autotanques.find(a => a.id === r.atId);
    const d = daysUntil(r.replDate);
    rows.push([
      at?.econ||'', at?.placa||'', at?.plantaActual||'', at?.serieUnidad||'', at?.serieTanque||'', at?.capacidad||'',
      r.partNo, r.partPn, r.partDesc, r.brand, r.serial,
      r.fabDate, r.instDate, r.replDate,
      d === null ? '' : d,
      statusKey(d).toUpperCase(),
      r.notes
    ]);
  });
  downloadCSV('inventario_NOM007.csv', rows);
}

function exportVencidosCSV() {
  const rows = [
    ['NO_ECONOMICO','PLACA','PIEZA_NO','DESCRIPCION','F_FABRICACION','F_REEMPLAZO','DIAS_RESTANTES','ESTADO']
  ];
  autotanques.forEach(at => {
    getLatestRecordsForUnit(at.id).forEach(r => {
      const d = daysUntil(r.replDate);
      if (d !== null && d <= 90) {
        rows.push([at?.econ||'',at?.placa||'',r.partNo,r.partDesc,r.fabDate,r.replDate,d,statusKey(d).toUpperCase()]);
      }
    });
  });
  downloadCSV('vencimientos_criticos_NOM007.csv', rows);
}

function exportReplMatrixExcel() {
  const table = document.getElementById('tableReplMatrix');
  if (!table) {
    alert('No se encontró la matriz para exportar.');
    return;
  }

  const allRows = Array.from(table.querySelectorAll('tr'));
  const visibleRows = allRows.filter(row => row.querySelectorAll('th,td').length && row.offsetParent !== null);
  if (!visibleRows.length) {
    alert('No hay datos visibles en la matriz para exportar.');
    return;
  }

  const escapeXls = (value) => String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');

  const rowsHtml = visibleRows.map((row) => {
    const cells = Array.from(row.querySelectorAll('th,td')).map((cell) => {
      const tag = cell.tagName.toLowerCase() === 'th' ? 'th' : 'td';
      const value = (cell.innerText || cell.textContent || '').replace(/\s+/g, ' ').trim();
      const computed = window.getComputedStyle(cell);
      const bgColor = computed.backgroundColor;
      const textColor = computed.color;
      const fontWeight = computed.fontWeight;
      const textAlign = computed.textAlign;

      const inlineStyles = [
        bgColor && bgColor !== 'rgba(0, 0, 0, 0)' ? `background:${bgColor}` : '',
        textColor ? `color:${textColor}` : '',
        fontWeight ? `font-weight:${fontWeight}` : '',
        textAlign ? `text-align:${textAlign}` : '',
        'white-space:nowrap'
      ].filter(Boolean).join(';');

      return `<${tag} style="${inlineStyles}">${escapeXls(value)}</${tag}>`;
    }).join('');
    return `<tr>${cells}</tr>`;
  }).join('');

  const now = new Date();
  const yyyy = now.getFullYear();
  const mm = String(now.getMonth() + 1).padStart(2, '0');
  const dd = String(now.getDate()).padStart(2, '0');
  const hh = String(now.getHours()).padStart(2, '0');
  const mi = String(now.getMinutes()).padStart(2, '0');
  const fileName = `matriz_vencimientos_componentes_${yyyy}${mm}${dd}_${hh}${mi}.xls`;

  const workbookHtml = `<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <style>
    table { border-collapse: collapse; font-family: Arial, sans-serif; font-size: 11px; }
    th, td { border: 1px solid #d1d5db; padding: 4px 6px; text-align: center; white-space: nowrap; }
  </style>
</head>
<body>
  <table>${rowsHtml}</table>
</body>
</html>`;

  const blob = new Blob(['\uFEFF', workbookHtml], {
    type: 'application/vnd.ms-excel;charset=utf-8;'
  });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = fileName;
  a.click();
  URL.revokeObjectURL(a.href);
}

function exportFormatoValvulasPorPlanta() {
  const selectedPlant = normalizePlantName(document.getElementById('normPlantSelect')?.value || '');
  if (!selectedPlant) return alert('Selecciona una planta para imprimir el formato.');

  const unitsByPlant = sortUnitsByMode(
    autotanques.filter(a => normalizePlantName(a.plantaActual || '') === selectedPlant),
    'econ-asc'
  );
  if (!unitsByPlant.length) return alert(`No hay autotanques registrados para ${selectedPlant}.`);

  // Solo unidades sin componentes capturados
  const units = unitsByPlant.filter(at => !records.some(r => r.atId === at.id));
  if (!units.length) return alert(`No hay autotanques sin componentes capturados en ${selectedPlant}.`);

  const headers = [
    'NO. ECONOMICO',
    'SELLOS PROF',
    'CINCHOS',
    'V. CHECK LOCK 3/4"',
    'V. LLENADO 3"',
    'V. NO RETROCESO 3"',
    'V. CARBURACION 3/4"',
    'V. RETORNO VAPORES 1/4',
    'V. NO RETROCESO 1 1/4"',
    'V. MAXIMO LLENADO (1)',
    'V. MAXIMO LLENADO (2)',
    'V. MAXIMO LLENADO (3)',
    'V. INTERNA DE 2"',
    'V. SEGURIDAD 3"',
    'MANGUERA DE SUMISTRO MODELO 20BHB'
  ];
  const rows = [new Array(headers.length).fill(''), headers];
  units.forEach(at => {
    const row = new Array(headers.length).fill('');
    row[0] = at.econ || '';
    rows.push(row);
  });

  const printWindow = window.open('', '_blank');
  if (!printWindow) {
    alert('El navegador bloqueó la ventana de impresión. Permite ventanas emergentes e intenta de nuevo.');
    return;
  }

  const rowsHtml = rows.map((row, idx) => {
    const tds = row.map(cell => `<td>${escapeHtml(cell || '')}</td>`).join('');
    const trClass = idx === 1 ? 'header-row' : '';
    return `<tr class="${trClass}">${tds}</tr>`;
  }).join('');
  const colWidths = [11, 5, 5, 6, 6, 7, 7, 8, 8, 6, 6, 6, 6, 6, 7];
  const colgroupHtml = `<colgroup>${colWidths.map(w => `<col style="width:${w}%">`).join('')}</colgroup>`;

  const html = `<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <title>Formato Fechas de Válvulas - ${escapeHtml(selectedPlant)}</title>
  <style>
    @page { size: landscape; margin: 10mm; }
    body { font-family: Arial, sans-serif; margin: 0; color: #111; }
    .meta { margin-bottom: 8px; font-size: 11px; }
    .meta b { margin-right: 4px; }
    table { width: 100%; border-collapse: collapse; table-layout: fixed; font-size: 9.5px; }
    td {
      border: 1px solid #000;
      padding: 4px 3px;
      vertical-align: middle;
      text-align: center;
      white-space: normal;
      overflow-wrap: anywhere;
      line-height: 1.15;
    }
    .header-row td { font-weight: 700; background: #f2f2f2; font-size: 8.5px; }
    td:first-child { text-align: left; font-weight: 600; }
  </style>
</head>
<body>
  <div class="meta"><b>PLANTA:</b>${escapeHtml(selectedPlant)} | <b>TOTAL AUTOTANQUES:</b> ${units.length}</div>
  <table>${colgroupHtml}<tbody>${rowsHtml}</tbody></table>
  <script>
    window.onload = function () {
      window.focus();
      window.print();
    };
  </script>
</body>
</html>`;

  printWindow.document.open();
  printWindow.document.write(html);
  printWindow.document.close();
}

function downloadCSV(filename, rows) {
  const csv = rows.map(r => r.map(c => `"${String(c||'').replace(/"/g,'""')}"`).join(',')).join('\n');
  const a = document.createElement('a');
  a.href = URL.createObjectURL(new Blob(['\uFEFF'+csv], {type:'text/csv;charset=utf-8'}));
  a.download = filename;
  a.click();
}

function exportChangeHistoryCSV() {
  if (!changeHistory.length) {
    alert('No hay historial de cambios para exportar.');
    return;
  }
  const rows = [['FECHA','MOVIMIENTO','USUARIO','ACCION']];
  changeHistory.forEach(item => {
    rows.push([
      formatDateTime(item.timestamp),
      item.movement || '',
      item.user || '',
      item.action || ''
    ]);
  });
  downloadCSV('historial_cambios_inventario.csv', rows);
}

function exportJSON() {
  const data = { autotanques, records, stationRecords, partImages, maintenanceSchedule, changeHistory, exportedAt: new Date().toISOString() };
  const a = document.createElement('a');
  a.href = URL.createObjectURL(new Blob([JSON.stringify(data,null,2)], {type:'application/json'}));
  a.download = 'respaldo_inventario_NOM007.json';
  a.click();
}

function normalizeCsvHeader(v) {
  return String(v || '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]/g, '');
}

function normalizePlantName(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  const normalized = normalizeCsvHeader(raw);
  const match = PLANTAS_ACTUALES.find(p => normalizeCsvHeader(p) === normalized);
  return match || '';
}

function detectCsvDelimiter(headerLine) {
  const commas = (headerLine.match(/,/g) || []).length;
  const semicolons = (headerLine.match(/;/g) || []).length;
  return semicolons > commas ? ';' : ',';
}

function parseCSVLine(line, delimiter) {
  const out = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (ch === delimiter && !inQuotes) {
      out.push(current.trim());
      current = '';
      continue;
    }
    current += ch;
  }
  out.push(current.trim());
  return out;
}

function findCsvIndex(headers, aliases) {
  for (const alias of aliases) {
    const idx = headers.indexOf(normalizeCsvHeader(alias));
    if (idx >= 0) return idx;
  }
  return -1;
}

function importUnitsCSV() {
  document.getElementById('importUnitsFile').click();
}

async function handleImportUnitsCSV(e) {
  const file = e.target.files[0];
  if (!file) return;

  const reader = new FileReader();
  reader.onload = async ev => {
    try {
      const raw = String(ev.target.result || '').replace(/\ufeff/g, '').replace(/\r\n/g, '\n').replace(/\r/g, '\n');
      const lines = raw.split('\n').filter(l => l.trim().length > 0);
      if (lines.length < 2) return alert('CSV inválido: debe incluir encabezados y al menos una fila.');

      const delimiter = detectCsvDelimiter(lines[0]);
      const headers = parseCSVLine(lines[0], delimiter).map(normalizeCsvHeader);
      const rows = lines.slice(1).map(line => parseCSVLine(line, delimiter));

      const idxEcon = findCsvIndex(headers, ['economico', 'noeconomico', 'numeroeconomico', 'numeconomico', 'econ']);
      const idxPlaca = findCsvIndex(headers, ['placa', 'placas']);
      const idxSerieUnidad = findCsvIndex(headers, ['serieunidad', 'noserieunidad', 'seriechasis', 'chasis', 'noserieautotanque', 'serieautotanque']);
      const idxSerieTanque = findCsvIndex(headers, ['serietanque', 'noserietanque']);
      const idxCapacidad = findCsvIndex(headers, ['capacidad', 'capacidadlitros', 'litros']);
      const idxAnio = findCsvIndex(headers, ['anio', 'ano', 'aniomodelo', 'modeloyear', 'modelo']);
      const idxMarca = findCsvIndex(headers, ['marca', 'marcavehiculo']);
      const idxModelo = findCsvIndex(headers, ['modelo', 'modeloanio']);
      const idxNumero = findCsvIndex(headers, ['n', 'no', 'numero', 'nro']);
      const idxTipo = findCsvIndex(headers, ['tipo', 'tipounidad']);
      const idxPlantaActual = findCsvIndex(headers, ['plantaactual', 'planta', 'plantaventa', 'sucursal']);
      const idxNotas = findCsvIndex(headers, ['notas', 'observaciones', 'comentarios']);

      if (idxEcon < 0 || idxPlaca < 0) {
        return alert('CSV inválido: requiere columnas de económico y placa.');
      }

      const nextUnits = normalizeAutotanques(autotanques.map(a => ({ ...a })));
      const indexByEcon = new Map(nextUnits.map((a, i) => [String(a.econ || '').trim().toLowerCase(), i]));
      const changedUnits = [];
      let inserted = 0;
      let updated = 0;
      let skipped = 0;

      for (const row of rows) {
        const econ = String(row[idxEcon] || '').trim();
        const placa = String(row[idxPlaca] || '').trim();
        if (!econ && !placa) {
          skipped++;
          continue;
        }
        if (!econ || !placa) {
          skipped++;
          continue;
        }

        const serieUnidad = idxSerieUnidad >= 0 ? String(row[idxSerieUnidad] || '').trim() : '';
        const serieTanque = idxSerieTanque >= 0 ? String(row[idxSerieTanque] || '').trim() : '';
        const capacidad = idxCapacidad >= 0 ? String(row[idxCapacidad] || '').trim() : '';
        const marca = idxMarca >= 0 ? String(row[idxMarca] || '').trim() : '';
        const modelo = idxModelo >= 0 ? String(row[idxModelo] || '').trim() : '';
        const numero = idxNumero >= 0 ? String(row[idxNumero] || '').trim() : '';
        const tipo = idxTipo >= 0 ? String(row[idxTipo] || '').trim() : '';
        let anio = idxAnio >= 0 ? String(row[idxAnio] || '').trim() : '';
        if (!anio && /^\d{4}$/.test(modelo)) anio = modelo;
        const plantaActual = normalizePlantName(idxPlantaActual >= 0 ? String(row[idxPlantaActual] || '').trim() : '');
        const notas = idxNotas >= 0 ? String(row[idxNotas] || '').trim() : '';
        const csvMeta = [
          marca ? `Marca: ${marca}` : '',
          modelo && !/^\d{4}$/.test(modelo) ? `Modelo: ${modelo}` : '',
          numero ? `N°: ${numero}` : '',
          tipo ? `Tipo: ${tipo}` : ''
        ].filter(Boolean).join(' | ');
        const notasFinal = [notas, csvMeta].filter(Boolean).join(' | ');

        const key = econ.toLowerCase();
        if (indexByEcon.has(key)) {
          const i = indexByEcon.get(key);
          const current = nextUnits[i];
        const merged = {
          ...current,
          econ,
          placa,
          plantaActual: plantaActual || current.plantaActual || '',
            serieUnidad: serieUnidad || current.serieUnidad || '',
            serieTanque: serieTanque || current.serieTanque || '',
            capacidad: capacidad || current.capacidad || '',
            anio: anio || current.anio || '',
            marcaUnidad: marca || current.marcaUnidad || '',
            modeloUnidad: modelo || current.modeloUnidad || '',
            notas: notasFinal || current.notas || '',
            expediente: normalizeExpediente(current.expediente)
          };
          nextUnits[i] = merged;
          changedUnits.push(merged);
          updated++;
        } else {
        const created = {
          id: genId(),
          econ,
          placa,
          plantaActual,
          serieUnidad,
          serieTanque,
            capacidad,
            anio,
            marcaUnidad: marca,
            modeloUnidad: modelo,
            activo: true,
            enServicio: true,
            notas: notasFinal,
          expediente: []
        };
          nextUnits.push(created);
          indexByEcon.set(key, nextUnits.length - 1);
          changedUnits.push(created);
          inserted++;
        }
      }

      if (!inserted && !updated) {
        return alert('No se detectaron filas válidas para importar.');
      }

      if (runtimeUseSupabase && !(await upsertUnitsRemote(changedUnits))) return;

      autotanques = normalizeAutotanques(nextUnits);
      if (!save()) return;
      populatePlantSelectors();
      renderAutotanques();
      populateATSelect();
      renderDashboard();
      alert(`✅ Importación CSV completada. Nuevos: ${inserted}, actualizados: ${updated}, omitidos: ${skipped}.`);
    } catch {
      alert('❌ No se pudo procesar el CSV. Revisa formato y encabezados.');
    }
  };
  reader.readAsText(file);
  e.target.value = '';
}

function importJSON() { document.getElementById('importFile').click(); }
function handleImport(e) {
  const file = e.target.files[0];
  if (!file) return;
  const reader = new FileReader();
  reader.onload = async ev => {
    try {
      const data = JSON.parse(ev.target.result);
      const hasPartImages = Object.prototype.hasOwnProperty.call(data || {}, 'partImages');
      const hasStationRecords = Object.prototype.hasOwnProperty.call(data || {}, 'stationRecords');
      const hasMaintenanceSchedule = Object.prototype.hasOwnProperty.call(data || {}, 'maintenanceSchedule');
      const hasChangeHistory = Object.prototype.hasOwnProperty.call(data || {}, 'changeHistory');
      const incomingPartImagesCount = hasPartImages
        ? Object.keys(data.partImages || {}).length
        : Object.keys(partImages || {}).length;
      const incomingStationRecordsCount = hasStationRecords
        ? (Array.isArray(data.stationRecords) ? data.stationRecords.length : 0)
        : stationRecords.length;
      const partImagesText = hasPartImages
        ? `${incomingPartImagesCount} imágenes de piezas`
        : `conservar imágenes actuales (${incomingPartImagesCount})`;
      const stationRecordsText = hasStationRecords
        ? `${incomingStationRecordsCount} registros de estaciones`
        : `conservar registros de estaciones actuales (${incomingStationRecordsCount})`;
      if (!confirm(`¿Importar ${data.autotanques?.length||0} autotanques, ${data.records?.length||0} registros, ${stationRecordsText} y ${partImagesText}? Esto REEMPLAZARÁ los datos actuales.`)) return;
      autotanques = normalizeAutotanques(data.autotanques || []);
      records     = data.records || [];
      stationRecords = hasStationRecords ? normalizeStationRecords(data.stationRecords || []) : normalizeStationRecords(stationRecords);
      maintenanceSchedule = hasMaintenanceSchedule
        ? (Array.isArray(data.maintenanceSchedule) ? data.maintenanceSchedule.map(normalizeMaintenanceEntry).filter(item => item.atId && item.maintDate) : [])
        : maintenanceSchedule;
      const policySync = applyReplacementPolicyToRecords(records);
      records = policySync.records;
      partImages  = hasPartImages ? normalizePartImages(data.partImages || {}) : normalizePartImages(partImages);
      if (hasChangeHistory) {
        changeHistory = Array.isArray(data.changeHistory)
          ? data.changeHistory.map(item => ({
              id: String(item?.id || genId()),
              timestamp: String(item?.timestamp || ''),
              action: String(item?.action || ''),
              movement: String(item?.movement || ''),
              user: String(item?.user || 'Sistema')
            })).filter(item => item.timestamp && item.movement).slice(0, CHANGE_HISTORY_LIMIT)
          : [];
        persistChangeHistory();
      }
      const replaceImagesPayload = hasPartImages ? partImages : null;
      const replaceStationRecordsPayload = hasStationRecords ? stationRecords : null;
      const replaceChangeHistoryPayload = hasChangeHistory ? changeHistory : null;
      if (runtimeUseSupabase && !(await replaceRemoteData(autotanques, records, replaceImagesPayload, replaceStationRecordsPayload, replaceChangeHistoryPayload))) return;
      if (!save()) return;
      persistMaintenanceSchedule();
      populatePlantSelectors();
      renderDashboard();
      renderAutotanques();
      renderEstaciones();
      populateATSelect();
      refreshSelectedPartImageUI();
      renderChangeHistory();
      alert('✅ Importación exitosa.');
    } catch { alert('❌ Archivo inválido.'); }
  };
  reader.readAsText(file);
  e.target.value = '';
}
