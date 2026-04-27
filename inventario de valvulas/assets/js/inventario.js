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
  { no:"12", pn:"CAT-12", qty:1, desc:"Manguera Modelo 20BHB",                                 brand:"" },
];

// ── STATE ─────────────────────────────────────────────────────────────
let autotanques = JSON.parse(localStorage.getItem('at_units') || '[]');
let records     = JSON.parse(localStorage.getItem('at_records') || '[]');
let partImages  = JSON.parse(localStorage.getItem('at_part_images') || '{}');
let selectedPart = null;
let editingATId  = null;
let currentDraftATId = null;
let editingRecordId = null;
let expandedReplGroups = new Set();
let draftExpedienteDocs = [];
let pendingExpedienteDeletePaths = [];
let originalExpedientePaths = new Set();

const EXPEDIENTE_CATEGORIES = {
  seguro: 'Seguro de la unidad',
  permiso: 'Permisos',
  pago: 'Pagos',
  verificacion: 'Verificacion / inspeccion',
  otro: 'Otro'
};
const COMPONENT_GUIDE_IMAGE = 'assets/img/diagrama_componentes_autotanque.png';
const PLANTAS_ACTUALES = ['QUERETARO', 'BALVANERA', 'GALERAS', 'PEDRO ESCOBEDO'];
const MAX_DOC_SIZE_LOCAL_BYTES = 1.5 * 1024 * 1024;
const MAX_DOC_SIZE_SUPABASE_BYTES = 10 * 1024 * 1024;
const MAX_PART_IMAGE_SIZE_BYTES = 3 * 1024 * 1024;
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
const SUPABASE_BUCKET_EXPEDIENTE = String(APP_SUPABASE_CONFIG.bucketExpediente || 'at_expediente').trim() || 'at_expediente';
const SUPABASE_ENABLED = Boolean(SUPABASE_URL && SUPABASE_ANON && window.supabase?.createClient);
const SUPABASE_CONFIG_SOURCE = HAS_FILE_SUPABASE_CONFIG
  ? 'archivo (assets/config/supabase.config.js)'
  : (SUPABASE_ENABLED ? 'localStorage' : 'sin configurar');
const supabaseClient = SUPABASE_ENABLED ? window.supabase.createClient(SUPABASE_URL, SUPABASE_ANON) : null;
let runtimeUseSupabase = SUPABASE_ENABLED;
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

alter table public.at_units add column if not exists planta_actual text;

alter table public.at_units enable row level security;
alter table public.at_records enable row level security;
alter table public.at_part_images enable row level security;

drop policy if exists "at_units_all" on public.at_units;
create policy "at_units_all" on public.at_units for all using (true) with check (true);

drop policy if exists "at_records_all" on public.at_records;
create policy "at_records_all" on public.at_records for all using (true) with check (true);

drop policy if exists "at_part_images_all" on public.at_part_images;
create policy "at_part_images_all" on public.at_part_images for all using (true) with check (true);

insert into storage.buckets (id, name, public)
values ('${SUPABASE_BUCKET_EXPEDIENTE}', '${SUPABASE_BUCKET_EXPEDIENTE}', false)
on conflict (id) do nothing;

drop policy if exists "at_expediente_rw" on storage.objects;
create policy "at_expediente_rw"
on storage.objects
for all
using (bucket_id = '${SUPABASE_BUCKET_EXPEDIENTE}')
with check (bucket_id = '${SUPABASE_BUCKET_EXPEDIENTE}');
`.trim();

autotanques = normalizeAutotanques(autotanques);
partImages = normalizePartImages(partImages);

function save() {
  if (runtimeUseSupabase) {
    // Siempre mantenemos cache local de imagenes por seguridad, aun en modo Supabase.
    try { localStorage.setItem('at_part_images', JSON.stringify(partImages)); } catch {}
    return true;
  }
  try {
    localStorage.setItem('at_units', JSON.stringify(autotanques));
    localStorage.setItem('at_records', JSON.stringify(records));
    localStorage.setItem('at_part_images', JSON.stringify(partImages));
    return true;
  } catch (err) {
    alert('No se pudo guardar en almacenamiento local. Reduce tamano de archivos del expediente o imagenes e intenta de nuevo.');
    return false;
  }
}

// ── UTILS ─────────────────────────────────────────────────────────────
function genId() { return Date.now().toString(36) + Math.random().toString(36).slice(2,6); }

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

  replInputEl.value = addYears(parsed.iso, 5);
  if (hintEl) {
    if (parsed.type === 'date') {
      hintEl.innerHTML = `Fecha capturada: <b>${parsed.iso}</b>.`;
    } else {
      hintEl.innerHTML = `Codigo <b>${parsed.code}</b>: mes ${parsed.month} (${parsed.monthName}), semana ${parsed.weekLetter} (${parsed.week}ra), año ${parsed.year}. Fecha base: <b>${parsed.iso}</b>.`;
    }
  }
  return parsed;
}

function addYears(dateStr, years) {
  if (!dateStr) return '';
  const d = new Date(dateStr);
  d.setFullYear(d.getFullYear() + years);
  return d.toISOString().split('T')[0];
}

function daysUntil(dateStr) {
  if (!dateStr) return null;
  const now = new Date(); now.setHours(0,0,0,0);
  const d = new Date(dateStr);
  return Math.round((d - now) / 86400000);
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

function normalizeAutotanques(list) {
  return (Array.isArray(list) ? list : []).map(at => ({
    ...at,
    plantaActual: normalizePlantName(at.plantaActual || at.planta_actual || ''),
    expediente: normalizeExpediente(at.expediente)
  }));
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
  const key = String(partNo || '').trim();
  if (!key) return null;
  const img = partImages[key];
  if (!img || typeof img !== 'object' || !img.dataUrl) return null;
  return img;
}

function getPartImageSrc(partNo) {
  const img = getPartImage(partNo);
  return img?.dataUrl || COMPONENT_GUIDE_IMAGE;
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
  const img = getPartImage(partNo);
  if (!img) {
    statusEl.textContent = 'Esta pieza usa la imagen guía general.';
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
    notas: at.notas || null,
    expediente: normalizeExpediente(at.expediente)
  };
}

function mapUnitFromDb(row) {
  return {
    id: row.id,
    econ: row.econ || '',
    placa: row.placa || '',
    plantaActual: normalizePlantName(row.planta_actual || ''),
    serieUnidad: row.serie_unidad || '',
    serieTanque: row.serie_tanque || '',
    capacidad: row.capacidad || '',
    anio: row.anio || '',
    notas: row.notas || '',
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

async function replaceRemoteData(allUnits, allRecords, allPartImages = null) {
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

  return true;
}

// ── TABS ──────────────────────────────────────────────────────────────
function switchTab(id) {
  document.querySelectorAll('.tab-btn').forEach((b,i) => {
    const tabs = ['dashboard','autotanques','registro','reemplazos','exportar'];
    b.classList.toggle('active', tabs[i] === id);
  });
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  document.getElementById('tab-'+id).classList.add('active');
  if (id === 'dashboard')   renderDashboard();
  if (id === 'autotanques') renderAutotanques();
  if (id === 'registro')    renderPartList(); populateATSelect(); refreshSelectedPartImageUI();
  if (id === 'reemplazos')  renderReemplazos();
}

// ── PART LIST ─────────────────────────────────────────────────────────
function renderPartList() {
  const q = (document.getElementById('searchPart')?.value || '').toLowerCase();
  const list = document.getElementById('partList');
  list.innerHTML = PARTS.filter(p =>
    p.desc.toLowerCase().includes(q) || p.no.includes(q) || p.pn.toLowerCase().includes(q)
  ).map(p => `
    <div class="part-item ${selectedPart?.no===p.no?'selected':''}" onclick="selectPart('${p.no}')">
      <span class="part-no">${p.no}</span>
      <div>
        <div class="part-desc">${p.desc}</div>
        <div class="part-pn">${p.pn}</div>
      </div>
    </div>
  `).join('');
}

function filterParts() { renderPartList(); }

function selectPart(no) {
  selectedPart = PARTS.find(p => p.no === no);
  renderPartList();

  // Show preview card
  const card = document.getElementById('partPreviewCard');
  card.style.display = 'block';

  // Diagram image — we embed the uploaded diagram as the base
  const img = document.getElementById('diagramImg');
  img.src = getPartImageSrc(no);
  img.alt = `Diagrama - Pieza ${no}`;

  // Show detail info
  document.getElementById('partDetailInfo').innerHTML = `
    <div class="detail-row"><span class="detail-key">PIEZA No.:</span><span class="detail-val">${selectedPart.no}</span></div>
    <div class="detail-row"><span class="detail-key">NÚMERO DE PARTE:</span><span class="detail-val">${selectedPart.pn}</span></div>
    <div class="detail-row"><span class="detail-key">DESCRIPCIÓN:</span><span class="detail-val">${selectedPart.desc}</span></div>
    <div class="detail-row"><span class="detail-key">MARCA:</span><span class="detail-val">${selectedPart.brand||'—'}</span></div>
    <div class="detail-row"><span class="detail-key">CANTIDAD POR UNIDAD:</span><span class="detail-val">${selectedPart.qty}</span></div>
  `;

  // Selected part info in form
  document.getElementById('selectedPartInfo').style.display = 'block';
  document.getElementById('selectedPartText').innerHTML = `
    <b style="color:var(--accent)">Pieza ${selectedPart.no}</b> — ${selectedPart.desc}<br>
    <span class="part-pn">${selectedPart.pn}</span>
  `;
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

// ── SAVE COMPONENT RECORD ─────────────────────────────────────────────
async function saveComponentRecord() {
  if (!selectedPart) return alert('Selecciona un componente de la lista.');
  const atId = document.getElementById('formAT').value;
  if (!atId) return alert('Selecciona un autotanque.');
  const notesText = document.getElementById('formNotes').value || '';
  const fabRawCode = document.getElementById('formFabDate').value || '';
  const parsedFab = parseFabCode(fabRawCode);
  if (!parsedFab && !notesText.trim()) {
    return alert('Ingresa un código/fecha válida o agrega una nota cuando el autotanque no cuenta con la válvula.');
  }
  const fabDate = parsedFab ? parsedFab.iso : '';
  const replDateAuto = fabDate ? addYears(fabDate, 5) : '';
  const notesFinal = ensureNoValveTagInNotes(notesText, Boolean(fabDate));

  const rec = {
    id:       genId(),
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
    notes:     notesFinal,
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
  ['formAT','formFabDate','formInstDate','formReplDate','formSerial','formBrand','formNotes']
    .forEach(id => document.getElementById(id).value = '');
  const hintEl = document.getElementById('formFabHint');
  if (hintEl) hintEl.innerHTML = 'Formatos válidos: <b>6A92</b>, <b>9C22</b>, <b>09C22</b> o <b>27/04/2026</b>.';
  document.getElementById('partImageFile').value = '';
  selectedPart = null;
  document.getElementById('selectedPartInfo').style.display = 'none';
  document.getElementById('partPreviewCard').style.display = 'none';
  refreshSelectedPartImageUI();
  renderPartList();
}

function renderDraftExpedienteList() {
  const box = document.getElementById('expedienteDraftList');
  if (!box) return;
  if (!draftExpedienteDocs.length) {
    box.innerHTML = '<p class="text-muted" style="padding:10px 12px">Sin documentos en el expediente.</p>';
    return;
  }

  box.innerHTML = draftExpedienteDocs.map(doc => `
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
    const payload = {
      partNo: selectedPart.no,
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
    alert(`✅ Imagen guardada para la pieza ${payload.partNo}.`);
  } catch {
    alert('No se pudo leer o guardar la imagen seleccionada.');
  }
}

async function removeSelectedPartImage() {
  if (!selectedPart) return alert('Selecciona una pieza.');
  const partNo = selectedPart.no;
  if (!partImages[partNo]) return alert('La pieza seleccionada no tiene imagen personalizada.');
  if (!confirm(`¿Quitar imagen personalizada de la pieza ${partNo}?`)) return;

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
  const docs = Array.isArray(expedienteDocs) ? expedienteDocs : [];
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

// Auto-calc replacement date
document.addEventListener('DOMContentLoaded', async () => {
  document.getElementById('formFabDate')?.addEventListener('input', updateReplacementFromFabInput);
  document.getElementById('editRecFabDate')?.addEventListener('change', e => {
    const replEl = document.getElementById('editRecReplDate');
    if (!replEl) return;
    if (!replEl.value && e.target.value) replEl.value = addYears(e.target.value, 5);
  });
  syncConfigInputs();
  updateStorageModeLabel();
  if (runtimeUseSupabase) {
    const loaded = await loadFromSupabase();
    if (!loaded) updateStorageModeLabel('error de carga, revisa SQL/politicas');
  }
  refreshSelectedPartImageUI();
  renderPartList();
  populateATSelect();
  renderDashboard();
  renderAutotanques();
  renderDraftExpedienteList();
});

// ── AUTOTANQUES CRUD ─────────────────────────────────────────────────
function openModalAutotanque(id) {
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
    document.getElementById('atEcon').value        = at.econ;
    document.getElementById('atPlaca').value       = at.placa;
    document.getElementById('atPlantaActual').value= at.plantaActual || '';
    document.getElementById('atSerieUnidad').value = at.serieUnidad;
    document.getElementById('atSerieTanque').value = at.serieTanque;
    document.getElementById('atCapacidad').value   = at.capacidad;
    document.getElementById('atAnio').value        = at.anio;
    document.getElementById('atNotas').value       = at.notas;
    draftExpedienteDocs = (at.expediente || []).map(d => ({ ...d }));
    originalExpedientePaths = new Set(draftExpedienteDocs.map(d => d.storagePath).filter(Boolean));
  } else {
    ['atEcon','atPlaca','atPlantaActual','atSerieUnidad','atSerieTanque','atCapacidad','atAnio','atNotas']
      .forEach(id => document.getElementById(id).value = '');
    draftExpedienteDocs = [];
  }
  renderDraftExpedienteList();
  document.getElementById('modalAT').classList.add('open');
}

async function saveAutotanque() {
  const econ = document.getElementById('atEcon').value.trim();
  const placa = document.getElementById('atPlaca').value.trim();
  const plantaActual = normalizePlantName(document.getElementById('atPlantaActual').value);
  if (!econ || !placa) return alert('No. Económico y Placas son obligatorios.');
  if (!plantaActual) return alert('Selecciona la planta actual.');
  const unitId = editingATId || currentDraftATId || genId();

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
  renderAutotanques();
  populateATSelect();
}

async function deleteAutotanque(id) {
  if (!confirm('¿Eliminar este autotanque y todos sus registros?')) return;
  if (!(await deleteAutotanqueRemote(id))) return;
  autotanques = autotanques.filter(a => a.id !== id);
  records     = records.filter(r => r.atId !== id);
  save();
  renderAutotanques();
}

function renderAutotanques() {
  const q = (document.getElementById('searchAT')?.value || '').toLowerCase();
  const orderMode = document.getElementById('sortAT')?.value || 'econ-asc';
  const tbody = document.getElementById('tableAT');
  if (!tbody) return;

  const filtered = autotanques.filter(a =>
    a.econ.toLowerCase().includes(q) ||
    a.placa.toLowerCase().includes(q) ||
    (a.plantaActual||'').toLowerCase().includes(q) ||
    (a.serieUnidad||'').toLowerCase().includes(q)
  );
  const sorted = sortUnitsByMode(filtered, orderMode);

  if (!sorted.length) {
    tbody.innerHTML = '<tr><td colspan="9" style="text-align:center;color:var(--muted);padding:30px">Sin autotanques registrados. Haz clic en "+ NUEVO AUTOTANQUE".</td></tr>';
    return;
  }

  tbody.innerHTML = sorted.map(at => {
    const atRecs = records.filter(r => r.atId === at.id);
    const withDate = atRecs.filter(r => r.replDate);
    const vencidos = withDate.filter(r => daysUntil(r.replDate) < 0).length;
    const criticos = withDate.filter(r => { const d=daysUntil(r.replDate); return d>=0&&d<=90; }).length;

    let estado = '<span class="badge badge-ok">OK</span>';
    if (vencidos > 0) estado = `<span class="badge badge-danger">${vencidos} VENCIDO(S)</span>`;
    else if (criticos > 0) estado = `<span class="badge badge-warn">${criticos} CRÍTICO(S)</span>`;
    else if (!atRecs.length) estado = '<span class="badge badge-none">SIN REGISTROS</span>';

    const pct = atRecs.length ? Math.round(atRecs.length/PARTS.length*100) : 0;
    const pctClass = pct < 50 ? 'danger' : pct < 80 ? 'warn' : '';

    return `<tr>
      <td><b style="color:var(--accent)">${at.econ}</b></td>
      <td>${at.placa}</td>
      <td style="font-family:monospace;font-size:11px">${at.serieUnidad||'—'}</td>
      <td style="font-family:monospace;font-size:11px">${at.serieTanque||'—'}</td>
      <td>${at.capacidad?at.capacidad+' L':'—'}</td>
      <td>${at.plantaActual || '—'}</td>
      <td class="progress-cell">
        <div class="prog-bar"><div class="prog-fill ${pctClass}" style="width:${pct}%"></div></div>
        <span style="font-size:11px">${atRecs.length}/${PARTS.length}</span>
      </td>
      <td>${estado}</td>
      <td>
        <div class="flex-gap">
          <button class="btn btn-secondary" style="padding:5px 10px;font-size:11px" onclick="viewAutotanque('${at.id}')">VER</button>
          <button class="btn btn-secondary" style="padding:5px 10px;font-size:11px" onclick="openModalAutotanque('${at.id}')">✏️</button>
          <button class="btn btn-danger" style="padding:5px 10px;font-size:11px" onclick="deleteAutotanque('${at.id}')">🗑</button>
        </div>
      </td>
    </tr>`;
  }).join('');
}

function viewAutotanque(id) {
  const at = autotanques.find(a => a.id === id);
  const atRecs = records.filter(r => r.atId === id);
  const expedienteDocs = at.expediente || [];
  document.getElementById('modalDetailTitle').textContent = `${at.econ} | ${at.placa}`;
  document.getElementById('modalDetailBody').innerHTML = `
    <div class="grid-2" style="margin-bottom:16px">
      <div class="detail-row"><span class="detail-key">ECONÓMICO:</span><span class="detail-val">${at.econ}</span></div>
      <div class="detail-row"><span class="detail-key">PLACA:</span><span class="detail-val">${at.placa}</span></div>
      <div class="detail-row"><span class="detail-key">SERIE UNIDAD:</span><span class="detail-val">${at.serieUnidad||'—'}</span></div>
      <div class="detail-row"><span class="detail-key">SERIE TANQUE:</span><span class="detail-val">${at.serieTanque||'—'}</span></div>
      <div class="detail-row"><span class="detail-key">CAPACIDAD:</span><span class="detail-val">${at.capacidad?at.capacidad+' L':'—'}</span></div>
      <div class="detail-row"><span class="detail-key">PLANTA ACTUAL:</span><span class="detail-val">${at.plantaActual || '—'}</span></div>
    </div>
    <div class="section-sep"></div>
    <div class="card-title">COMPONENTES REGISTRADOS (${atRecs.length})</div>
    <div class="table-wrap" style="max-height:300px;overflow-y:auto">
      <table>
        <thead><tr><th>PIEZA</th><th>DESCRIPCIÓN</th><th>F.FAB</th><th>F.REEMPLAZO</th><th>ESTADO</th></tr></thead>
        <tbody>
          ${atRecs.length ? atRecs.map(r => `
            <tr>
              <td style="font-family:monospace;color:var(--accent)">${r.partNo}</td>
              <td>${r.partDesc}</td>
              <td>${formatDate(r.fabDate)}</td>
              <td>${formatDate(r.replDate)}</td>
              <td>${statusBadge(daysUntil(r.replDate))}</td>
            </tr>`).join('') :
            '<tr><td colspan="5" style="text-align:center;color:var(--muted);padding:20px">Sin componentes registrados</td></tr>'
          }
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
  if (!tbody) return;
  const filterAt     = document.getElementById('filterReplAT')?.value || '';
  const filterStatus = document.getElementById('filterReplStatus')?.value || '';
  const searchTerm   = (document.getElementById('searchRepl')?.value || '').trim().toLowerCase();

  let rows = records.map(r => ({
    ...r,
    at: autotanques.find(a => a.id === r.atId),
    days: daysUntil(r.replDate)
  })).filter(r => r.at);

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
            <td>
              <div class="flex-gap">
                <button class="btn btn-secondary" style="padding:4px 8px;font-size:10px" onclick="openRecordEditor('${r.id}')">✏️</button>
                <button class="btn btn-danger" style="padding:4px 8px;font-size:10px" onclick="deleteRecord('${r.id}')">🗑</button>
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
  const replDate = replInput || addYears(fabDate, 5);

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
function renderDashboard() {
  const totalAT  = autotanques.length;
  const totalRec = records.length;
  const vencidos = records.filter(r => { const d=daysUntil(r.replDate); return d!==null && d<0; }).length;
  const criticos = records.filter(r => { const d=daysUntil(r.replDate); return d!==null && d>=0 && d<=90; }).length;

  document.getElementById('statsGrid').innerHTML = `
    <div class="stat-card stat-blue"><div class="stat-value">${totalAT}</div><div class="stat-label">AUTOTANQUES</div></div>
    <div class="stat-card stat-green"><div class="stat-value">${totalRec}</div><div class="stat-label">COMPONENTES REGISTRADOS</div></div>
    <div class="stat-card stat-red"><div class="stat-value">${vencidos}</div><div class="stat-label">VENCIDOS</div></div>
    <div class="stat-card stat-amber"><div class="stat-value">${criticos}</div><div class="stat-label">CRÍTICOS (≤90 DÍAS)</div></div>
  `;

  // Critical list
  const critRecs = records
    .map(r => ({ ...r, at: autotanques.find(a => a.id === r.atId), days: daysUntil(r.replDate) }))
    .filter(r => r.at && r.days !== null && r.days <= 90)
    .sort((a,b) => a.days - b.days)
    .slice(0, 10);

  const critDiv = document.getElementById('criticalList');
  if (!critRecs.length) {
    critDiv.innerHTML = '<p style="color:var(--ok);font-size:13px;padding:10px 0">✅ Sin vencimientos críticos próximos.</p>';
  } else {
    critDiv.innerHTML = `<table><thead><tr><th>UNIDAD</th><th>PIEZA</th><th>DÍAS</th><th>ESTADO</th></tr></thead><tbody>
      ${critRecs.map(r => `<tr>
        <td><b style="color:var(--accent)">${r.at.econ}</b></td>
        <td>${r.partNo} — ${r.partDesc.slice(0,30)}...</td>
        <td style="font-family:monospace">${r.days < 0 ? 'VENCIDO' : r.days+' días'}</td>
        <td>${statusBadge(r.days)}</td>
      </tr>`).join('')}
    </tbody></table>`;
  }

  // Recent activity
  const recent = [...records]
    .sort((a,b) => new Date(b.createdAt) - new Date(a.createdAt))
    .slice(0, 5);
  const actDiv = document.getElementById('recentActivity');
  if (!recent.length) {
    actDiv.innerHTML = '<p class="text-muted">Sin registros aún.</p>';
  } else {
    actDiv.innerHTML = recent.map(r => {
      const at = autotanques.find(a => a.id === r.atId);
      const d = new Date(r.createdAt);
      return `<div style="padding:8px 0;border-bottom:1px solid var(--border)">
        <div style="font-size:12px"><b style="color:var(--accent)">${at?.econ||'?'}</b> — Pieza ${r.partNo}: ${r.partDesc.slice(0,40)}</div>
        <div class="text-muted">${d.toLocaleString('es-MX')}</div>
      </div>`;
    }).join('');
  }
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
  records.forEach(r => {
    const at = autotanques.find(a => a.id === r.atId);
    const d = daysUntil(r.replDate);
    if (d !== null && d <= 90) {
      rows.push([at?.econ||'',at?.placa||'',r.partNo,r.partDesc,r.fabDate,r.replDate,d,statusKey(d).toUpperCase()]);
    }
  });
  downloadCSV('vencimientos_criticos_NOM007.csv', rows);
}

function downloadCSV(filename, rows) {
  const csv = rows.map(r => r.map(c => `"${String(c||'').replace(/"/g,'""')}"`).join(',')).join('\n');
  const a = document.createElement('a');
  a.href = URL.createObjectURL(new Blob(['\uFEFF'+csv], {type:'text/csv;charset=utf-8'}));
  a.download = filename;
  a.click();
}

function exportJSON() {
  const data = { autotanques, records, partImages, exportedAt: new Date().toISOString() };
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
      const incomingPartImagesCount = hasPartImages
        ? Object.keys(data.partImages || {}).length
        : Object.keys(partImages || {}).length;
      const partImagesText = hasPartImages
        ? `${incomingPartImagesCount} imágenes de piezas`
        : `conservar imágenes actuales (${incomingPartImagesCount})`;
      if (!confirm(`¿Importar ${data.autotanques?.length||0} autotanques, ${data.records?.length||0} registros y ${partImagesText}? Esto REEMPLAZARÁ los datos actuales.`)) return;
      autotanques = normalizeAutotanques(data.autotanques || []);
      records     = data.records || [];
      partImages  = hasPartImages ? normalizePartImages(data.partImages || {}) : normalizePartImages(partImages);
      const replaceImagesPayload = hasPartImages ? partImages : null;
      if (runtimeUseSupabase && !(await replaceRemoteData(autotanques, records, replaceImagesPayload))) return;
      if (!save()) return;
      renderDashboard();
      renderAutotanques();
      populateATSelect();
      refreshSelectedPartImageUI();
      alert('✅ Importación exitosa.');
    } catch { alert('❌ Archivo inválido.'); }
  };
  reader.readAsText(file);
  e.target.value = '';
}
