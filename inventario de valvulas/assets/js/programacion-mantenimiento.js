(function programacionMantenimientoModule() {
  'use strict';

  const PROGRAMACION_STORAGE_KEY = 'at_programacion_mantenimiento';
  const PROGRAMACION_TABLE = String(window.SUPABASE_CONFIG?.tableMaintenanceProgramacion || 'mantenimiento_programado').trim() || 'mantenimiento_programado';
  const UNIDAD_INICIO_DEFAULT = 'QI-193';
  const ESTADOS = [
    'DISPONIBLE',
    'PROGRAMADO',
    'EN MANTENIMIENTO',
    'ESPERANDO REFACCIONES',
    'TERMINADO',
    'LIBERADO',
    'FUERA DE SERVICIO'
  ];

  let programacionMantenimiento = loadProgramacionLocal();
  let programacionRemotaCargada = false;
  let programacionRemotaDisponible = true;
  let moduloInicializado = false;

  function escapeSafe(value) {
    if (typeof escapeHtml === 'function') return escapeHtml(value);
    return String(value ?? '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function nowIso() {
    return new Date().toISOString();
  }

  function toDateOnly(isoLike) {
    const raw = String(isoLike || '').trim();
    if (!raw) return '';
    if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;
    const d = new Date(raw);
    if (Number.isNaN(d.getTime())) return '';
    const year = d.getFullYear();
    const month = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
  }

  function addDaysToIso(baseIsoDate, days) {
    const date = new Date(`${baseIsoDate}T00:00:00`);
    date.setDate(date.getDate() + days);
    return toDateOnly(date.toISOString());
  }

  function startOfWeekMonday(date) {
    const d = new Date(date);
    d.setHours(0, 0, 0, 0);
    const day = d.getDay();
    const diff = day === 0 ? -6 : (1 - day);
    d.setDate(d.getDate() + diff);
    return d;
  }

  function nextMondayIso(refDate = new Date()) {
    const base = new Date(refDate);
    base.setHours(0, 0, 0, 0);
    const monday = startOfWeekMonday(base);
    if (base > monday) monday.setDate(monday.getDate() + 7);
    return toDateOnly(monday.toISOString());
  }

  function getIsoWeekLabel(isoStart, isoEnd) {
    const start = new Date(`${isoStart}T00:00:00`);
    const end = new Date(`${isoEnd}T00:00:00`);
    if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) return 'Sin semana';
    const weekRef = new Date(start);
    weekRef.setDate(weekRef.getDate() + 3 - ((weekRef.getDay() + 6) % 7));
    const week1 = new Date(weekRef.getFullYear(), 0, 4);
    const isoWeek = 1 + Math.round(((weekRef.getTime() - week1.getTime()) / 86400000 - 3 + ((week1.getDay() + 6) % 7)) / 7);
    const year = weekRef.getFullYear();
    const startLabel = typeof formatDate === 'function' ? formatDate(isoStart) : isoStart;
    const endLabel = typeof formatDate === 'function' ? formatDate(isoEnd) : isoEnd;
    return `SEM ${String(isoWeek).padStart(2, '0')}-${year} (${startLabel} → ${endLabel})`;
  }

  function clamp(value, min, max) {
    return Math.max(min, Math.min(max, value));
  }

  function getCurrentUserLabel() {
    try {
      return String(typeof activeAuditUser !== 'undefined' ? activeAuditUser : 'Sistema').trim() || 'Sistema';
    } catch {
      return 'Sistema';
    }
  }

  function normalizeEstado(estado, riesgoOperativo = 0) {
    const current = String(estado || '').trim().toUpperCase();
    if (current === 'FINALIZADO') return 'TERMINADO';
    if (ESTADOS.includes(current)) return current;
    return riesgoOperativo >= 81 ? 'FUERA DE SERVICIO' : 'DISPONIBLE';
  }

  function normalizeProgramacionEntry(entry) {
    const riesgo = clamp(Number(entry?.riesgo_operativo || 0), 0, 100);
    const now = nowIso();
    return {
      id: String(entry?.id || (typeof genId === 'function' ? genId() : `${Date.now()}`)),
      unidad_id: String(entry?.unidad_id || '').trim(),
      economico: String(entry?.economico || '').trim(),
      planta: String(entry?.planta || '').trim(),
      fecha_inicio: toDateOnly(entry?.fecha_inicio || ''),
      fecha_fin: toDateOnly(entry?.fecha_fin || ''),
      prioridad: Number(entry?.prioridad || 0),
      estado: normalizeEstado(entry?.estado, riesgo),
      tecnico: String(entry?.tecnico || '').trim(),
      observaciones: String(entry?.observaciones || '').trim(),
      riesgo_operativo: riesgo,
      componentes_vencidos: String(entry?.componentes_vencidos || '').trim(),
      created_at: String(entry?.created_at || now),
      bitacora: Array.isArray(entry?.bitacora) ? entry.bitacora.map(item => ({
        fecha: String(item?.fecha || now),
        accion: String(item?.accion || '').trim(),
        usuario: String(item?.usuario || 'Sistema').trim() || 'Sistema'
      })).filter(item => item.accion) : []
    };
  }

  function loadProgramacionLocal() {
    const raw = localStorage.getItem(PROGRAMACION_STORAGE_KEY);
    if (!raw) return [];
    try {
      const parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) return [];
      return parsed.map(normalizeProgramacionEntry).filter(item => item.unidad_id);
    } catch {
      return [];
    }
  }

  function persistProgramacionLocal() {
    try {
      localStorage.setItem(PROGRAMACION_STORAGE_KEY, JSON.stringify(programacionMantenimiento));
    } catch {}
  }

  function hasRemoteProgramacionSupport() {
    return Boolean(
      programacionRemotaDisponible &&
      typeof runtimeUseSupabase !== 'undefined' &&
      runtimeUseSupabase &&
      typeof supabaseClient !== 'undefined' &&
      supabaseClient
    );
  }

  function isMissingProgramacionTableError(error) {
    const code = String(error?.code || '').trim();
    const msg = String(error?.message || '').toLowerCase();
    return code === '42P01' || msg.includes('does not exist') || msg.includes('relation') && msg.includes('mantenimiento_programado');
  }

  function mapProgramacionToDb(entry) {
    return {
      id: entry.id,
      unidad_id: entry.unidad_id,
      economico: entry.economico,
      planta: entry.planta,
      fecha_inicio: entry.fecha_inicio || null,
      fecha_fin: entry.fecha_fin || null,
      prioridad: entry.prioridad,
      estado: entry.estado,
      tecnico: entry.tecnico || null,
      observaciones: entry.observaciones || null,
      riesgo_operativo: entry.riesgo_operativo,
      componentes_vencidos: entry.componentes_vencidos || null,
      created_at: entry.created_at
    };
  }

  async function upsertProgramacionRemota(entries) {
    if (!hasRemoteProgramacionSupport()) return true;
    const rows = (Array.isArray(entries) ? entries : []).map(mapProgramacionToDb);
    if (!rows.length) return true;
    const { error } = await supabaseClient
      .from(PROGRAMACION_TABLE)
      .upsert(rows, { onConflict: 'id' });

    if (error) {
      if (isMissingProgramacionTableError(error)) {
        programacionRemotaDisponible = false;
        console.warn(`[PROGRAMACIÓN] Falta tabla ${PROGRAMACION_TABLE}. Se usará solo almacenamiento local.`);
        return true;
      }
      console.warn('[PROGRAMACIÓN] Error al sincronizar programación:', error);
      return false;
    }
    return true;
  }

  async function cargarProgramacionRemota() {
    if (programacionRemotaCargada || !hasRemoteProgramacionSupport()) return;
    const { data, error } = await supabaseClient
      .from(PROGRAMACION_TABLE)
      .select('id, unidad_id, economico, planta, fecha_inicio, fecha_fin, prioridad, estado, tecnico, observaciones, riesgo_operativo, componentes_vencidos, created_at')
      .order('created_at', { ascending: false });

    if (error) {
      if (isMissingProgramacionTableError(error)) {
        programacionRemotaDisponible = false;
        console.warn(`[PROGRAMACIÓN] Tabla ${PROGRAMACION_TABLE} no disponible. Operando en local.`);
      } else {
        console.warn('[PROGRAMACIÓN] Error al cargar desde Supabase:', error);
      }
      programacionRemotaCargada = true;
      return;
    }

    programacionMantenimiento = (Array.isArray(data) ? data : []).map(item =>
      normalizeProgramacionEntry({
        ...item,
        bitacora: []
      })
    );
    persistProgramacionLocal();
    programacionRemotaCargada = true;
  }

  function getDiasHasta(fecha) {
    if (typeof daysUntil === 'function') return daysUntil(fecha);
    if (!fecha) return null;
    const target = new Date(`${fecha}T00:00:00`);
    if (Number.isNaN(target.getTime())) return null;
    const now = new Date();
    now.setHours(0, 0, 0, 0);
    return Math.floor((target - now) / 86400000);
  }

  function toFloat2(value) {
    return Math.round((Number(value) + Number.EPSILON) * 100) / 100;
  }

  function describeRisk(riesgo) {
    const score = Number(riesgo || 0);
    if (score <= 30) return { label: 'OPERABLE', cls: 'prog-risk-operable' };
    if (score <= 60) return { label: 'PROGRAMAR', cls: 'prog-risk-programar' };
    if (score <= 80) return { label: 'URGENTE', cls: 'prog-risk-urgente' };
    return { label: 'FUERA DE SERVICIO', cls: 'prog-risk-fuera' };
  }

  function estadoCssClass(estado) {
    const slug = String(estado || '').toLowerCase().replace(/\s+/g, '-');
    return `prog-status prog-status-${slug}`;
  }

  function pushBitacora(entry, accion) {
    const item = {
      fecha: nowIso(),
      accion: String(accion || '').trim(),
      usuario: getCurrentUserLabel()
    };
    if (!item.accion) return entry;
    return {
      ...entry,
      bitacora: [...(Array.isArray(entry.bitacora) ? entry.bitacora : []), item]
    };
  }

  function calcularPrioridadPorUnidad(unidad) {
    const unitRecords = (Array.isArray(records) ? records : []).filter(rec => rec.atId === unidad.id);
    let vencidos = 0;
    let criticos = 0;
    let diasVencido = 0;
    const componentesVencidos = [];

    unitRecords.forEach(rec => {
      const dias = getDiasHasta(rec.replDate);
      const desc = `${String(rec.partNo || '').trim()} ${String(rec.partDesc || '').trim()}`.trim();
      if (dias === null) return;
      if (dias < 0) {
        vencidos += 1;
        diasVencido = Math.max(diasVencido, Math.abs(dias));
        if (desc) componentesVencidos.push(desc);
        return;
      }
      if (dias <= 90) criticos += 1;
    });

    const prioridad = toFloat2((vencidos * 5) + (criticos * 10) + (diasVencido / 30));
    const riesgoOperativo = clamp(Math.round(prioridad * 4.5), 0, 100);

    return {
      unidad_id: unidad.id,
      economico: String(unidad.econ || '').trim() || '—',
      planta: String(unidad.plantaActual || '').trim() || 'SIN PLANTA',
      prioridad,
      riesgo_operativo: riesgoOperativo,
      componentes_vencidos_count: vencidos,
      componentes_criticos_count: criticos,
      dias_vencido: diasVencido,
      componentes_vencidos: [...new Set(componentesVencidos)].join(', ')
    };
  }

  function ordenarPrioridades(listado) {
    return [...listado].sort((a, b) =>
      (b.prioridad - a.prioridad) ||
      (b.componentes_vencidos_count - a.componentes_vencidos_count) ||
      (b.riesgo_operativo - a.riesgo_operativo) ||
      (typeof compareTextNatural === 'function'
        ? compareTextNatural(a.economico, b.economico)
        : String(a.economico).localeCompare(String(b.economico)))
    );
  }

  function mergeProgramacionConPrioridades(prioridades) {
    const byUnidad = new Map(programacionMantenimiento.map(item => [item.unidad_id, item]));
    const merged = prioridades.map(priority => {
      const current = byUnidad.get(priority.unidad_id);
      const baseEstado = current?.estado || normalizeEstado('', priority.riesgo_operativo);
      const next = normalizeProgramacionEntry({
        ...current,
        ...priority,
        estado: normalizeEstado(baseEstado, priority.riesgo_operativo),
        tecnico: current?.tecnico || '',
        observaciones: current?.observaciones || '',
        created_at: current?.created_at || nowIso(),
        bitacora: current?.bitacora || []
      });
      if (!current) {
        return pushBitacora(next, 'Registro inicial de programación creado automáticamente.');
      }
      return next;
    });

    const orphan = programacionMantenimiento.filter(item => !prioridades.some(p => p.unidad_id === item.unidad_id));
    programacionMantenimiento = [...merged, ...orphan];
    persistProgramacionLocal();
  }

  function ordenarColaDesdeUnidadInicio(prioridades, economicoInicio) {
    const queue = [...prioridades];
    const needle = String(economicoInicio || '').trim().toUpperCase();
    const idx = queue.findIndex(item => String(item.economico || '').trim().toUpperCase() === needle);
    if (idx <= 0) return queue;
    const starter = queue[idx];
    const rest = queue.filter((_, i) => i !== idx);
    return [starter, ...rest];
  }

  async function calcularPrioridadMantenimiento(options = {}) {
    const unidades = Array.isArray(autotanques) ? autotanques : [];
    const prioridades = ordenarPrioridades(unidades.map(calcularPrioridadPorUnidad));
    mergeProgramacionConPrioridades(prioridades);
    if (!options.skipSync) {
      await upsertProgramacionRemota(programacionMantenimiento);
    }
    if (!options.silent) renderProgramacionMantenimiento();
    return prioridades;
  }

  function getEntryById(entryId) {
    return programacionMantenimiento.find(item => item.id === entryId);
  }

  function getDefaultTecnico() {
    return String(document.getElementById('progTecnicoDefault')?.value || '').trim();
  }

  async function generarProgramacionSemanal() {
    const prioridades = await calcularPrioridadMantenimiento({ silent: true, skipSync: true });
    if (!prioridades.length) {
      renderProgramacionMantenimiento();
      return;
    }

    const unidadInicio = String(document.getElementById('progUnidadInicio')?.value || UNIDAD_INICIO_DEFAULT).trim();
    const queue = ordenarColaDesdeUnidadInicio(prioridades, unidadInicio)
      .filter(item => item.prioridad > 0 || item.riesgo_operativo > 0);

    if (!queue.length) {
      renderProgramacionMantenimiento();
      return;
    }

    const tecnicoDefault = getDefaultTecnico();
    const baseMonday = nextMondayIso(new Date());
    const updates = [];

    queue.forEach((item, index) => {
      const existing = programacionMantenimiento.find(entry => entry.unidad_id === item.unidad_id);
      if (!existing) return;
      const start = addDaysToIso(baseMonday, index * 7);
      const end = addDaysToIso(start, 2);
      const estadoActual = normalizeEstado(existing.estado, existing.riesgo_operativo);
      const estadoProgramado = estadoActual === 'LIBERADO' ? 'LIBERADO' : 'PROGRAMADO';
      const observacionAuto = `Programación semanal automática (${typeof formatDate === 'function' ? formatDate(start) : start} - ${typeof formatDate === 'function' ? formatDate(end) : end}).`;
      let updated = normalizeProgramacionEntry({
        ...existing,
        fecha_inicio: start,
        fecha_fin: end,
        estado: estadoProgramado,
        tecnico: existing.tecnico || tecnicoDefault || '',
        observaciones: existing.observaciones ? `${existing.observaciones}\n${observacionAuto}` : observacionAuto
      });
      updated = pushBitacora(updated, 'PROGRAMAR: programación semanal generada automáticamente.');
      updates.push(updated);
    });

    const updateIds = new Set(updates.map(item => item.id));
    programacionMantenimiento = programacionMantenimiento.map(entry => updateIds.has(entry.id)
      ? updates.find(item => item.id === entry.id)
      : entry
    );

    persistProgramacionLocal();
    await upsertProgramacionRemota(updates);
    renderProgramacionMantenimiento();
  }

  async function actualizarTecnicoProgramacion(entryId, tecnico) {
    const entry = getEntryById(entryId);
    if (!entry) return;
    const updated = normalizeProgramacionEntry({ ...entry, tecnico: String(tecnico || '').trim() });
    const idx = programacionMantenimiento.findIndex(item => item.id === entryId);
    if (idx < 0) return;
    programacionMantenimiento[idx] = updated;
    persistProgramacionLocal();
    await upsertProgramacionRemota([updated]);
  }

  function estadoSiguientePorAccion(entry, accion) {
    const today = toDateOnly(nowIso());
    if (accion === 'PROGRAMAR') {
      const start = entry.fecha_inicio || nextMondayIso(new Date());
      return normalizeProgramacionEntry({
        ...entry,
        estado: 'PROGRAMADO',
        fecha_inicio: start,
        fecha_fin: entry.fecha_fin || addDaysToIso(start, 2)
      });
    }
    if (accion === 'INICIAR') {
      const start = today;
      return normalizeProgramacionEntry({
        ...entry,
        estado: 'EN MANTENIMIENTO',
        fecha_inicio: entry.fecha_inicio || start,
        fecha_fin: entry.fecha_fin || addDaysToIso(entry.fecha_inicio || start, 2)
      });
    }
    if (accion === 'FINALIZAR') {
      return normalizeProgramacionEntry({
        ...entry,
        estado: 'TERMINADO',
        fecha_fin: today
      });
    }
    if (accion === 'LIBERAR') {
      return normalizeProgramacionEntry({
        ...entry,
        estado: 'LIBERADO',
        fecha_fin: entry.fecha_fin || today
      });
    }
    return entry;
  }

  async function accionProgramacion(entryId, accion) {
    const entry = getEntryById(entryId);
    if (!entry) return;
    let updated = estadoSiguientePorAccion(entry, accion);
    updated = pushBitacora(updated, `${String(accion || '').toUpperCase()}: estado actualizado a ${updated.estado}.`);
    const idx = programacionMantenimiento.findIndex(item => item.id === entryId);
    if (idx < 0) return;
    programacionMantenimiento[idx] = updated;
    persistProgramacionLocal();
    await upsertProgramacionRemota([updated]);
    renderProgramacionMantenimiento();
  }

  function renderUnidadInicioSelect(prioridades) {
    const select = document.getElementById('progUnidadInicio');
    if (!select) return;
    const currentValue = String(select.value || UNIDAD_INICIO_DEFAULT).trim();
    const options = prioridades.map(item =>
      `<option value="${escapeSafe(item.economico)}">${escapeSafe(item.economico)} | ${escapeSafe(item.planta)}</option>`
    ).join('');
    select.innerHTML = options || `<option value="${UNIDAD_INICIO_DEFAULT}">${UNIDAD_INICIO_DEFAULT}</option>`;
    const hasCurrent = prioridades.some(item => item.economico === currentValue);
    const hasDefault = prioridades.some(item => item.economico === UNIDAD_INICIO_DEFAULT);
    if (hasCurrent) select.value = currentValue;
    else if (hasDefault) select.value = UNIDAD_INICIO_DEFAULT;
  }

  function renderPrioridades(prioridades) {
    const tbody = document.getElementById('progPriorityBody');
    if (!tbody) return;
    if (!prioridades.length) {
      tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;color:var(--muted);padding:20px">Sin unidades para priorizar.</td></tr>';
      return;
    }

    tbody.innerHTML = prioridades.map(item => {
      const risk = describeRisk(item.riesgo_operativo);
      return `<tr>
        <td><b style="color:var(--accent)">${escapeSafe(item.economico)}</b></td>
        <td>${escapeSafe(item.planta)}</td>
        <td>${item.componentes_vencidos_count}</td>
        <td>${item.componentes_criticos_count}</td>
        <td>${item.dias_vencido}</td>
        <td>${item.prioridad.toFixed(2)}</td>
        <td><span class="prog-risk ${risk.cls}">${risk.label} (${item.riesgo_operativo})</span></td>
      </tr>`;
    }).join('');
  }

  function renderProgramacionSemanal(prioridades) {
    const tbody = document.getElementById('progWeeklyBody');
    if (!tbody) return;
    if (!programacionMantenimiento.length) {
      tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;color:var(--muted);padding:20px">Sin unidades programadas.</td></tr>';
      return;
    }

    const priorityMap = new Map(prioridades.map(item => [item.unidad_id, item]));
    const rows = [...programacionMantenimiento]
      .filter(item => item.unidad_id)
      .sort((a, b) =>
        String(a.fecha_inicio || '9999-99-99').localeCompare(String(b.fecha_inicio || '9999-99-99')) ||
        (Number(b.prioridad || 0) - Number(a.prioridad || 0))
      );

    tbody.innerHTML = rows.map(item => {
      const prio = priorityMap.get(item.unidad_id);
      const risk = describeRisk(item.riesgo_operativo);
      const semana = item.fecha_inicio && item.fecha_fin
        ? getIsoWeekLabel(item.fecha_inicio, item.fecha_fin)
        : 'SIN ASIGNAR';
      const tecnicoEscaped = escapeSafe(item.tecnico || '');

      return `<tr>
        <td>${escapeSafe(semana)}</td>
        <td><b style="color:var(--accent)">${escapeSafe(item.economico || prio?.economico || '—')}</b></td>
        <td>${escapeSafe(item.planta || prio?.planta || 'SIN PLANTA')}</td>
        <td>
          <input class="prog-tech-input" type="text" value="${tecnicoEscaped}" onchange="actualizarTecnicoProgramacion('${escapeSafe(item.id)}', this.value)">
        </td>
        <td><span class="${estadoCssClass(item.estado)}">${escapeSafe(item.estado)}</span></td>
        <td>${Number(item.prioridad || 0).toFixed(2)}</td>
        <td><span class="prog-risk ${risk.cls}">${risk.label}</span></td>
        <td>
          <div class="prog-actions-inline">
            <button class="btn btn-secondary" type="button" onclick="accionProgramacionMantenimiento('${escapeSafe(item.id)}','PROGRAMAR')">PROGRAMAR</button>
            <button class="btn btn-secondary" type="button" onclick="accionProgramacionMantenimiento('${escapeSafe(item.id)}','INICIAR')">INICIAR</button>
            <button class="btn btn-secondary" type="button" onclick="accionProgramacionMantenimiento('${escapeSafe(item.id)}','FINALIZAR')">FINALIZAR</button>
            <button class="btn btn-secondary" type="button" onclick="accionProgramacionMantenimiento('${escapeSafe(item.id)}','LIBERAR')">LIBERAR</button>
          </div>
        </td>
      </tr>`;
    }).join('');
  }

  function renderKpis(prioridades) {
    const total = prioridades.length;
    const programadas = programacionMantenimiento.filter(item =>
      ['PROGRAMADO', 'EN MANTENIMIENTO', 'ESPERANDO REFACCIONES'].includes(item.estado)
    ).length;
    const fueraServicio = programacionMantenimiento.filter(item => item.estado === 'FUERA DE SERVICIO').length;
    const backlog = programacionMantenimiento.filter(item => !['LIBERADO', 'TERMINADO'].includes(item.estado)).length;
    const operables = clamp(total - fueraServicio, 0, total);
    const pctOperable = total ? Math.round((operables / total) * 100) : 0;
    const semanas = backlog;

    const setText = (id, value) => {
      const el = document.getElementById(id);
      if (el) el.textContent = value;
    };
    setText('progKpiProgramadas', String(programadas));
    setText('progKpiFueraServicio', String(fueraServicio));
    setText('progKpiBacklog', String(backlog));
    setText('progKpiOperable', `${pctOperable}%`);
    setText('progKpiSemanas', String(semanas));
  }

  function renderAlertas() {
    const wrap = document.getElementById('progAlerts');
    if (!wrap) return;
    const alerts = [];
    const today = new Date();
    today.setHours(0, 0, 0, 0);

    programacionMantenimiento.forEach(item => {
      const diasInicio = getDiasHasta(item.fecha_inicio);
      if (item.estado === 'PROGRAMADO' && diasInicio !== null && diasInicio >= 0 && diasInicio <= 7) {
        alerts.push({
          tipo: 'MANTENIMIENTO PRÓXIMO',
          texto: `${item.economico}: inicia en ${diasInicio} día(s).`,
          cls: 'prog-risk-programar'
        });
      }
      if (item.riesgo_operativo >= 61) {
        alerts.push({
          tipo: 'UNIDAD CRÍTICA',
          texto: `${item.economico}: riesgo operativo ${item.riesgo_operativo}.`,
          cls: item.riesgo_operativo >= 81 ? 'prog-risk-fuera' : 'prog-risk-urgente'
        });
      }
      if (item.estado === 'PROGRAMADO' && diasInicio !== null && diasInicio < 0) {
        alerts.push({
          tipo: 'MANTENIMIENTO VENCIDO',
          texto: `${item.economico}: vencido por ${Math.abs(diasInicio)} día(s).`,
          cls: 'prog-risk-fuera'
        });
      }
      if (item.estado === 'EN MANTENIMIENTO' && item.fecha_inicio) {
        const started = new Date(`${item.fecha_inicio}T00:00:00`);
        const diff = Math.floor((today.getTime() - started.getTime()) / 86400000);
        if (diff > 3) {
          alerts.push({
            tipo: 'MANTENIMIENTO RETRASADO',
            texto: `${item.economico}: ${diff} día(s) en taller.`,
            cls: 'prog-risk-urgente'
          });
        }
      }
    });

    if (!alerts.length) {
      wrap.innerHTML = '<p class="text-muted">Sin alertas por ahora.</p>';
      return;
    }

    wrap.innerHTML = alerts.slice(0, 18).map(item => `
      <div class="prog-alert-item">
        <div class="prog-alert-text"><b>${escapeSafe(item.tipo)}:</b> ${escapeSafe(item.texto)}</div>
        <span class="prog-risk ${item.cls}">${escapeSafe(item.tipo)}</span>
      </div>
    `).join('');
  }

  async function renderProgramacionMantenimiento() {
    await cargarProgramacionRemota();
    const prioridades = await calcularPrioridadMantenimiento({ silent: true, skipSync: true });
    renderUnidadInicioSelect(prioridades);
    renderKpis(prioridades);
    renderAlertas();
    renderPrioridades(prioridades);
    renderProgramacionSemanal(prioridades);
  }

  function openProgramacionTab() {
    document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
    document.querySelectorAll('.tab-panel').forEach(panel => panel.classList.remove('active'));
    const tabBtn = document.getElementById('tabBtnProgramacion');
    const panel = document.getElementById('tab-programacion');
    if (tabBtn) tabBtn.classList.add('active');
    if (panel) panel.classList.add('active');
    renderProgramacionMantenimiento();
  }

  function watchBootstrapAndInit() {
    if (moduloInicializado) return;
    if (typeof appBootstrapped !== 'undefined' && appBootstrapped) {
      moduloInicializado = true;
      renderProgramacionMantenimiento();
      return;
    }
    setTimeout(watchBootstrapAndInit, 450);
  }

  document.addEventListener('DOMContentLoaded', watchBootstrapAndInit);

  window.openProgramacionTab = openProgramacionTab;
  window.calcularPrioridadMantenimiento = calcularPrioridadMantenimiento;
  window.generarProgramacionSemanal = generarProgramacionSemanal;
  window.accionProgramacionMantenimiento = accionProgramacion;
  window.actualizarTecnicoProgramacion = actualizarTecnicoProgramacion;
})();
