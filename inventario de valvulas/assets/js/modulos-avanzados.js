(function advancedModules() {
  'use strict';

  const REFACCIONES_KEY = 'inv_refacciones';
  const MOVIMIENTOS_KEY = 'inv_movimientos';
  const EVIDENCIAS_MOVIL_KEY = 'tec_evidencias';
  const QR_COMPONENTES_KEY = 'qr_componentes';
  const BLOQUEOS_KEY = 'unidad_bloqueos';
  const AUDITORIA_KEY = 'auditoria_sistema';
  const FUGAS_KEY = 'unidad_fugas_reportadas';

  const TABLE_REFACCIONES = String(window.SUPABASE_CONFIG?.tableRefacciones || 'inventario_refacciones').trim() || 'inventario_refacciones';
  const TABLE_MOVIMIENTOS = String(window.SUPABASE_CONFIG?.tableMovimientosInventario || 'movimientos_inventario').trim() || 'movimientos_inventario';
  const TABLE_AUDITORIA = String(window.SUPABASE_CONFIG?.tableAuditoriaSistema || 'auditoria_sistema').trim() || 'auditoria_sistema';
  const AUDITORIA_ENABLED = false;

  let refacciones = readArray(REFACCIONES_KEY).map(normalizeRefaccion);
  let movimientosInventario = readArray(MOVIMIENTOS_KEY).map(normalizeMovimiento);
  let evidenciasMovil = readArray(EVIDENCIAS_MOVIL_KEY);
  let qrComponentes = readArray(QR_COMPONENTES_KEY).map(normalizeQRItem);
  let bloqueosUnidades = readArray(BLOQUEOS_KEY).map(normalizeBloqueo);
  let auditoriaSistema = readArray(AUDITORIA_KEY).map(normalizeAuditoria);
  let facturaDraft = {
    proveedor: '',
    folio: '',
    fecha: '',
    total: 0,
    items: []
  };

  function readArray(key) {
    try {
      const parsed = JSON.parse(localStorage.getItem(key) || '[]');
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  }

  function writeArray(key, value) {
    try {
      localStorage.setItem(key, JSON.stringify(Array.isArray(value) ? value : []));
    } catch {}
  }

  function nowIso() {
    return new Date().toISOString();
  }

  function genLocalId() {
    return typeof genId === 'function'
      ? genId()
      : `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 7)}`;
  }

  function safeText(v) {
    if (typeof escapeHtml === 'function') return escapeHtml(v);
    return String(v ?? '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function toDateOnly(v) {
    const raw = String(v || '').trim();
    if (!raw) return '';
    if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;
    const d = new Date(raw);
    if (Number.isNaN(d.getTime())) return '';
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    return `${y}-${m}-${day}`;
  }

  function addDays(isoDate, days) {
    const d = new Date(`${isoDate}T00:00:00`);
    d.setDate(d.getDate() + days);
    return toDateOnly(d.toISOString());
  }

  function todayIso() {
    return toDateOnly(nowIso());
  }

  function getCurrentUser() {
    try {
      return String(typeof activeAuditUser !== 'undefined' ? activeAuditUser : 'Sistema').trim() || 'Sistema';
    } catch {
      return 'Sistema';
    }
  }

  function maintenanceRows() {
    const programacion = readArray('at_programacion_mantenimiento');
    const rows = programacion
      .map(item => ({
        id: String(item?.id || ''),
        unidad_id: String(item?.unidad_id || ''),
        economico: String(item?.economico || ''),
        planta: String(item?.planta || ''),
        fecha_inicio: toDateOnly(item?.fecha_inicio || ''),
        fecha_fin: toDateOnly(item?.fecha_fin || ''),
        estado: String(item?.estado || '').toUpperCase(),
        tecnico: String(item?.tecnico || ''),
        prioridad: Number(item?.prioridad || 0),
        riesgo_operativo: Number(item?.riesgo_operativo || 0),
        componentes_vencidos: String(item?.componentes_vencidos || '')
      }))
      .filter(item => item.unidad_id || item.economico);
    return rows;
  }

  function normalizeRefaccion(item) {
    return {
      id: String(item?.id || genLocalId()),
      nombre: String(item?.nombre || '').trim(),
      numero_parte: String(item?.numero_parte || '').trim(),
      marca: String(item?.marca || '').trim(),
      stock_actual: Math.max(0, Number(item?.stock_actual || 0)),
      stock_minimo: Math.max(0, Number(item?.stock_minimo || 0)),
      ubicacion: String(item?.ubicacion || '').trim(),
      proveedor: String(item?.proveedor || '').trim(),
      costo: Math.max(0, Number(item?.costo || 0)),
      categoria: String(item?.categoria || '').trim(),
      created_at: String(item?.created_at || nowIso())
    };
  }

  function normalizeMovimiento(item) {
    return {
      id: String(item?.id || genLocalId()),
      refaccion_id: String(item?.refaccion_id || '').trim(),
      tipo_movimiento: String(item?.tipo_movimiento || 'SALIDA').toUpperCase(),
      cantidad: Math.max(1, Number(item?.cantidad || 1)),
      unidad: String(item?.unidad || '').trim(),
      tecnico: String(item?.tecnico || '').trim(),
      fecha: String(item?.fecha || nowIso()),
      observaciones: String(item?.observaciones || '').trim()
    };
  }

  function normalizeQRItem(item) {
    return {
      id: String(item?.id || genLocalId()),
      record_id: String(item?.record_id || '').trim(),
      unidad_id: String(item?.unidad_id || '').trim(),
      economico: String(item?.economico || '').trim(),
      componente: String(item?.componente || '').trim(),
      serie: String(item?.serie || '').trim(),
      fecha_instalacion: String(item?.fecha_instalacion || '').trim(),
      fecha_reemplazo: String(item?.fecha_reemplazo || '').trim(),
      estado: String(item?.estado || '').trim(),
      payload: String(item?.payload || '').trim(),
      qr_url: String(item?.qr_url || '').trim(),
      created_at: String(item?.created_at || nowIso())
    };
  }

  function normalizeBloqueo(item) {
    return {
      id: String(item?.id || genLocalId()),
      unidad_id: String(item?.unidad_id || '').trim(),
      economico: String(item?.economico || '').trim(),
      motivo: String(item?.motivo || '').trim(),
      fecha_bloqueo: String(item?.fecha_bloqueo || nowIso()),
      responsable: String(item?.responsable || 'Sistema').trim(),
      estado: String(item?.estado || 'FUERA DE SERVICIO').trim()
    };
  }

  function normalizeAuditoria(item) {
    return {
      id: String(item?.id || genLocalId()),
      usuario: String(item?.usuario || 'Sistema').trim(),
      accion: String(item?.accion || '').trim(),
      modulo: String(item?.modulo || '').trim(),
      descripcion: String(item?.descripcion || '').trim(),
      fecha: String(item?.fecha || nowIso()),
      unidad: String(item?.unidad || '').trim(),
      evidencia: String(item?.evidencia || '').trim()
    };
  }

  function persistAll() {
    writeArray(REFACCIONES_KEY, refacciones);
    writeArray(MOVIMIENTOS_KEY, movimientosInventario);
    writeArray(EVIDENCIAS_MOVIL_KEY, evidenciasMovil);
    writeArray(QR_COMPONENTES_KEY, qrComponentes);
    writeArray(BLOQUEOS_KEY, bloqueosUnidades);
    writeArray(AUDITORIA_KEY, auditoriaSistema.slice(0, 5000));
  }

  function logAuditoria(modulo, accion, descripcion, unidad = '', evidencia = '') {
    if (!AUDITORIA_ENABLED) return;
    const entry = normalizeAuditoria({
      id: genLocalId(),
      usuario: getCurrentUser(),
      accion,
      modulo,
      descripcion,
      fecha: nowIso(),
      unidad,
      evidencia
    });
    auditoriaSistema.unshift(entry);
    auditoriaSistema = auditoriaSistema.slice(0, 5000);
    writeArray(AUDITORIA_KEY, auditoriaSistema);
    upsertSupabase(TABLE_AUDITORIA, [entry]);
  }

  function hasSupabase() {
    return Boolean(
      typeof runtimeUseSupabase !== 'undefined' &&
      runtimeUseSupabase &&
      typeof supabaseClient !== 'undefined' &&
      supabaseClient
    );
  }

  function isMissingTable(error) {
    const code = String(error?.code || '').trim();
    const msg = String(error?.message || '').toLowerCase();
    return code === '42P01' || msg.includes('does not exist');
  }

  async function upsertSupabase(table, rows) {
    if (!hasSupabase() || !rows.length) return;
    try {
      const { error } = await supabaseClient.from(table).upsert(rows);
      if (error && !isMissingTable(error)) console.warn(`[ADV] Error upsert ${table}`, error);
    } catch (err) {
      console.warn(`[ADV] FallÃ³ sync ${table}`, err);
    }
  }

  function removeExtendedTabSelection() {
    document.querySelectorAll('.ext-tab-btn').forEach(btn => btn.classList.remove('active'));
  }

  function getAdvancedTabButtonId(tabId) {
    const map = {
      cronograma: 'tabBtnCronograma',
      'inventario-refacciones': 'tabBtnInventarioRef'
    };
    return map[tabId] || '';
  }

  function openAdvancedTab(tabId) {
    document.querySelectorAll('.tab-panel').forEach(panel => panel.classList.remove('active'));
    document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
    removeExtendedTabSelection();
    document.getElementById(`tab-${tabId}`)?.classList.add('active');
    const buttonId = getAdvancedTabButtonId(tabId);
    if (buttonId) document.getElementById(buttonId)?.classList.add('active');

    if (tabId === 'cronograma') renderCronogramaGantt();
    if (tabId === 'inventario-refacciones') renderInventarioRefacciones();
  }

  function bindTabBridge() {
    document.querySelectorAll('.tab-btn').forEach(btn => {
      if (btn.dataset.advBound === '1') return;
      btn.addEventListener('click', () => removeExtendedTabSelection());
      btn.dataset.advBound = '1';
    });
  }

  function initializePlantFilter() {
    const plants = Array.from(new Set((Array.isArray(autotanques) ? autotanques : []).map(a => String(a?.plantaActual || '').trim()).filter(Boolean)));
    const select = document.getElementById('cronogramaPlantFilter');
    if (!select) return;
    const prev = select.value || '';
    select.innerHTML = `<option value="">Todas las plantas</option>${plants.map(p => `<option value="${safeText(p)}">${safeText(p)}</option>`).join('')}`;
    if (prev && plants.includes(prev)) select.value = prev;
  }

  function weekOfYear(date) {
    const d = new Date(date);
    const target = new Date(d.valueOf());
    const dayNr = (d.getDay() + 6) % 7;
    target.setDate(target.getDate() - dayNr + 3);
    const firstThursday = target.valueOf();
    target.setMonth(0, 1);
    if (target.getDay() !== 4) target.setMonth(0, 1 + ((4 - target.getDay()) + 7) % 7);
    return 1 + Math.ceil((firstThursday - target) / 604800000);
  }

  function calcularDuracionMantenimiento(entry) {
    const start = toDateOnly(entry?.fecha_inicio || '');
    const end = toDateOnly(entry?.fecha_fin || '');
    if (!start || !end) return 0;
    const ds = new Date(`${start}T00:00:00`);
    const de = new Date(`${end}T00:00:00`);
    if (Number.isNaN(ds.getTime()) || Number.isNaN(de.getTime())) return 0;
    return Math.max(1, Math.floor((de - ds) / 86400000) + 1);
  }

  function actualizarAvanceMantenimiento(entry) {
    const estado = String(entry?.estado || '').toUpperCase();
    if (estado === 'LIBERADO' || estado === 'TERMINADO') return 100;
    if (estado === 'ESPERANDO REFACCIONES') return 85;
    if (estado === 'EN MANTENIMIENTO') {
      const start = toDateOnly(entry?.fecha_inicio || '');
      const end = toDateOnly(entry?.fecha_fin || '');
      if (!start || !end) return 50;
      const total = calcularDuracionMantenimiento(entry);
      const elapsed = Math.max(0, Math.floor((new Date(`${todayIso()}T00:00:00`) - new Date(`${start}T00:00:00`)) / 86400000) + 1);
      return Math.min(95, Math.max(10, Math.round((elapsed / total) * 100)));
    }
    if (estado === 'PROGRAMADO') return 15;
    if (estado === 'FUERA DE SERVICIO') return 0;
    return 5;
  }

  function estadoToBarClass(estado) {
    const value = String(estado || '').toUpperCase();
    if (value === 'LIBERADO' || value === 'TERMINADO') return 'adv-state-liberado';
    if (value === 'PROGRAMADO') return 'adv-state-programado';
    if (value === 'EN MANTENIMIENTO') return 'adv-state-en-mantenimiento';
    if (value === 'ESPERANDO REFACCIONES') return 'adv-state-esperando-refacciones';
    return 'adv-state-fuera-de-servicio';
  }

  function stateLabelClass(stock, min) {
    if (stock <= 0) return { text: 'PIEZA AGOTADA', cls: 'adv-status-danger' };
    if (stock < min) return { text: 'STOCK CRÃTICO', cls: 'adv-status-danger' };
    if (stock === min) return { text: 'STOCK BAJO', cls: 'adv-status-warn' };
    return { text: 'OPERABLE', cls: 'adv-status-ok' };
  }

  function buildCronogramaRows() {
    const plant = String(document.getElementById('cronogramaPlantFilter')?.value || '').trim();
    const status = String(document.getElementById('cronogramaStatusFilter')?.value || '').trim().toUpperCase();
    return maintenanceRows().filter(row => {
      if (plant && row.planta !== plant) return false;
      if (status && row.estado !== status) return false;
      return Boolean(row.fecha_inicio && row.fecha_fin);
    });
  }

  function renderCronogramaCapacity(rows) {
    const wrap = document.getElementById('cronogramaCapacity');
    if (!wrap) return;
    if (!rows.length) {
      wrap.innerHTML = '<p class="text-muted">Sin mantenimientos con fecha para cronograma.</p>';
      return;
    }

    const capByPlant = new Map();
    const conflicts = [];
    rows.forEach(a => {
      capByPlant.set(a.planta, (capByPlant.get(a.planta) || 0) + 1);
      rows.forEach(b => {
        if (a.id === b.id || a.unidad_id !== b.unidad_id) return;
        if (a.fecha_inicio <= b.fecha_fin && b.fecha_inicio <= a.fecha_fin) {
          conflicts.push(`${a.economico}: ${a.fecha_inicio} â†” ${b.fecha_fin}`);
        }
      });
    });

    const cards = Array.from(capByPlant.entries()).map(([plant, count]) =>
      `<div class="adv-pill"><b>${safeText(plant || 'SIN PLANTA')}</b><br>Capacidad ocupada: ${count}</div>`
    );
    const conflictsHtml = conflicts.length
      ? `<div class="adv-pill"><b>Conflictos:</b><br>${safeText(Array.from(new Set(conflicts)).slice(0, 6).join(' | '))}</div>`
      : `<div class="adv-pill"><b>Conflictos:</b><br>Sin conflictos detectados.</div>`;
    wrap.innerHTML = `${cards.join('')}${conflictsHtml}`;
  }

  function renderCronogramaGantt() {
    initializePlantFilter();
    const rows = buildCronogramaRows();
    renderCronogramaCapacity(rows);
    const container = document.getElementById('cronogramaGanttGrid');
    if (!container) return;

    const header = [
      '<div class="adv-gantt-cell header adv-gantt-row-head">UNIDAD | PLANTA | TÃ‰CNICO | INICIO | FIN | ESTADO | AVANCE</div>'
    ];
    for (let w = 1; w <= 52; w++) {
      header.push(`<div class="adv-gantt-cell header">S${w}</div>`);
    }

    const body = rows.sort((a, b) =>
      String(a.fecha_inicio).localeCompare(String(b.fecha_inicio)) ||
      String(a.economico).localeCompare(String(b.economico))
    ).flatMap(row => {
      const startWeek = weekOfYear(new Date(`${row.fecha_inicio}T00:00:00`));
      const endWeek = weekOfYear(new Date(`${row.fecha_fin}T00:00:00`));
      const avance = actualizarAvanceMantenimiento(row);
      const head = `<div class="adv-gantt-cell adv-gantt-row-head">${safeText(row.economico)} | ${safeText(row.planta || 'SIN PLANTA')} | ${safeText(row.tecnico || 'SIN TÃ‰CNICO')} | ${safeText(typeof formatDate === 'function' ? formatDate(row.fecha_inicio) : row.fecha_inicio)} | ${safeText(typeof formatDate === 'function' ? formatDate(row.fecha_fin) : row.fecha_fin)} | ${safeText(row.estado)} | ${avance}%</div>`;
      const cols = [];
      for (let w = 1; w <= 52; w++) {
        if (w >= startWeek && w <= endWeek) {
          cols.push(`<div class="adv-gantt-cell"><div class="adv-gantt-bar ${estadoToBarClass(row.estado)}" title="${safeText(row.economico)} - ${row.estado} (${avance}%)"></div></div>`);
        } else {
          cols.push('<div class="adv-gantt-cell"></div>');
        }
      }
      return [head, ...cols];
    });

    container.innerHTML = [...header, ...body].join('');
  }

  function populateRefaccionesSelect() {
    const select = document.getElementById('movRefaccionId');
    if (!select) return;
    const prev = select.value || '';
    select.innerHTML = '<option value="">â€” Seleccionar refacciÃ³n â€”</option>' +
      refacciones.map(item => `<option value="${safeText(item.id)}">${safeText(item.nombre)} | ${safeText(item.numero_parte)}</option>`).join('');
    if (prev && refacciones.some(item => item.id === prev)) select.value = prev;
  }

  function guardarRefaccion() {
    const payload = normalizeRefaccion({
      id: genLocalId(),
      nombre: document.getElementById('invNombre')?.value || '',
      numero_parte: document.getElementById('invNumeroParte')?.value || '',
      marca: document.getElementById('invMarca')?.value || '',
      stock_actual: Number(document.getElementById('invStockActual')?.value || 0),
      stock_minimo: Number(document.getElementById('invStockMinimo')?.value || 0),
      ubicacion: document.getElementById('invUbicacion')?.value || '',
      proveedor: document.getElementById('invProveedor')?.value || '',
      costo: Number(document.getElementById('invCosto')?.value || 0),
      categoria: document.getElementById('invCategoria')?.value || '',
      created_at: nowIso()
    });
    if (!payload.nombre || !payload.numero_parte) {
      alert('Nombre y nÃºmero de parte son obligatorios.');
      return;
    }

    const exists = refacciones.find(item => item.numero_parte.toLowerCase() === payload.numero_parte.toLowerCase());
    if (exists) {
      exists.nombre = payload.nombre;
      exists.marca = payload.marca;
      exists.stock_actual = payload.stock_actual;
      exists.stock_minimo = payload.stock_minimo;
      exists.ubicacion = payload.ubicacion;
      exists.proveedor = payload.proveedor;
      exists.costo = payload.costo;
      exists.categoria = payload.categoria;
      logAuditoria('INVENTARIO REFACCIONES', 'ACTUALIZAR', `RefacciÃ³n actualizada: ${exists.nombre}`, '', '');
    } else {
      refacciones.push(payload);
      logAuditoria('INVENTARIO REFACCIONES', 'CREAR', `RefacciÃ³n registrada: ${payload.nombre}`, '', '');
    }
    persistAll();
    upsertSupabase(TABLE_REFACCIONES, refacciones);
    limpiarRefaccionForm();
    renderInventarioRefacciones();
  }

  function limpiarRefaccionForm() {
    ['invNombre','invNumeroParte','invMarca','invStockActual','invStockMinimo','invUbicacion','invProveedor','invCosto','invCategoria']
      .forEach(id => {
        const el = document.getElementById(id);
        if (el) el.value = '';
      });
  }

  function validarStockDisponible(refaccionId, cantidad) {
    const item = refacciones.find(r => r.id === refaccionId);
    if (!item) return false;
    return Number(item.stock_actual || 0) >= Number(cantidad || 0);
  }

  function registrarMovimientoInventario() {
    const refaccionId = String(document.getElementById('movRefaccionId')?.value || '').trim();
    const tipo = String(document.getElementById('movTipo')?.value || 'SALIDA').toUpperCase();
    const cantidad = Math.max(1, Number(document.getElementById('movCantidad')?.value || 1));
    const unidad = String(document.getElementById('movUnidad')?.value || '').trim();
    const tecnico = String(document.getElementById('movTecnico')?.value || '').trim();
    const observaciones = String(document.getElementById('movObservaciones')?.value || '').trim();

    const ref = refacciones.find(item => item.id === refaccionId);
    if (!ref) return alert('Selecciona una refacciÃ³n.');
    if (tipo === 'SALIDA' && !validarStockDisponible(refaccionId, cantidad)) {
      alert('No hay stock suficiente para la salida solicitada.');
      return;
    }

    if (tipo === 'ENTRADA') ref.stock_actual += cantidad;
    if (tipo === 'SALIDA') ref.stock_actual = Math.max(0, ref.stock_actual - cantidad);
    if (tipo === 'AJUSTE') ref.stock_actual = Math.max(0, cantidad);

    const mov = normalizeMovimiento({
      id: genLocalId(),
      refaccion_id: refaccionId,
      tipo_movimiento: tipo,
      cantidad,
      unidad,
      tecnico,
      fecha: nowIso(),
      observaciones
    });
    movimientosInventario.unshift(mov);
    logAuditoria('INVENTARIO REFACCIONES', tipo, `${tipo} de ${cantidad} pza(s): ${ref.nombre}`, unidad, '');
    persistAll();
    upsertSupabase(TABLE_REFACCIONES, [ref]);
    upsertSupabase(TABLE_MOVIMIENTOS, [mov]);
    renderInventarioRefacciones();
  }

  function descontarRefaccionesAutomaticamente(unidad, tecnico = 'Sistema') {
    const impacted = refacciones.slice(0, 3);
    impacted.forEach(ref => {
      if (ref.stock_actual <= 0) return;
      ref.stock_actual = Math.max(0, ref.stock_actual - 1);
      const mov = normalizeMovimiento({
        id: genLocalId(),
        refaccion_id: ref.id,
        tipo_movimiento: 'SALIDA',
        cantidad: 1,
        unidad,
        tecnico,
        fecha: nowIso(),
        observaciones: 'Descuento automÃ¡tico por finalizaciÃ³n de mantenimiento'
      });
      movimientosInventario.unshift(mov);
      upsertSupabase(TABLE_MOVIMIENTOS, [mov]);
    });
    persistAll();
    upsertSupabase(TABLE_REFACCIONES, impacted);
    logAuditoria('INVENTARIO REFACCIONES', 'SALIDA AUTOMÃTICA', 'Descuento automÃ¡tico de refacciones al finalizar mantenimiento', unidad, '');
  }

  function generarAlertaInventario() {
    return refacciones.map(item => {
      const status = stateLabelClass(item.stock_actual, item.stock_minimo);
      return { item, status };
    });
  }

  function inventoryKpis() {
    const agotadas = refacciones.filter(item => item.stock_actual <= 0).length;
    const criticas = refacciones.filter(item => item.stock_actual > 0 && item.stock_actual < item.stock_minimo).length;
    const costoTotal = refacciones.reduce((acc, item) => acc + (item.stock_actual * item.costo), 0);
    const monthKey = todayIso().slice(0, 7);
    const consumoMensual = movimientosInventario
      .filter(mov => mov.tipo_movimiento === 'SALIDA' && String(mov.fecha || '').slice(0, 7) === monthKey)
      .reduce((acc, mov) => acc + Number(mov.cantidad || 0), 0);
    return { agotadas, criticas, costoTotal, consumoMensual };
  }

  function renderInventarioRefacciones() {
    populateRefaccionesSelect();
    const stockBody = document.getElementById('invStockBody');
    const movBody = document.getElementById('invMovBody');
    const alertWrap = document.getElementById('invAlertas');
    if (!stockBody || !movBody || !alertWrap) return;

    const alerts = generarAlertaInventario();
    stockBody.innerHTML = refacciones.length
      ? refacciones.map(item => {
          const status = stateLabelClass(item.stock_actual, item.stock_minimo);
          return `<tr>
            <td>${safeText(item.nombre)}</td>
            <td>${safeText(item.numero_parte)}</td>
            <td>${item.stock_actual}</td>
            <td>${item.stock_minimo}</td>
            <td><span class="adv-status-tag ${status.cls}">${status.text}</span></td>
            <td>$${item.costo.toFixed(2)}</td>
          </tr>`;
        }).join('')
      : '<tr><td colspan="6" style="text-align:center;color:var(--muted);padding:20px">Sin refacciones registradas.</td></tr>';

    movBody.innerHTML = movimientosInventario.length
      ? movimientosInventario.slice(0, 200).map(mov => {
          const ref = refacciones.find(item => item.id === mov.refaccion_id);
          return `<tr>
            <td>${safeText(typeof formatDateTime === 'function' ? formatDateTime(mov.fecha) : mov.fecha)}</td>
            <td>${safeText(mov.tipo_movimiento)}</td>
            <td>${safeText(ref?.nombre || 'RefacciÃ³n eliminada')}</td>
            <td>${mov.cantidad}</td>
            <td>${safeText(mov.unidad || 'â€”')}</td>
            <td>${safeText(mov.tecnico || 'â€”')}</td>
          </tr>`;
        }).join('')
      : '<tr><td colspan="6" style="text-align:center;color:var(--muted);padding:20px">Sin movimientos de inventario.</td></tr>';

    const criticalRows = alerts.filter(a => a.status.text !== 'OPERABLE');
    alertWrap.innerHTML = criticalRows.length
      ? criticalRows.map(row => `<div class="adv-pill"><span class="adv-status-tag ${row.status.cls}">${row.status.text}</span> ${safeText(row.item.nombre)} (${row.item.stock_actual}/${row.item.stock_minimo})</div>`).join('')
      : '<p class="text-muted">Sin alertas de inventario.</p>';

    const kpis = inventoryKpis();
    const setKpi = (id, value) => {
      const el = document.getElementById(id);
      if (el) el.textContent = value;
    };
    setKpi('invKpiAgotadas', String(kpis.agotadas));
    setKpi('invKpiCriticas', String(kpis.criticas));
    setKpi('invKpiCosto', `$${kpis.costoTotal.toFixed(2)}`);
    setKpi('invKpiConsumo', String(kpis.consumoMensual));
  }

  function normalizeLooseNumber(value) {
    const raw = String(value || '').trim();
    if (!raw) return 0;
    const cleaned = raw.replace(/[^0-9,.-]/g, '');
    if (!cleaned) return 0;
    const normalized = cleaned.includes(',') && cleaned.includes('.')
      ? cleaned.replace(/,/g, '')
      : cleaned.replace(',', '.');
    const parsed = Number(normalized);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  function normalizeForMatch(value) {
    return String(value || '')
      .toLowerCase()
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/[^a-z0-9]+/g, ' ')
      .trim();
  }

  function getFacturaTextInput() {
    const txt = String(document.getElementById('exportInvoiceText')?.value || '').trim();
    if (txt) return Promise.resolve(txt);
    const file = document.getElementById('exportInvoiceFile')?.files?.[0];
    if (!file) return Promise.resolve('');
    const ext = String(file.name || '').toLowerCase();
    const isText = ext.endsWith('.txt') || ext.endsWith('.csv') || ext.endsWith('.json') || ext.endsWith('.xml');
    if (!isText) return Promise.resolve('');
    return new Promise((resolve) => {
      const reader = new FileReader();
      reader.onload = () => resolve(String(reader.result || ''));
      reader.onerror = () => resolve('');
      reader.readAsText(file);
    });
  }

  function detectFacturaMeta(text) {
    const src = String(text || '');
    const proveedorMatch = src.match(/(?:proveedor|vendor|razon social)\s*[:\-]\s*(.+)/i);
    const folioMatch = src.match(/(?:folio|factura|invoice|uuid)\s*[:#\-]\s*([a-z0-9\-]+)/i);
    const fechaMatch = src.match(/(\d{2}\/\d{2}\/\d{4}|\d{4}-\d{2}-\d{2})/);
    const totalMatch = src.match(/(?:total)\s*[:\$ ]+\s*([\d.,]+)/i);
    return {
      proveedor: proveedorMatch ? proveedorMatch[1].split('\n')[0].trim() : '',
      folio: folioMatch ? folioMatch[1].trim() : '',
      fecha: fechaMatch ? toDateOnly(fechaMatch[1]) : '',
      total: totalMatch ? normalizeLooseNumber(totalMatch[1]) : 0
    };
  }

  function suggestRefaccionIdByDescription(desc) {
    const needle = normalizeForMatch(desc);
    if (!needle) return '';
    let winner = '';
    let bestScore = 0;
    refacciones.forEach(item => {
      const bag = [
        item.nombre,
        item.numero_parte,
        item.marca,
        item.categoria
      ].map(normalizeForMatch).join(' ');
      if (!bag) return;
      let score = 0;
      const tokens = needle.split(' ').filter(Boolean);
      tokens.forEach(token => {
        if (token.length > 2 && bag.includes(token)) score += 1;
      });
      if (bag.includes(needle)) score += 3;
      if (score > bestScore) {
        bestScore = score;
        winner = item.id;
      }
    });
    return bestScore > 0 ? winner : '';
  }

  function parseFacturaItems(text) {
    const compactText = String(text || '')
      .replace(/\t/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
    let lines = String(text || '')
      .split(/\r?\n/)
      .map(line => line.trim())
      .filter(Boolean);

    if (lines.length <= 1 && compactText) {
      lines = compactText
        .split(/(?=\b\d+\s+[A-Z]{1,4}-[A-Z0-9]+:)/g)
        .map(line => line.trim())
        .filter(Boolean);
      if (!lines.length) lines = [compactText];
    }

    const sanitizeDesc = (value) => String(value || '')
      .replace(/^\d+\s+[A-Z]{1,4}-[A-Z0-9]+:\s*/i, '')
      .replace(/^(?:\d+(?:[.,]\d+)?%?\s+)+/i, '')
      .replace(/\s+/g, ' ')
      .trim();

    const items = [];
    lines.forEach(line => {
      const compact = line.replace(/\s+/g, ' ');

      let match = compact.match(/^(\d+(?:[.,]\d+)?)\s*[xX]\s+(.+?)\s+([\d.,]+)$/);
      if (match) {
        const qty = Math.max(1, Math.round(normalizeLooseNumber(match[1])));
        const desc = sanitizeDesc(match[2]);
        const amount = normalizeLooseNumber(match[3]);
        const unit = qty > 0 ? (amount / qty) : amount;
        items.push({
          id: genLocalId(),
          descripcion: desc,
          refaccion_id: suggestRefaccionIdByDescription(desc),
          cantidad: qty,
          costo_unitario: unit > 0 ? unit : 0
        });
        return;
      }

      match = compact.match(/^(.+?)\s+(\d+(?:[.,]\d+)?)\s+([\d.,]+)\s+([\d.,]+)$/);
      if (match) {
        const desc = sanitizeDesc(match[1]);
        const qty = Math.max(1, Math.round(normalizeLooseNumber(match[2])));
        const unit = normalizeLooseNumber(match[3]);
        items.push({
          id: genLocalId(),
          descripcion: desc,
          refaccion_id: suggestRefaccionIdByDescription(desc),
          cantidad: qty,
          costo_unitario: unit > 0 ? unit : 0
        });
        return;
      }

      match = compact.match(/^(.+?)\s+(\d+(?:[.,]\d+)?)\s*PZA\b\s+([\d.,]+)/i);
      if (match) {
        const desc = sanitizeDesc(match[1]);
        const qty = Math.max(1, Math.round(normalizeLooseNumber(match[2])));
        const unit = normalizeLooseNumber(match[3]);
        items.push({
          id: genLocalId(),
          descripcion: desc,
          refaccion_id: suggestRefaccionIdByDescription(desc),
          cantidad: qty,
          costo_unitario: unit > 0 ? unit : 0
        });
      }
    });
    return items;
  }
  function renderFacturaMeta() {
    const wrap = document.getElementById('exportInvoiceMeta');
    if (!wrap) return;
    if (!facturaDraft.items.length) {
      wrap.innerHTML = '<p class="text-muted" style="margin:0">Sin factura procesada.</p>';
      return;
    }
    wrap.innerHTML = `
      <div><b>Proveedor:</b> ${safeText(facturaDraft.proveedor || 'â€”')}</div>
      <div><b>Folio:</b> ${safeText(facturaDraft.folio || 'â€”')} | <b>Fecha:</b> ${safeText(facturaDraft.fecha || 'â€”')} | <b>Total detectado:</b> $${Number(facturaDraft.total || 0).toFixed(2)}</div>
      <div class="text-muted" style="margin-top:4px">Confirma los renglones antes de aplicar.</div>
    `;
  }

  function renderFacturaItems() {
    const tbody = document.getElementById('exportInvoiceItemsBody');
    if (!tbody) return;
    if (!facturaDraft.items.length) {
      tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;color:var(--muted);padding:20px">Sin renglones detectados.</td></tr>';
      return;
    }
    const options = '<option value="">â€” Seleccionar â€”</option>' + refacciones
      .map(item => `<option value="${safeText(item.id)}">${safeText(item.nombre)} | ${safeText(item.numero_parte)}</option>`)
      .join('');

    tbody.innerHTML = facturaDraft.items.map((row, idx) => {
      const importe = Number(row.cantidad || 0) * Number(row.costo_unitario || 0);
      return `
        <tr>
          <td><input type="text" value="${safeText(row.descripcion)}" onchange="actualizarItemFacturaRefacciones(${idx}, 'descripcion', this.value)"></td>
          <td><select onchange="actualizarItemFacturaRefacciones(${idx}, 'refaccion_id', this.value)">${options}</select></td>
          <td><input type="number" min="1" value="${Number(row.cantidad || 1)}" onchange="actualizarItemFacturaRefacciones(${idx}, 'cantidad', this.value)"></td>
          <td><input type="number" min="0" step="0.01" value="${Number(row.costo_unitario || 0).toFixed(2)}" onchange="actualizarItemFacturaRefacciones(${idx}, 'costo_unitario', this.value)"></td>
          <td>$${importe.toFixed(2)}</td>
        </tr>
      `;
    }).join('');

    facturaDraft.items.forEach((row, idx) => {
      const select = tbody.querySelectorAll('select')[idx];
      if (select) select.value = row.refaccion_id || '';
    });
  }

  async function procesarFacturaRefacciones() {
    const text = await getFacturaTextInput();
    const file = document.getElementById('exportInvoiceFile')?.files?.[0];
    const fileName = String(file?.name || '').toLowerCase();
    const isTextFile = /\.(txt|csv|json|xml)$/i.test(fileName);
    if (!text) {
      if (file && !isTextFile) {
        facturaDraft = {
          proveedor: '',
          folio: file?.name || '',
          fecha: toDateOnly(file?.lastModified ? new Date(file.lastModified).toISOString() : todayIso()),
          total: 0,
          items: [{
            id: genLocalId(),
            descripcion: '',
            refaccion_id: '',
            cantidad: 1,
            costo_unitario: 0
          }]
        };
        renderFacturaMeta();
        renderFacturaItems();
        alert('El archivo no trae texto extraible en navegador. Ya puedes capturar manualmente el renglon o pegar OCR y volver a procesar.');
        return;
      }
      alert('Sube una factura de texto/CSV/XML o pega texto OCR para procesar.');
      return;
    }
    const meta = detectFacturaMeta(text);
    let items = parseFacturaItems(text);
    if (!items.length) {
      items = [{
        id: genLocalId(),
        descripcion: String(text || '').replace(/\s+/g, ' ').trim().slice(0, 180),
        refaccion_id: '',
        cantidad: 1,
        costo_unitario: 0
      }];
    }
    facturaDraft = {
      proveedor: meta.proveedor,
      folio: meta.folio,
      fecha: meta.fecha,
      total: meta.total,
      items
    };
    renderFacturaMeta();
    renderFacturaItems();
    if (items.length === 1 && !items[0].refaccion_id && Number(items[0].costo_unitario || 0) === 0) {
      alert('Se cargo en modo asistido. Asigna refaccion, cantidad y costo antes de aplicar.');
    }
  }
  function agregarRenglonFacturaRefacciones() {
    facturaDraft.items.push({
      id: genLocalId(),
      descripcion: '',
      refaccion_id: '',
      cantidad: 1,
      costo_unitario: 0
    });
    renderFacturaMeta();
    renderFacturaItems();
  }

  function actualizarItemFacturaRefacciones(index, field, value) {
    const row = facturaDraft.items[index];
    if (!row) return;
    if (field === 'cantidad') row.cantidad = Math.max(1, Math.round(Number(value || 1)));
    else if (field === 'costo_unitario') row.costo_unitario = Math.max(0, Number(value || 0));
    else row[field] = String(value || '').trim();
    renderFacturaItems();
  }

  function limpiarFacturaRefacciones() {
    facturaDraft = { proveedor: '', folio: '', fecha: '', total: 0, items: [] };
    const fileEl = document.getElementById('exportInvoiceFile');
    const txtEl = document.getElementById('exportInvoiceText');
    if (fileEl) fileEl.value = '';
    if (txtEl) txtEl.value = '';
    renderFacturaMeta();
    renderFacturaItems();
  }

  function aplicarFacturaRefacciones() {
    if (!facturaDraft.items.length) {
      alert('No hay renglones para aplicar.');
      return;
    }
    const validRows = facturaDraft.items.filter(item =>
      item.refaccion_id && Number(item.cantidad || 0) > 0
    );
    if (!validRows.length) {
      alert('Asigna una refacciÃ³n y cantidad vÃ¡lida al menos en un renglÃ³n.');
      return;
    }

    const folio = String(facturaDraft.folio || '').trim();
    const proveedor = String(facturaDraft.proveedor || '').trim();
    const referencia = [folio ? `Folio ${folio}` : '', proveedor ? `Prov. ${proveedor}` : ''].filter(Boolean).join(' | ');

    validRows.forEach(item => {
      const ref = refacciones.find(r => r.id === item.refaccion_id);
      if (!ref) return;
      ref.stock_actual += Number(item.cantidad || 0);
      if (Number(item.costo_unitario || 0) > 0) {
        ref.costo = Number(item.costo_unitario || 0);
      }
      const mov = normalizeMovimiento({
        id: genLocalId(),
        refaccion_id: ref.id,
        tipo_movimiento: 'ENTRADA',
        cantidad: Number(item.cantidad || 0),
        unidad: 'FACTURA',
        tecnico: getCurrentUser(),
        fecha: nowIso(),
        observaciones: `Entrada por factura${referencia ? ` | ${referencia}` : ''} | ${item.descripcion || ref.nombre}`
      });
      movimientosInventario.unshift(mov);
      upsertSupabase(TABLE_MOVIMIENTOS, [mov]);
    });

    persistAll();
    upsertSupabase(TABLE_REFACCIONES, refacciones);
    logAuditoria('INVENTARIO REFACCIONES', 'ENTRADA FACTURA', `Entrada automÃ¡tica por factura (${validRows.length} renglÃ³n/renglones)`, '', '');
    renderInventarioRefacciones();
    limpiarFacturaRefacciones();
    alert('Factura aplicada al inventario correctamente.');
  }

  function getTechName() {
    return String(document.getElementById('tecActivoNombre')?.value || '').trim();
  }

  function filterMaintenancesForTech() {
    const tech = getTechName().toLowerCase();
    const rows = maintenanceRows();
    if (!tech) return rows;
    return rows.filter(item => String(item.tecnico || '').toLowerCase().includes(tech));
  }

  function asignarTecnico(entryId, tecnico) {
    const schedule = readArray('at_programacion_mantenimiento');
    const idx = schedule.findIndex(item => String(item?.id || '') === entryId);
    if (idx < 0) return;
    schedule[idx].tecnico = String(tecnico || '').trim();
    writeArray('at_programacion_mantenimiento', schedule);
    logAuditoria('TÃ‰CNICOS', 'ASIGNAR', `TÃ©cnico ${tecnico} asignado a ${schedule[idx].economico}`, schedule[idx].economico, '');
  }

  async function iniciarMantenimientoMovil(entryId) {
    if (window.accionProgramacionMantenimiento) {
      await window.accionProgramacionMantenimiento(entryId, 'INICIAR');
    }
    logAuditoria('TÃ‰CNICOS', 'INICIAR', 'Mantenimiento iniciado desde app mÃ³vil', '', '');
    renderTecnicosMovil();
  }

  async function finalizarMantenimientoMovil(entryId) {
    if (window.accionProgramacionMantenimiento) {
      await window.accionProgramacionMantenimiento(entryId, 'FINALIZAR');
    }
    const row = maintenanceRows().find(item => item.id === entryId);
    descontarRefaccionesAutomaticamente(row?.economico || '', getTechName() || 'TÃ©cnico mÃ³vil');
    logAuditoria('TÃ‰CNICOS', 'FINALIZAR', 'Mantenimiento finalizado desde app mÃ³vil', row?.economico || '', '');
    renderTecnicosMovil();
    renderInventarioRefacciones();
  }

  async function compressImage(file, maxWidth = 1280, quality = 0.72) {
    return new Promise((resolve) => {
      const reader = new FileReader();
      reader.onload = () => {
        const img = new Image();
        img.onload = () => {
          const scale = Math.min(1, maxWidth / img.width);
          const canvas = document.createElement('canvas');
          canvas.width = Math.round(img.width * scale);
          canvas.height = Math.round(img.height * scale);
          const ctx = canvas.getContext('2d');
          ctx.drawImage(img, 0, 0, canvas.width, canvas.height);
          resolve(canvas.toDataURL('image/jpeg', quality));
        };
        img.src = String(reader.result || '');
      };
      reader.readAsDataURL(file);
    });
  }

  async function subirEvidenciaMovil() {
    const unidad = String(document.getElementById('tecEvidUnidad')?.value || '').trim();
    const tipo = String(document.getElementById('tecEvidTipo')?.value || '').trim() || 'Evidencia';
    const files = Array.from(document.getElementById('tecEvidFile')?.files || []);
    if (!unidad || !files.length) {
      alert('Captura unidad y al menos una imagen.');
      return;
    }

    const tecnico = getTechName() || getCurrentUser();
    const payloads = [];
    for (const file of files.slice(0, 10)) {
      const compressed = await compressImage(file);
      payloads.push({
        id: genLocalId(),
        unidad,
        tipo,
        tecnico,
        fecha: nowIso(),
        fileName: file.name,
        mimeType: file.type || 'image/jpeg',
        imageData: compressed
      });
    }
    evidenciasMovil = [...payloads, ...evidenciasMovil].slice(0, 1000);
    writeArray(EVIDENCIAS_MOVIL_KEY, evidenciasMovil);
    logAuditoria('TÃ‰CNICOS', 'SUBIR EVIDENCIA', `${payloads.length} evidencia(s) mÃ³vil(es)`, unidad, payloads.map(p => p.id).join(','));
    renderTecnicosMovil();
  }

  function renderEvidenciasMovil() {
    const wrap = document.getElementById('tecEvidenciasList');
    if (!wrap) return;
    if (!evidenciasMovil.length) {
      wrap.innerHTML = '<p class="text-muted">Sin evidencias.</p>';
      return;
    }
    wrap.innerHTML = evidenciasMovil.slice(0, 30).map(item => `
      <div class="adv-pill" style="display:flex;gap:10px;align-items:center">
        <img src="${safeText(item.imageData)}" class="adv-qr-preview" alt="Evidencia">
        <div>
          <b>${safeText(item.unidad)}</b> | ${safeText(item.tipo)}<br>
          ${safeText(typeof formatDateTime === 'function' ? formatDateTime(item.fecha) : item.fecha)} | ${safeText(item.tecnico)}
        </div>
      </div>
    `).join('');
  }

  function renderTecnicosMovil() {
    const board = document.getElementById('tecBoard');
    if (!board) return;
    const rows = filterMaintenancesForTech();
    board.innerHTML = rows.length
      ? rows.map(row => `
        <div class="adv-tec-card">
          <div class="adv-tec-title">${safeText(row.economico)} | ${safeText(row.planta || 'SIN PLANTA')}</div>
          <div style="font-size:12px;margin-bottom:8px">Estado: <b>${safeText(row.estado)}</b></div>
          <div style="font-size:12px;margin-bottom:10px">TÃ©cnico: <input type="text" value="${safeText(row.tecnico || '')}" onchange="asignarTecnico('${safeText(row.id)}', this.value)"></div>
          <div class="flex-gap">
            <button class="btn btn-secondary" type="button" onclick="iniciarMantenimientoMovil('${safeText(row.id)}')">INICIAR</button>
            <button class="btn btn-secondary" type="button" onclick="finalizarMantenimientoMovil('${safeText(row.id)}')">FINALIZAR</button>
          </div>
        </div>
      `).join('')
      : '<p class="text-muted">Sin mantenimientos para el tÃ©cnico seleccionado.</p>';
    renderEvidenciasMovil();
  }

  function buildQRPayloadFromRecord(rec, at) {
    const payload = {
      record_id: rec.id,
      autotanque: at?.econ || '',
      componente: `${rec.partNo || ''} ${rec.partDesc || ''}`.trim(),
      serie: rec.serial || '',
      fecha_instalacion: rec.instDate || '',
      fecha_reemplazo: rec.replDate || '',
      estado: typeof statusKey === 'function' ? statusKey(typeof daysUntil === 'function' ? daysUntil(rec.replDate) : null) : '',
      historial: `Expediente/${at?.id || ''}/${rec.id}`
    };
    return JSON.stringify(payload);
  }

  function qrImageUrl(payload) {
    return `https://api.qrserver.com/v1/create-qr-code/?size=240x240&margin=5&data=${encodeURIComponent(payload)}`;
  }

  function generarQRComponente(recordId) {
    const rec = (Array.isArray(records) ? records : []).find(item => item.id === recordId);
    if (!rec) return null;
    const at = (Array.isArray(autotanques) ? autotanques : []).find(item => item.id === rec.atId);
    const payload = buildQRPayloadFromRecord(rec, at);
    const qrItem = normalizeQRItem({
      id: genLocalId(),
      record_id: rec.id,
      unidad_id: rec.atId,
      economico: at?.econ || '',
      componente: `${rec.partNo || ''} ${rec.partDesc || ''}`.trim(),
      serie: rec.serial || '',
      fecha_instalacion: rec.instDate || '',
      fecha_reemplazo: rec.replDate || '',
      estado: typeof statusKey === 'function' ? statusKey(typeof daysUntil === 'function' ? daysUntil(rec.replDate) : null) : '',
      payload,
      qr_url: qrImageUrl(payload),
      created_at: nowIso()
    });
    qrComponentes = [qrItem, ...qrComponentes.filter(item => item.record_id !== rec.id)];
    writeArray(QR_COMPONENTES_KEY, qrComponentes);
    logAuditoria('QR COMPONENTES', 'GENERAR', `QR generado para ${qrItem.economico} - ${qrItem.componente}`, qrItem.economico, qrItem.id);
    return qrItem;
  }

  function imprimirQR(qrId) {
    const qr = qrComponentes.find(item => item.id === qrId);
    if (!qr) return alert('No se encontrÃ³ QR.');
    const popup = window.open('', '_blank', 'width=420,height=540');
    if (!popup) return alert('Permite ventanas emergentes para imprimir QR.');
    popup.document.write(`<!doctype html><html><head><title>QR ${safeText(qr.economico)}</title><style>
      body{font-family:Arial,sans-serif;padding:20px;text-align:center}
      .tag{width:320px;border:1px solid #333;padding:12px;margin:auto}
      img{width:240px;height:240px;object-fit:contain}
      .meta{font-size:12px;text-align:left;line-height:1.5}
    </style></head><body>
      <div class="tag">
        <h3>${safeText(qr.economico)}</h3>
        <img src="${safeText(qr.qr_url)}" alt="QR">
        <div class="meta">
          <b>Componente:</b> ${safeText(qr.componente)}<br>
          <b>Serie:</b> ${safeText(qr.serie || 'â€”')}<br>
          <b>InstalaciÃ³n:</b> ${safeText(typeof formatDate === 'function' ? formatDate(qr.fecha_instalacion) : qr.fecha_instalacion)}<br>
          <b>Reemplazo:</b> ${safeText(typeof formatDate === 'function' ? formatDate(qr.fecha_reemplazo) : qr.fecha_reemplazo)}
        </div>
      </div>
      <script>window.onload=function(){window.print();}</script>
    </body></html>`);
    popup.document.close();
  }

  function exportarPDFQR(qrId) {
    imprimirQR(qrId);
  }

  async function escanearQR() {
    const resultEl = document.getElementById('tecQrResult');
    if (!resultEl) return;
    if (!('BarcodeDetector' in window)) {
      resultEl.innerHTML = '<p class="text-muted">El navegador no soporta BarcodeDetector. Pega el payload manual para abrir historial.</p>';
      return;
    }
    try {
      const stream = await navigator.mediaDevices.getUserMedia({ video: { facingMode: 'environment' } });
      const video = document.createElement('video');
      video.srcObject = stream;
      video.play();
      await new Promise(resolve => setTimeout(resolve, 1400));
      const canvas = document.createElement('canvas');
      canvas.width = video.videoWidth || 1280;
      canvas.height = video.videoHeight || 720;
      canvas.getContext('2d').drawImage(video, 0, 0);
      stream.getTracks().forEach(track => track.stop());
      const detector = new window.BarcodeDetector({ formats: ['qr_code'] });
      const detections = await detector.detect(canvas);
      if (!detections.length) {
        resultEl.innerHTML = '<p class="text-muted">No se detectÃ³ QR en la captura.</p>';
        return;
      }
      const qrText = String(detections[0].rawValue || '').trim();
      resultEl.innerHTML = `<p>QR detectado:</p><pre style="white-space:pre-wrap">${safeText(qrText)}</pre>`;
      abrirHistorialQR(qrText);
    } catch (error) {
      resultEl.innerHTML = `<p class="text-muted">No fue posible escanear QR: ${safeText(error?.message || error)}</p>`;
    }
  }

  function openModalLike(title, html) {
    const modal = document.getElementById('modalDashboardDrill');
    const titleEl = document.getElementById('modalDashboardDrillTitle');
    const bodyEl = document.getElementById('modalDashboardDrillBody');
    if (!modal || !titleEl || !bodyEl) return alert('No se pudo abrir el detalle QR.');
    titleEl.textContent = title;
    bodyEl.innerHTML = html;
    modal.classList.add('open');
  }

  function abrirHistorialQR(qrPayload) {
    if (!qrPayload) return;
    let parsed;
    try {
      parsed = JSON.parse(String(qrPayload));
    } catch {
      const byRaw = qrComponentes.find(item => item.payload === qrPayload);
      if (byRaw) parsed = JSON.parse(byRaw.payload);
    }
    if (!parsed) {
      alert('No se pudo interpretar el contenido QR.');
      return;
    }
    const rec = (Array.isArray(records) ? records : []).find(item => item.id === parsed.record_id);
    if (!rec) {
      openModalLike('Historial QR', '<p class="text-muted">No se encontrÃ³ el componente en historial local.</p>');
      return;
    }
    const at = (Array.isArray(autotanques) ? autotanques : []).find(item => item.id === rec.atId);
    openModalLike(
      `Historial QR - ${parsed.autotanque || at?.econ || 'Unidad'}`,
      `<div class="grid-2">
        <div class="detail-row"><span class="detail-key">AUTOTANQUE:</span><span class="detail-val">${safeText(parsed.autotanque || at?.econ || 'â€”')}</span></div>
        <div class="detail-row"><span class="detail-key">COMPONENTE:</span><span class="detail-val">${safeText(parsed.componente || 'â€”')}</span></div>
        <div class="detail-row"><span class="detail-key">SERIE:</span><span class="detail-val">${safeText(parsed.serie || 'â€”')}</span></div>
        <div class="detail-row"><span class="detail-key">INSTALACIÃ“N:</span><span class="detail-val">${safeText(typeof formatDate === 'function' ? formatDate(parsed.fecha_instalacion) : parsed.fecha_instalacion || 'â€”')}</span></div>
        <div class="detail-row"><span class="detail-key">REEMPLAZO:</span><span class="detail-val">${safeText(typeof formatDate === 'function' ? formatDate(parsed.fecha_reemplazo) : parsed.fecha_reemplazo || 'â€”')}</span></div>
        <div class="detail-row"><span class="detail-key">ESTADO:</span><span class="detail-val">${safeText(parsed.estado || 'â€”')}</span></div>
      </div>
      <div class="section-sep"></div>
      <div class="detail-row"><span class="detail-key">NOTAS:</span><span class="detail-val">${safeText(rec.notes || 'â€”')}</span></div>`
    );
  }

  function renderQRComponentes() {
    const search = String(document.getElementById('qrSearch')?.value || '').toLowerCase();
    const tbody = document.getElementById('qrBody');
    if (!tbody) return;
    const rows = qrComponentes.filter(item => {
      const bag = `${item.economico} ${item.componente} ${item.serie}`.toLowerCase();
      return !search || bag.includes(search);
    });
    tbody.innerHTML = rows.length
      ? rows.map(item => `
        <tr>
          <td>${safeText(item.economico || 'â€”')}</td>
          <td>${safeText(item.componente || 'â€”')}</td>
          <td>${safeText(item.serie || 'â€”')}</td>
          <td>${safeText(typeof formatDate === 'function' ? formatDate(item.fecha_instalacion) : item.fecha_instalacion || 'â€”')}</td>
          <td>${safeText(typeof formatDate === 'function' ? formatDate(item.fecha_reemplazo) : item.fecha_reemplazo || 'â€”')}</td>
          <td>${safeText(item.estado || 'â€”')}</td>
          <td><img class="adv-qr-preview" src="${safeText(item.qr_url)}" alt="QR"></td>
          <td>
            <div class="flex-gap">
              <button class="btn btn-secondary" style="padding:4px 8px;font-size:10px" onclick="imprimirQR('${safeText(item.id)}')">IMPRIMIR QR</button>
              <button class="btn btn-secondary" style="padding:4px 8px;font-size:10px" onclick="exportarPDFQR('${safeText(item.id)}')">PDF QR</button>
              <button class="btn btn-secondary" style="padding:4px 8px;font-size:10px" onclick="abrirHistorialQRPorId('${safeText(item.id)}')">VER HISTORIAL</button>
            </div>
          </td>
        </tr>
      `).join('')
      : '<tr><td colspan="8" style="text-align:center;color:var(--muted);padding:20px">Sin QR generados.</td></tr>';
  }

  function abrirHistorialQRPorId(qrId) {
    const qr = qrComponentes.find(item => item.id === qrId);
    if (!qr) return alert('No se encontrÃ³ QR.');
    abrirHistorialQR(qr.payload);
  }

  function getFugasReportadas() {
    return readArray(FUGAS_KEY).map(item => String(item || '').trim()).filter(Boolean);
  }

  function saveProgramacionRows(rows) {
    writeArray('at_programacion_mantenimiento', rows);
  }

  function evaluarBloqueoUnidad() {
    const rows = maintenanceRows();
    const fugas = new Set(getFugasReportadas());
    const now = todayIso();
    const scheduled = readArray('at_programacion_mantenimiento');
    const nextBloqueos = [];

    (Array.isArray(autotanques) ? autotanques : []).forEach(at => {
      const atRecords = (Array.isArray(records) ? records : []).filter(rec => rec.atId === at.id);
      const criticos = atRecords.filter(rec => {
        const d = typeof daysUntil === 'function' ? daysUntil(rec.replDate) : null;
        return d !== null && d >= 0 && d <= 90;
      });
      const vencidos = atRecords.filter(rec => {
        const d = typeof daysUntil === 'function' ? daysUntil(rec.replDate) : null;
        return d !== null && d < 0;
      });
      const criticalValveExpired = vencidos.some(rec =>
        String(rec.partDesc || '').toLowerCase().includes('valvula')
      );
      const multipleCritical = criticos.length >= 3;
      const maintenanceOverdue = rows.some(row => row.unidad_id === at.id && row.estado === 'PROGRAMADO' && row.fecha_inicio && row.fecha_inicio < now);
      const riskOver80 = rows.some(row => row.unidad_id === at.id && Number(row.riesgo_operativo || 0) > 80);
      const leak = fugas.has(at.id) || fugas.has(String(at.econ || '').trim());

      if (criticalValveExpired || multipleCritical || maintenanceOverdue || riskOver80 || leak) {
        const motivo = [
          criticalValveExpired ? 'VÃ¡lvula crÃ­tica vencida' : '',
          multipleCritical ? 'MÃºltiples componentes crÃ­ticos' : '',
          maintenanceOverdue ? 'Mantenimiento vencido' : '',
          riskOver80 ? 'Riesgo operativo > 80' : '',
          leak ? 'Fuga reportada' : ''
        ].filter(Boolean).join(' | ');
        nextBloqueos.push(normalizeBloqueo({
          id: genLocalId(),
          unidad_id: at.id,
          economico: at.econ,
          motivo,
          fecha_bloqueo: nowIso(),
          responsable: getCurrentUser(),
          estado: 'FUERA DE SERVICIO'
        }));
      }
    });

    const keyByUnit = new Map(bloqueosUnidades.map(item => [item.unidad_id, item]));
    nextBloqueos.forEach(item => {
      if (!keyByUnit.has(item.unidad_id)) {
        bloqueosUnidades.unshift(item);
        logAuditoria('BLOQUEOS', 'BLOQUEAR', `Unidad bloqueada: ${item.motivo}`, item.economico, '');
      }
    });

    const blockedSet = new Set(bloqueosUnidades.map(item => item.unidad_id));
    const updatedSchedule = scheduled.map(item => blockedSet.has(String(item.unidad_id || '').trim())
      ? { ...item, estado: 'FUERA DE SERVICIO' }
      : item
    );
    saveProgramacionRows(updatedSchedule);
    persistAll();
    renderBloqueos();
  }

  function bloquearUnidadAutomaticamente(unidadId, motivo, responsable = 'Sistema') {
    const at = (Array.isArray(autotanques) ? autotanques : []).find(item => item.id === unidadId);
    if (!at) return;
    if (bloqueosUnidades.some(item => item.unidad_id === unidadId)) return;
    const bloq = normalizeBloqueo({
      unidad_id: unidadId,
      economico: at.econ,
      motivo,
      fecha_bloqueo: nowIso(),
      responsable,
      estado: 'FUERA DE SERVICIO'
    });
    bloqueosUnidades.unshift(bloq);
    const schedule = readArray('at_programacion_mantenimiento').map(item =>
      String(item?.unidad_id || '') === unidadId ? { ...item, estado: 'FUERA DE SERVICIO' } : item
    );
    saveProgramacionRows(schedule);
    persistAll();
    logAuditoria('BLOQUEOS', 'BLOQUEAR', motivo, at.econ, '');
    renderBloqueos();
  }

  function liberarUnidad(unidadId) {
    const at = (Array.isArray(autotanques) ? autotanques : []).find(item => item.id === unidadId);
    if (!at) return;
    const token = prompt('AutorizaciÃ³n requerida. Escribe AUTORIZAR-LIBERACION');
    if (token !== 'AUTORIZAR-LIBERACION') {
      alert('No autorizado.');
      return;
    }
    bloqueosUnidades = bloqueosUnidades.filter(item => item.unidad_id !== unidadId);
    const schedule = readArray('at_programacion_mantenimiento').map(item =>
      String(item?.unidad_id || '') === unidadId && String(item?.estado || '').toUpperCase() === 'FUERA DE SERVICIO'
        ? { ...item, estado: 'LIBERADO' }
        : item
    );
    saveProgramacionRows(schedule);
    persistAll();
    logAuditoria('BLOQUEOS', 'LIBERAR', 'Unidad liberada manualmente con autorizaciÃ³n', at.econ, '');
    renderBloqueos();
  }

  function renderBloqueos() {
    const tbody = document.getElementById('bloqueosBody');
    if (!tbody) return;
    tbody.innerHTML = bloqueosUnidades.length
      ? bloqueosUnidades.map(item => `
        <tr>
          <td>${safeText(item.economico || 'â€”')}</td>
          <td>${safeText(item.motivo || 'â€”')}</td>
          <td>${safeText(typeof formatDateTime === 'function' ? formatDateTime(item.fecha_bloqueo) : item.fecha_bloqueo)}</td>
          <td>${safeText(item.responsable || 'Sistema')}</td>
          <td><span class="adv-status-tag adv-status-danger">FUERA DE SERVICIO</span></td>
          <td><button class="btn btn-secondary" style="padding:4px 8px;font-size:10px" onclick="liberarUnidad('${safeText(item.unidad_id)}')">LIBERAR</button></td>
        </tr>
      `).join('')
      : '<tr><td colspan="6" style="text-align:center;color:var(--muted);padding:20px">Sin unidades bloqueadas.</td></tr>';
  }

  function renderAuditoriaSistema() {
    const tbody = document.getElementById('auditBody');
    if (!tbody) return;
    const q = String(document.getElementById('auditSearch')?.value || '').toLowerCase();
    const rows = auditoriaSistema.filter(item => {
      const bag = `${item.usuario} ${item.modulo} ${item.accion} ${item.descripcion} ${item.unidad}`.toLowerCase();
      return !q || bag.includes(q);
    });
    tbody.innerHTML = rows.length
      ? rows.slice(0, 500).map(item => `
        <tr>
          <td>${safeText(typeof formatDateTime === 'function' ? formatDateTime(item.fecha) : item.fecha)}</td>
          <td>${safeText(item.usuario)}</td>
          <td>${safeText(item.modulo)}</td>
          <td>${safeText(item.accion)}</td>
          <td>${safeText(item.unidad || 'â€”')}</td>
          <td style="white-space:pre-wrap;overflow-wrap:anywhere">${safeText(item.descripcion)}</td>
        </tr>
      `).join('')
      : '<tr><td colspan="6" style="text-align:center;color:var(--muted);padding:20px">Sin eventos de auditorÃ­a.</td></tr>';
  }

function patchProgramacionActions() {
    const tryPatch = () => {
      if (!window.accionProgramacionMantenimiento || window.accionProgramacionMantenimiento.__patched) {
        setTimeout(tryPatch, 400);
        return;
      }
      const original = window.accionProgramacionMantenimiento;
      window.accionProgramacionMantenimiento = async function patchedAccion(entryId, accion) {
        const actionUpper = String(accion || '').toUpperCase();
        await original(entryId, accion);
        const refreshed = readArray('at_programacion_mantenimiento');
        const current = refreshed.find(item => String(item?.id || '') === String(entryId || ''));
        if (actionUpper === 'FINALIZAR') {
          descontarRefaccionesAutomaticamente(current?.economico || '', current?.tecnico || getCurrentUser());
          generarAlertaInventario();
        }
      };
      window.accionProgramacionMantenimiento.__patched = true;
    };
    tryPatch();
  }

  function patchComponentSaveForQR() {
    const tryPatch = () => {
      if (!window.saveComponentRecord || window.saveComponentRecord.__patchedQR) {
        setTimeout(tryPatch, 400);
        return;
      }
      const original = window.saveComponentRecord;
      window.saveComponentRecord = async function patchedSaveComponentRecord(...args) {
        const beforeIds = new Set((Array.isArray(records) ? records : []).map(item => item.id));
        await original.apply(this, args);
        const created = (Array.isArray(records) ? records : []).filter(item => !beforeIds.has(item.id));
        created.forEach(rec => generarQRComponente(rec.id));
        if (created.length) renderQRComponentes();
      };
      window.saveComponentRecord.__patchedQR = true;
    };
    tryPatch();
  }

  function initMobileMode() {
    const check = () => {
      const isMobile = window.matchMedia('(max-width: 760px)').matches;
      document.body.classList.toggle('adv-mobile-mode', isMobile);
    };
    check();
    window.addEventListener('resize', check);
  }

  function bootstrapAdvancedModules() {
    bindTabBridge();
    initializePlantFilter();
    initMobileMode();
    patchProgramacionActions();
    if (window.openProgramacionTab && !window.openProgramacionTab.__patchedExt) {
      const originalOpenProgramacion = window.openProgramacionTab;
      window.openProgramacionTab = function patchedOpenProgramacion(...args) {
        removeExtendedTabSelection();
        return originalOpenProgramacion.apply(this, args);
      };
      window.openProgramacionTab.__patchedExt = true;
    }
    renderCronogramaGantt();
    renderInventarioRefacciones();
    renderFacturaMeta();
    renderFacturaItems();
  }

  document.addEventListener('DOMContentLoaded', () => {
    const wait = () => {
      if (typeof appBootstrapped !== 'undefined' && appBootstrapped) {
        bootstrapAdvancedModules();
        return;
      }
      setTimeout(wait, 450);
    };
    wait();
  });

  window.openAdvancedTab = openAdvancedTab;
  window.renderCronogramaGantt = renderCronogramaGantt;
  window.calcularDuracionMantenimiento = calcularDuracionMantenimiento;
  window.actualizarAvanceMantenimiento = actualizarAvanceMantenimiento;
  window.guardarRefaccion = guardarRefaccion;
  window.limpiarRefaccionForm = limpiarRefaccionForm;
  window.registrarMovimientoInventario = registrarMovimientoInventario;
  window.descontarRefaccionesAutomaticamente = descontarRefaccionesAutomaticamente;
  window.validarStockDisponible = validarStockDisponible;
  window.generarAlertaInventario = generarAlertaInventario;
  window.renderInventarioRefacciones = renderInventarioRefacciones;
  window.procesarFacturaRefacciones = procesarFacturaRefacciones;
  window.agregarRenglonFacturaRefacciones = agregarRenglonFacturaRefacciones;
  window.actualizarItemFacturaRefacciones = actualizarItemFacturaRefacciones;
  window.aplicarFacturaRefacciones = aplicarFacturaRefacciones;
  window.limpiarFacturaRefacciones = limpiarFacturaRefacciones;
})();


