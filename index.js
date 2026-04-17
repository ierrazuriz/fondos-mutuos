process.on('uncaughtException', (err) => {
  console.error('[FATAL] uncaughtException:', err.message, err.stack);
  process.exit(1);
});
process.on('unhandledRejection', (reason) => {
  console.error('[FATAL] unhandledRejection:', reason);
});

const express = require('express');
const cors = require('cors');
const path = require('path');
const { spawn } = require('child_process');
const multer = require('multer');
const XLSX = require('xlsx');
const { fetchDailyData } = require('./scraper');
const {
  getCachedData, saveData, listDates, saveMonthly, getMonthly, getMonthlyHistory,
  getLatestBci, saveBci,
  getHistorialArchivos, saveHistorialArchivo, getOperacionesByHistorial,
  deleteHistorialArchivo, getBalanceAcciones, upsertAjusteBalance, deleteAjusteBalance,
  saveFetchDataCache, getFetchDataCache, hasFetchDataCache,
} = require('./db');

const upload = multer({ storage: multer.memoryStorage() });

const DEFAULT_CATS = [
  'Accionario Nacional',
  'Accionario Nacional Large Cap',
  'Accionario Nacional Otros',
  'Accionario Nacional Small & Mid Cap',
  'Inversionistas Calificados Accionario Nacional',
];

// ─── Helpers ──────────────────────────────────────────────────────────────
function getYesterday() {
  const d = new Date();
  d.setDate(d.getDate() - 1);
  return d.toISOString().split('T')[0];
}

function parseChilean(s) {
  if (!s || s === '-') return 0;
  return parseFloat(String(s).replace(/\./g, '').replace(',', '.')) || 0;
}

function aggregateRows(rows, headers, cats) {
  const aportesH  = (headers || []).find((h) => h.includes('Flujo Aporte'));
  const rescatesH = (headers || []).find((h) => h.includes('Flujo Rescate'));
  const filtered  = (rows || []).filter((r) => cats.includes(r['Categoría AFM'] || ''));
  const aportes   = filtered.reduce((s, r) => s + parseChilean(r[aportesH]  || '0'), 0);
  const rescates  = filtered.reduce((s, r) => s + parseChilean(r[rescatesH] || '0'), 0);
  return { aportes, rescates, netFlow: aportes - rescates, count: filtered.length };
}

/** Returns ISO working days (Mon-Fri) for a given year/month up to yesterday */
function workingDaysOf(year, month) {
  const yesterday = getYesterday();
  const days = [];
  const d = new Date(year, month - 1, 1);
  while (d.getMonth() === month - 1) {
    const iso = d.toISOString().split('T')[0];
    if (d.getDay() !== 0 && d.getDay() !== 6) days.push(iso);
    d.setDate(d.getDate() + 1);
  }
  return { all: days, past: days.filter((iso) => iso <= yesterday) };
}

/** Calculate monthly totals from daily cache */
function calcMonthlySummary(year, month, cats = DEFAULT_CATS) {
  const { all, past } = workingDaysOf(year, month);
  let aportes = 0, rescates = 0, loaded = 0;
  for (const date of past) {
    const cached = getCachedData(date);
    if (!cached) continue;
    const agg = aggregateRows(cached.data.rows, cached.data.headers, cats);
    aportes  += agg.aportes;
    rescates += agg.rescates;
    loaded++;
  }
  return { aportes, rescates, netFlow: aportes - rescates, daysCount: loaded, workingDays: all.length };
}

// ─── App ──────────────────────────────────────────────────────────────────
const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public'), {
  setHeaders: (res, filePath) => {
    if (filePath.endsWith('.html')) {
      res.setHeader('Cache-Control', 'no-store');
    }
  }
}));

// GET / — serve the main HTML app
app.get('/', (_req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

// GET /api/daily?date=YYYY-MM-DD
app.get('/api/daily', async (req, res) => {
  const date = req.query.date || getYesterday();
  const cached = getCachedData(date);
  if (cached) {
    return res.json({ date, source: 'cache', fetched_at: cached.fetched_at, ...cached.data });
  }
  try {
    const result = await fetchDailyData(date);
    saveData(date, result);
    res.json({ date, source: 'aafm', fetched_at: new Date().toISOString(), ...result });
  } catch (err) {
    console.error('[api] Error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/refresh?date=YYYY-MM-DD
app.post('/api/refresh', async (req, res) => {
  const date = req.query.date || req.body?.date || getYesterday();
  try {
    const result = await fetchDailyData(date);
    saveData(date, result);
    // Recalculate monthly summary for this date's month
    const [y, m] = date.split('-').map(Number);
    const summary = calcMonthlySummary(y, m);
    if (summary.daysCount > 0) saveMonthly(`${y}-${String(m).padStart(2,'0')}`, summary);
    res.json({ date, source: 'aafm', fetched_at: new Date().toISOString(), ...result });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/dates
app.get('/api/dates', (_req, res) => res.json(listDates()));

// GET /api/monthly-summary?year=YYYY&month=MM&cats=Cat1|Cat2
app.get('/api/monthly-summary', (req, res) => {
  const now   = new Date();
  const year  = parseInt(req.query.year)  || now.getFullYear();
  const month = parseInt(req.query.month) || (now.getMonth() + 1);
  const cats  = req.query.cats ? req.query.cats.split('|') : DEFAULT_CATS;

  const { past } = workingDaysOf(year, month);
  const result = past.map((date) => {
    const cached = getCachedData(date);
    if (!cached) return { date, loaded: false };
    return { date, loaded: true, ...aggregateRows(cached.data.rows, cached.data.headers, cats) };
  });
  res.json(result);
});

// GET /api/monthly-history?months=12
app.get('/api/monthly-history', (req, res) => {
  const months = Math.min(parseInt(req.query.months) || 12, 24);
  res.json(getMonthlyHistory(months));
});

// GET /api/backfill?months=12  — fetch missing historical days from AAFM (SSE stream)
app.get('/api/backfill', async (req, res) => {
  const months = Math.min(parseInt(req.query.months) || 12, 24);

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const send = (obj) => { if (!res.writableEnded) res.write(`data: ${JSON.stringify(obj)}\n\n`); };

  try {
    const now = new Date();
    const yesterday = getYesterday();
    const days = new Set();
    for (let i = months; i >= 0; i--) {
      const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
      const { past } = workingDaysOf(d.getFullYear(), d.getMonth() + 1);
      past.forEach(day => days.add(day));
    }
    const allDays  = [...days].filter(d => d <= yesterday).sort();
    const toFetch  = allDays.filter(d => !getCachedData(d));
    const already  = allDays.length - toFetch.length;

    send({ type: 'start', total: toFetch.length, already });
    if (!toFetch.length) { send({ type: 'done', done: 0, total: 0 }); return res.end(); }

    let done = 0;
    const CONCURRENCY = 4;
    for (let i = 0; i < toFetch.length; i += CONCURRENCY) {
      if (res.writableEnded) break;
      const batch = toFetch.slice(i, i + CONCURRENCY);
      await Promise.all(batch.map(date =>
        fetchDailyData(date)
          .then(result => { saveData(date, result); })
          .catch(() => {/* skip days with no AAFM data (holidays, etc.) */})
          .finally(() => { done++; send({ type: 'progress', done, total: toFetch.length, date }); })
      ));
    }

    // Recalculate monthly summaries from newly cached data
    for (let i = months; i >= 0; i--) {
      const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
      const y = d.getFullYear(), m = d.getMonth() + 1;
      const ym = `${y}-${String(m).padStart(2,'0')}`;
      const summary = calcMonthlySummary(y, m);
      if (summary.daysCount > 0) saveMonthly(ym, summary);
    }

    send({ type: 'done', done, total: toFetch.length });
  } catch (err) {
    send({ type: 'error', message: err.message });
  }
  res.end();
});

// POST /api/recalc-history  — recalculate stored monthly summaries from cache
app.post('/api/recalc-history', (req, res) => {
  const now = new Date();
  const results = [];
  for (let i = 1; i <= 12; i++) {
    const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
    const y = d.getFullYear(), m = d.getMonth() + 1;
    const ym = `${y}-${String(m).padStart(2,'0')}`;
    const summary = calcMonthlySummary(y, m);
    if (summary.daysCount > 0) {
      saveMonthly(ym, summary);
      results.push({ year_month: ym, ...summary });
    }
  }
  res.json({ recalculated: results.length, results });
});

// ─── AYR endpoints (emf-ffmm compatible) ─────────────────────────────────

/** Build the full AYR dataset: one row per working day with cumulative totals */
function buildFetchData(cats) {
  const useCats = cats || DEFAULT_CATS;
  const dates = listDates().map(r => r.date).reverse(); // oldest first
  const rows = [];
  let acumAportes = 0, acumRescates = 0;

  for (const date of dates) {
    const cached = getCachedData(date);
    const yr = date.substring(0, 4);
    if (rows.length === 0 || yr !== rows[rows.length-1].fecha.substring(0, 4)) { acumAportes = 0; acumRescates = 0; }
    if (!cached) continue;
    const agg = aggregateRows(cached.data.rows, cached.data.headers, useCats);
    acumAportes  += agg.aportes;
    acumRescates += agg.rescates;
    rows.push({
      id: rows.length + 1,
      fecha: date,
      flujo_aportes: agg.aportes,
      flujo_rescates: agg.rescates,
      neto_aportes_rescates: agg.netFlow,
      acumulado_aportes: acumAportes,
      acumulado_rescates: acumRescates,
      neto_acumulado: acumAportes - acumRescates,
    });
  }
  return rows;
}

// GET /api/fetch-data
app.get('/api/fetch-data', (_req, res) => {
  try {
    if (hasFetchDataCache()) {
      res.json(getFetchDataCache());
    } else {
      res.json(buildFetchData());
    }
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/seed-fetch-data — seed fetch-data cache from reference
app.post('/api/seed-fetch-data', (req, res) => {
  try {
    const rows = req.body;
    if (!Array.isArray(rows) || rows.length === 0) {
      return res.status(400).json({ error: 'Expected array of rows' });
    }
    saveFetchDataCache(rows);
    res.json({ ok: true, count: rows.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/update/:date — refresh a single date
app.get('/api/update/:date', async (req, res) => {
  const date = req.params.date;
  try {
    const result = await fetchDailyData(date);
    saveData(date, result);
    const [y, m] = date.split('-').map(Number);
    const summary = calcMonthlySummary(y, m);
    if (summary.daysCount > 0) saveMonthly(`${y}-${String(m).padStart(2,'0')}`, summary);
    res.json({ ok: true, date, rows: result.rows.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/updatefrom/:date — refresh all dates from a given date up to yesterday
app.get('/api/updatefrom/:date', async (req, res) => {
  const fromDate = req.params.date;
  const yesterday = getYesterday();
  try {
    // Collect all working days from fromDate to yesterday
    const days = [];
    const d = new Date(fromDate + 'T12:00:00Z');
    const end = new Date(yesterday + 'T12:00:00Z');
    while (d <= end) {
      const dow = d.getUTCDay();
      if (dow >= 1 && dow <= 5) days.push(d.toISOString().split('T')[0]);
      d.setUTCDate(d.getUTCDate() + 1);
    }

    const CONCURRENCY = 15;
    let fetched = 0;
    for (let i = 0; i < days.length; i += CONCURRENCY) {
      const batch = days.slice(i, i + CONCURRENCY);
      await Promise.all(batch.map(date =>
        fetchDailyData(date)
          .then(result => { saveData(date, result); fetched++; })
          .catch(() => {})
      ));
    }

    // Recalculate monthly summaries
    const monthsSeen = new Set(days.map(d => d.substring(0, 7)));
    for (const ym of monthsSeen) {
      const [y, m] = ym.split('-').map(Number);
      const summary = calcMonthlySummary(y, m);
      if (summary.daysCount > 0) saveMonthly(ym, summary);
    }

    res.json({ ok: true, message: `${fetched} días actualizados desde ${fromDate}`, fetched });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/download-excel — download AYR data as Excel
app.get('/api/download-excel', (_req, res) => {
  try {
    const rows = buildFetchData();
    const wsData = [
      ['ID', 'Fecha', 'Flujo Aportes', 'Flujo Rescates', 'Neto A-R', 'Acum. Aportes', 'Acum. Rescates', 'Neto Acumulado'],
      ...rows.map(r => [r.id, r.fecha, r.flujo_aportes, r.flujo_rescates, r.neto_aportes_rescates, r.acumulado_aportes, r.acumulado_rescates, r.neto_acumulado]),
    ];
    const wb = XLSX.utils.book_new();
    const ws = XLSX.utils.aoa_to_sheet(wsData);
    XLSX.utils.book_append_sheet(wb, ws, 'Aportes y Rescates');
    const buf = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', 'attachment; filename="Aportes_y_Rescates.xlsx"');
    res.send(buf);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── Balance Acciones API ─────────────────────────────────────────────────

// GET /api/balance-acciones
app.get('/api/balance-acciones', (_req, res) => {
  try {
    res.json(getBalanceAcciones());
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/historial-archivos
app.get('/api/historial-archivos', (_req, res) => {
  try {
    res.json(getHistorialArchivos());
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/historial-operaciones/:id
app.get('/api/historial-operaciones/:id', (req, res) => {
  try {
    res.json(getOperacionesByHistorial(Number(req.params.id)));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/historial-archivos/:id
app.delete('/api/historial-archivos/:id', (req, res) => {
  try {
    deleteHistorialArchivo(Number(req.params.id));
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/save-operaciones — receives FormData: archivo (file), operaciones (json string), nombreArchivo
app.post('/api/save-operaciones', upload.single('archivo'), (req, res) => {
  try {
    const nombreArchivo = req.body.nombreArchivo || (req.file ? req.file.originalname : 'archivo.csv');
    const operaciones = JSON.parse(req.body.operaciones || '[]');
    const id = saveHistorialArchivo(nombreArchivo, 'csv', operaciones);
    res.json({ ok: true, id, saved: operaciones.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/upload-balance-base — receives FormData: archivo (Excel)
app.post('/api/upload-balance-base', upload.single('archivo'), (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'No file received' });
    const wb = XLSX.read(req.file.buffer, { type: 'buffer' });
    const ws = wb.Sheets[wb.SheetNames[0]];
    const cols = ['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z'];
    const rawRows = XLSX.utils.sheet_to_json(ws, { header: cols, raw: false });

    // Parse balance base: expect columns like Nemotecnico, Existencia, Precio
    // Try to detect header row
    let headerIdx = rawRows.findIndex(r =>
      Object.values(r).some(v => /nemot|instrumento/i.test(String(v || '')))
    );
    if (headerIdx === -1) headerIdx = 0;
    const headers = rawRows[headerIdx];
    const dataRows = rawRows.slice(headerIdx + 1);

    // Map columns
    const colNem  = cols.find(c => /nemot|instrumento/i.test(String(headers[c] || '')));
    const colQty  = cols.find(c => /exist|cantidad|qty/i.test(String(headers[c] || '')));
    const colPrc  = cols.find(c => /precio|price/i.test(String(headers[c] || '')));

    const operaciones = dataRows
      .filter(r => colNem && r[colNem] && String(r[colNem]).trim())
      .map(r => ({
        Nemotecnico: String(r[colNem] || '').trim().toUpperCase(),
        Cantidad: parseFloat(String(r[colQty] || '0').replace(/\./g, '').replace(',', '.')) || 0,
        Precio: parseFloat(String(r[colPrc] || '0').replace(/\./g, '').replace(',', '.')) || 0,
        Tipo: 'Compra',
        Monto: 0,
        Fecha: null,
        Corredor: 'Balance Base',
      }))
      .filter(r => r.Cantidad > 0);

    const id = saveHistorialArchivo(req.file.originalname, 'balance_base', operaciones);
    res.json({ ok: true, id, saved: operaciones.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/actualizar-fila-balance — { nemotecnico, existencia (ignored for now, just updates price) }
app.post('/api/actualizar-fila-balance', (req, res) => {
  try {
    const { nemotecnico, precioCierre } = req.body;
    if (!nemotecnico) return res.status(400).json({ error: 'nemotecnico requerido' });
    upsertAjusteBalance(nemotecnico, parseFloat(precioCierre) || 0);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/ajuste-manual-balance/:nemotecnico
app.delete('/api/ajuste-manual-balance/:nemotecnico', (req, res) => {
  try {
    deleteAjusteBalance(req.params.nemotecnico);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/descargar-archivo-original/:id
app.get('/api/descargar-archivo-original/:id', (_req, res) => {
  res.status(404).json({ error: 'Archivo original no almacenado' });
});

// GET /api/descargar-csv-transformado/:id
app.get('/api/descargar-csv-transformado/:id', (req, res) => {
  try {
    const ops = getOperacionesByHistorial(Number(req.params.id));
    if (!ops.length) return res.status(404).json({ error: 'No operations found' });
    const headers = Object.keys(ops[0]);
    const csvLines = [headers.join(','), ...ops.map(op => headers.map(h => JSON.stringify(op[h] ?? '')).join(','))];
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', `attachment; filename="operaciones_${req.params.id}.csv"`);
    res.send(csvLines.join('\n'));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── BCI API ──────────────────────────────────────────────────────────────
app.get('/api/bci', (_req, res) => {
  const data = getLatestBci();
  if (!data) return res.status(404).json({ error: 'Sin datos BCI. Ejecuta el sync.' });
  res.json(data);
});

app.post('/api/bci/sync', (_req, res) => {
  runBciSync();
  res.json({ status: 'started' });
});

// ─── BCI Sync Runner ──────────────────────────────────────────────────────
function runBciSync() {
  if (!process.env.GOOGLE_TOKEN_JSON) {
    console.warn('[bci] GOOGLE_TOKEN_JSON no definida, sync omitido');
    return;
  }

  const script = path.join(__dirname, 'bci_sync.py');
  // En Linux (Railway) el binario es python3; en Windows puede ser python
  const pythonBin = process.platform === 'win32' ? 'python' : 'python3';
  const proc = spawn(pythonBin, [script], { env: process.env });

  proc.stdout.on('data', (d) => process.stdout.write('[bci] ' + d));
  proc.stderr.on('data', (d) => process.stderr.write('[bci] ' + d));
  proc.on('error', (e) => console.error('[bci] spawn error:', e.message));
  proc.on('close', (code) => {
    if (code !== 0) console.error(`[bci] sync failed (exit ${code})`);
    else console.log('[bci] sync completado');
  });
}

// ─── Cron BCI: L-V a las 09:30 hora Chile (UTC-3 = 12:30 UTC) ────────────
function scheduleBciCron() {
  function msUntilNext(h, m) {
    const now = new Date();
    const target = new Date();
    target.setUTCHours(h, m, 0, 0);
    if (target <= now) target.setUTCDate(target.getUTCDate() + 1);
    return target - now;
  }

  function tick() {
    const day = new Date().getUTCDay(); // 0=Dom 6=Sab
    if (day >= 1 && day <= 5) {
      console.log('[bci] Cron disparo: iniciando sync...');
      runBciSync();
    }
    // Próxima ejecución en 24h
    setTimeout(tick, 24 * 60 * 60 * 1000);
  }

  const ms = msUntilNext(12, 30); // 12:30 UTC = 09:30 CLT
  console.log(`[bci] Próximo sync en ${Math.round(ms/60000)} min`);
  setTimeout(tick, ms);
}

// ─── Startup ──────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);

  // Pre-warm today's cache — deferred 2s so Railway health check passes first
  const yesterday = getYesterday();
  if (!getCachedData(yesterday)) {
    setTimeout(() => {
      console.log(`[startup] Fetching ${yesterday} from AAFM...`);
      fetchDailyData(yesterday)
        .then((result) => {
          saveData(yesterday, result);
          console.log(`[startup] Cached ${result.rows.length} fondos for ${yesterday}`);
          // Update current month summary
          const [y, m] = yesterday.split('-').map(Number);
          const s = calcMonthlySummary(y, m);
          if (s.daysCount > 0) saveMonthly(`${y}-${String(m).padStart(2,'0')}`, s);
        })
        .catch((err) => console.error('[startup] Fetch failed:', err.message));
    }, 2000);
  } else {
    console.log(`[startup] Cache warm for ${yesterday}`);
  }

  // Build monthly history from existing cache (fast, no network)
  const now = new Date();
  let saved = 0;
  for (let i = 1; i <= 12; i++) {
    const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
    const y = d.getFullYear(), m = d.getMonth() + 1;
    const ym = `${y}-${String(m).padStart(2,'0')}`;
    const summary = calcMonthlySummary(y, m);
    if (summary.daysCount > 0) { saveMonthly(ym, summary); saved++; }
  }
  if (saved) console.log(`[startup] Monthly history: ${saved} months stored`);

  // Background backfill: fetch missing historical days so Histórico Anual loads on first visit
  // Runs 10s after startup to let health check pass first; uses low concurrency to avoid OOM
  setTimeout(async () => {
    try {
      const yesterday = getYesterday();
      const days = new Set();
      const ref = new Date();
      for (let i = 12; i >= 0; i--) {
        const d = new Date(ref.getFullYear(), ref.getMonth() - i, 1);
        workingDaysOf(d.getFullYear(), d.getMonth() + 1).past.forEach(day => days.add(day));
      }
      const toFetch = [...days].filter(d => d <= yesterday && !getCachedData(d)).sort();
      if (!toFetch.length) { console.log('[backfill] Nothing to fetch, cache complete'); return; }
      console.log(`[backfill] Fetching ${toFetch.length} missing days in background...`);
      let done = 0;
      const CONCURRENCY = 2; // Low to avoid OOM on Railway free tier
      for (let i = 0; i < toFetch.length; i += CONCURRENCY) {
        const batch = toFetch.slice(i, i + CONCURRENCY);
        await Promise.all(batch.map(date =>
          fetchDailyData(date)
            .then(result => { saveData(date, result); done++; })
            .catch(() => { done++; }) // skip holidays/unavailable dates
        ));
      }
      // Recalculate monthly summaries
      const ref2 = new Date();
      for (let i = 12; i >= 0; i--) {
        const d = new Date(ref2.getFullYear(), ref2.getMonth() - i, 1);
        const y = d.getFullYear(), m = d.getMonth() + 1;
        const ym = `${y}-${String(m).padStart(2,'0')}`;
        const summary = calcMonthlySummary(y, m);
        if (summary.daysCount > 0) saveMonthly(ym, summary);
      }
      console.log(`[backfill] Done — ${done} days processed`);
    } catch (err) {
      console.error('[backfill] Error:', err.message);
    }
  }, 10000);

  // BCI cron (solo al mediodía, no en startup para evitar OOM)
  scheduleBciCron();
});
