const express = require('express');
const cors = require('cors');
const path = require('path');
const { fetchDailyData } = require('./scraper');
const { getCachedData, saveData, listDates, saveMonthly, getMonthly, getMonthlyHistory } = require('./db');

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
app.use(express.static(path.join(__dirname, 'public')));

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

// ─── Startup ──────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);

  // Pre-warm today's cache
  const yesterday = getYesterday();
  if (!getCachedData(yesterday)) {
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
});
