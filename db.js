const Database = require('better-sqlite3');
const path = require('path');

const DB_PATH = process.env.DB_PATH || path.join(__dirname, 'data.db');
let db;

function getDB() {
  if (!db) {
    db = new Database(DB_PATH);
    db.exec(`
      CREATE TABLE IF NOT EXISTS daily_data (
        date TEXT PRIMARY KEY,
        data TEXT NOT NULL,
        fetched_at TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS monthly_summary (
        year_month    TEXT PRIMARY KEY,
        aportes       REAL NOT NULL,
        rescates      REAL NOT NULL,
        net_flow      REAL NOT NULL,
        days_count    INTEGER NOT NULL,
        working_days  INTEGER NOT NULL,
        calculated_at TEXT NOT NULL
      );
    `);
  }
  return db;
}

function getCachedData(date) {
  const row = getDB().prepare('SELECT data, fetched_at FROM daily_data WHERE date = ?').get(date);
  if (!row) return null;
  return { data: JSON.parse(row.data), fetched_at: row.fetched_at };
}

function saveData(date, data) {
  getDB().prepare(`
    INSERT OR REPLACE INTO daily_data (date, data, fetched_at)
    VALUES (?, ?, ?)
  `).run(date, JSON.stringify(data), new Date().toISOString());
}

function listDates() {
  return getDB().prepare('SELECT date, fetched_at FROM daily_data ORDER BY date DESC LIMIT 30').all();
}

function saveMonthly(yearMonth, { aportes, rescates, netFlow, daysCount, workingDays }) {
  getDB().prepare(`
    INSERT OR REPLACE INTO monthly_summary
    (year_month, aportes, rescates, net_flow, days_count, working_days, calculated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run(yearMonth, aportes, rescates, netFlow, daysCount, workingDays, new Date().toISOString());
}

function getMonthly(yearMonth) {
  return getDB().prepare('SELECT * FROM monthly_summary WHERE year_month = ?').get(yearMonth) || null;
}

function getMonthlyHistory(months = 12) {
  return getDB().prepare(`
    SELECT * FROM monthly_summary ORDER BY year_month DESC LIMIT ?
  `).all(months).reverse(); // oldest first for chart
}

// ─── BCI Cartolas ─────────────────────────────────────────────────────────
function ensureBciTable() {
  getDB().exec(`
    CREATE TABLE IF NOT EXISTS bci_cartolas (
      id        INTEGER PRIMARY KEY AUTOINCREMENT,
      synced_at TEXT NOT NULL,
      fuente    TEXT,
      data      TEXT NOT NULL
    )
  `);
}

function getLatestBci() {
  ensureBciTable();
  const row = getDB().prepare(
    'SELECT data, synced_at, fuente FROM bci_cartolas ORDER BY id DESC LIMIT 1'
  ).get();
  if (!row) return null;
  return JSON.parse(row.data);
}

function saveBci(synced_at, fuente, data) {
  ensureBciTable();
  getDB().prepare(
    'INSERT INTO bci_cartolas (synced_at, fuente, data) VALUES (?,?,?)'
  ).run(synced_at, fuente, JSON.stringify(data));
}

module.exports = { getCachedData, saveData, listDates, saveMonthly, getMonthly, getMonthlyHistory, getLatestBci, saveBci };
