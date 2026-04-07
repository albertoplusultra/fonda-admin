const path = require("path");
const fs = require("fs");

let Database;
try {
  Database = require("better-sqlite3");
} catch {
  Database = null;
}

const DATA_DIR = path.join(__dirname, "data");
const DB_PATH = path.join(DATA_DIR, "prices.db");

let _db = null;
let _available = !!Database;

function getDb() {
  if (!_available) return null;
  if (_db) return _db;

  try {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    _db = new Database(DB_PATH);
    _db.pragma("journal_mode = WAL");

    _db.exec(`
      CREATE TABLE IF NOT EXISTS price_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        scraped_at TEXT NOT NULL,
        hotel_name TEXT NOT NULL,
        hotel_url TEXT NOT NULL,
        target_date TEXT NOT NULL,
        price REAL,
        error TEXT
      );
      CREATE INDEX IF NOT EXISTS idx_hotel_date
        ON price_history(hotel_name, target_date);
    `);
  } catch (err) {
    console.warn("SQLite no disponible:", err.message);
    _available = false;
    return null;
  }

  return _db;
}

function initDb() {
  getDb();
}

/**
 * Persist every cell of a scraping run inside a single transaction.
 * @param {string} scrapedAt  ISO timestamp of this run
 * @param {Array}  hotels     array of { name, url, prices: [{ date, price, error }] }
 * @param {string[]} dates    ordered ISO date strings
 */
function saveScrapingRun(scrapedAt, hotels, dates) {
  const db = getDb();
  if (!db) return;
  const insert = db.prepare(`
    INSERT INTO price_history (scraped_at, hotel_name, hotel_url, target_date, price, error)
    VALUES (@scrapedAt, @hotelName, @hotelUrl, @targetDate, @price, @error)
  `);

  const runAll = db.transaction(() => {
    for (const hotel of hotels) {
      for (let i = 0; i < dates.length; i++) {
        const entry = hotel.prices[i];
        insert.run({
          scrapedAt,
          hotelName: hotel.name,
          hotelUrl: hotel.url,
          targetDate: entry.date,
          price: entry.price ?? null,
          error: entry.error ?? null,
        });
      }
    }
  });

  runAll();
}

/**
 * Return the last N prices for every (hotel, date) combination.
 * @param {string[]} hotelNames
 * @param {string[]} dates
 * @param {number}   limit
 * @returns {{ [hotelName]: { [date]: Array<{price:number|null, scraped_at:string}> } }}
 */
function getHistoryBulk(hotelNames, dates, limit = 7) {
  const db = getDb();
  if (!db) return {};

  const placeholdersH = hotelNames.map(() => "?").join(",");
  const placeholdersD = dates.map(() => "?").join(",");

  const rows = db
    .prepare(
      `SELECT hotel_name, target_date, price, scraped_at
       FROM price_history
       WHERE hotel_name IN (${placeholdersH})
         AND target_date IN (${placeholdersD})
       ORDER BY hotel_name, target_date, scraped_at DESC`,
    )
    .all(...hotelNames, ...dates);

  const result = {};
  const counters = {};

  for (const row of rows) {
    const key = `${row.hotel_name}||${row.target_date}`;
    counters[key] = (counters[key] || 0) + 1;
    if (counters[key] > limit) continue;

    if (!result[row.hotel_name]) result[row.hotel_name] = {};
    if (!result[row.hotel_name][row.target_date])
      result[row.hotel_name][row.target_date] = [];

    result[row.hotel_name][row.target_date].push({
      price: row.price,
      scraped_at: row.scraped_at,
    });
  }

  return result;
}

/**
 * Return the most recent scraping run reconstructed as a matrix
 * compatible with the shape returned by generateCompetitionMatrix.
 * Returns null if no data exists.
 */
function getLatestRun() {
  const db = getDb();
  if (!db) return null;

  const latest = db
    .prepare("SELECT scraped_at FROM price_history ORDER BY scraped_at DESC LIMIT 1")
    .get();
  if (!latest) return null;

  const rows = db
    .prepare(
      `SELECT hotel_name, hotel_url, target_date, price, error
       FROM price_history
       WHERE scraped_at = ?
       ORDER BY hotel_name, target_date`,
    )
    .all(latest.scraped_at);

  if (!rows.length) return null;

  const datesSet = new Set();
  const hotelsMap = new Map();

  for (const row of rows) {
    datesSet.add(row.target_date);
    if (!hotelsMap.has(row.hotel_name)) {
      hotelsMap.set(row.hotel_name, { name: row.hotel_name, url: row.hotel_url, pricesMap: {} });
    }
    hotelsMap.get(row.hotel_name).pricesMap[row.target_date] = {
      date: row.target_date,
      price: row.price,
      currency: "EUR",
      error: row.error,
    };
  }

  const dates = [...datesSet].sort();
  const hotels = [...hotelsMap.values()].map((h) => ({
    name: h.name,
    url: h.url,
    prices: dates.map((d) => h.pricesMap[d] || { date: d, price: null, currency: "EUR", error: null }),
  }));

  return { dates, stayNights: 1, hotels, scrapedAt: latest.scraped_at };
}

module.exports = { initDb, saveScrapingRun, getHistoryBulk, getLatestRun };
