const { createClient } = require("@libsql/client");

let _client = null;

function getClient() {
  if (_client) return _client;

  const url = process.env.TURSO_DATABASE_URL || process.env.DB_TURSO_DATABASE_URL;
  const authToken = process.env.TURSO_AUTH_TOKEN || process.env.DB_TURSO_AUTH_TOKEN;

  if (!url) {
    console.warn("Turso no configurado: falta TURSO_DATABASE_URL");
    return null;
  }

  _client = createClient({ url, authToken });
  return _client;
}

async function initDb() {
  const client = getClient();
  if (!client) return;

  await client.batch(
    [
      `CREATE TABLE IF NOT EXISTS price_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        scraped_at TEXT NOT NULL,
        hotel_name TEXT NOT NULL,
        hotel_url TEXT NOT NULL,
        target_date TEXT NOT NULL,
        price REAL,
        error TEXT
      )`,
      `CREATE INDEX IF NOT EXISTS idx_hotel_date
        ON price_history(hotel_name, target_date)`,
    ],
    "write",
  );
}

async function saveScrapingRun(scrapedAt, hotels, dates) {
  const client = getClient();
  if (!client) return;

  const stmts = [];
  for (const hotel of hotels) {
    for (let i = 0; i < dates.length; i++) {
      const entry = hotel.prices[i];
      stmts.push({
        sql: `INSERT INTO price_history (scraped_at, hotel_name, hotel_url, target_date, price, error)
              VALUES (?, ?, ?, ?, ?, ?)`,
        args: [
          scrapedAt,
          hotel.name,
          hotel.url,
          entry.date,
          entry.price ?? null,
          entry.error ?? null,
        ],
      });
    }
  }

  await client.batch(stmts, "write");
}

async function getHistoryBulk(hotelNames, dates, limit = 7) {
  const client = getClient();
  if (!client) return {};

  const phH = hotelNames.map(() => "?").join(",");
  const phD = dates.map(() => "?").join(",");

  const rs = await client.execute({
    sql: `SELECT hotel_name, target_date, price, scraped_at
          FROM price_history
          WHERE hotel_name IN (${phH})
            AND target_date IN (${phD})
          ORDER BY hotel_name, target_date, scraped_at DESC`,
    args: [...hotelNames, ...dates],
  });

  const result = {};
  const counters = {};

  for (const row of rs.rows) {
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

async function getLatestRun() {
  const client = getClient();
  if (!client) return null;

  const rs = await client.execute(
    `SELECT p.hotel_name, p.hotel_url, p.target_date, p.price, p.error, p.scraped_at
     FROM price_history p
     INNER JOIN (
       SELECT hotel_name, target_date, MAX(scraped_at) AS max_at
       FROM price_history
       GROUP BY hotel_name, target_date
     ) latest ON p.hotel_name = latest.hotel_name
            AND p.target_date = latest.target_date
            AND p.scraped_at = latest.max_at
     ORDER BY p.hotel_name, p.target_date`,
  );

  if (!rs.rows.length) return null;

  const datesSet = new Set();
  const hotelsMap = new Map();
  let maxScrapedAt = "";

  for (const row of rs.rows) {
    datesSet.add(row.target_date);
    if (row.scraped_at > maxScrapedAt) maxScrapedAt = row.scraped_at;
    if (!hotelsMap.has(row.hotel_name)) {
      hotelsMap.set(row.hotel_name, {
        name: row.hotel_name,
        url: row.hotel_url,
        pricesMap: {},
      });
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
    prices: dates.map(
      (d) => h.pricesMap[d] || { date: d, price: null, currency: "EUR", error: null },
    ),
  }));

  return { dates, stayNights: 1, hotels, scrapedAt: maxScrapedAt };
}

/**
 * Return the set of hotel names that have been scraped today (Madrid timezone).
 */
async function getScrapedHotelsToday() {
  const client = getClient();
  if (!client) return new Set();

  const now = new Date();
  const madridDate = now.toLocaleDateString("en-CA", { timeZone: "Europe/Madrid" });
  const todayStart = `${madridDate}T00:00:00`;

  const rs = await client.execute({
    sql: "SELECT DISTINCT hotel_name FROM price_history WHERE scraped_at >= ?",
    args: [todayStart],
  });

  return new Set(rs.rows.map((r) => r.hotel_name));
}

module.exports = { initDb, saveScrapingRun, getHistoryBulk, getLatestRun, getScrapedHotelsToday };
