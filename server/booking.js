const fs = require("fs");
const path = require("path");
const { saveScrapingRun } = require("./db");

const DEFAULT_HEADERS = {
  "user-agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
  "accept-language": "es-ES,es;q=0.9,en;q=0.8",
};

let browserPromise = null;

function toISODate(date) {
  return date.toISOString().slice(0, 10);
}

function addDays(date, days) {
  const d = new Date(date);
  d.setDate(d.getDate() + days);
  return d;
}

function parseMoneyToNumber(value) {
  if (!value) return null;
  const cleaned = String(value).replace(/[^\d,.\-]/g, "");
  if (!cleaned) return null;

  const lastComma = cleaned.lastIndexOf(",");
  const lastDot = cleaned.lastIndexOf(".");
  let normalized = cleaned;

  if (lastComma > -1 && lastDot > -1) {
    if (lastComma > lastDot) {
      normalized = cleaned.replace(/\./g, "").replace(",", ".");
    } else {
      normalized = cleaned.replace(/,/g, "");
    }
  } else if (lastComma > -1) {
    normalized = cleaned.replace(/\./g, "").replace(",", ".");
  } else {
    normalized = cleaned.replace(/,/g, "");
  }

  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : null;
}

function getTempDir() {
  const dir = path.join(__dirname, "tmp", "booking-shots");
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

function safeName(v) {
  return String(v || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80);
}

async function getBrowser() {
  if (!browserPromise) {
    const puppeteer = require("puppeteer");
    browserPromise = puppeteer.launch({
      headless: "new",
      args: ["--no-sandbox", "--disable-setuid-sandbox"],
    });
  }
  return browserPromise;
}

async function runPool(items, limit, worker) {
  const results = new Array(items.length);
  let cursor = 0;
  async function next() {
    while (true) {
      const i = cursor;
      cursor += 1;
      if (i >= items.length) return;
      results[i] = await worker(items[i], i);
    }
  }
  await Promise.all(Array.from({ length: Math.max(1, limit) }, () => next()));
  return results;
}

/**
 * Extract the cheapest room price from the room availability table.
 * Expects the page to already be loaded at a hotel URL with checkin/checkout params.
 */
async function extractMinPriceFromRoomTable(page) {
  return page.evaluate(() => {
    const table =
      document.querySelector("#hprt-table") || document.querySelector("table.hprt-table");
    if (!table) return { price: null, error: "No se encontró la tabla de habitaciones" };

    const priceCells = table.querySelectorAll(
      ".bui-price-display__value, .prco-val498-buy498, [data-testid='price-and-tax-tag'], .hprt-price-price, td.hprt-table-cell-price",
    );

    const prices = [];
    for (const cell of priceCells) {
      const text = (cell.textContent || "").replace(/\s+/g, " ").trim();
      const patterns = [
        /(\d[\d.,]*)\s*[€]/,
        /[€]\s*(\d[\d.,]*)/,
        /EUR\s*(\d[\d.,]*)/,
        /(\d[\d.,]*)\s*EUR/,
      ];
      for (const re of patterns) {
        const m = text.match(re);
        if (m) {
          const numStr = m[1].replace(/\./g, "").replace(",", ".");
          const num = parseFloat(numStr);
          if (num > 20 && num < 10000) prices.push(num);
          break;
        }
      }
    }

    if (prices.length === 0) {
      const bodyText = (table.textContent || "").replace(/\s+/g, " ");
      const regex = /(?:EUR|€)\s*(\d[\d.,]*)|(\d[\d.,]*)\s*(?:EUR|€)/g;
      let m;
      while ((m = regex.exec(bodyText)) !== null) {
        const numStr = (m[1] || m[2]).replace(/\./g, "").replace(",", ".");
        const num = parseFloat(numStr);
        if (num > 20 && num < 10000) prices.push(num);
      }
    }

    if (prices.length === 0) {
      return { price: null, error: "No se encontraron precios en la tabla" };
    }

    const unique = [...new Set(prices)];
    return { price: Math.min(...unique), allPrices: unique.sort((a, b) => a - b), error: null };
  });
}

/**
 * Scrape prices for one hotel across multiple dates.
 * Opens one browser page and reuses it for each date (faster than opening a new one each time).
 */
async function scrapeHotelPrices(hotel, dates) {
  const browser = await getBrowser();
  const page = await browser.newPage();
  const results = {};

  for (const d of dates) {
    results[d] = { date: d, price: null, currency: "EUR", error: null };
  }

  try {
    await page.setViewport({ width: 1400, height: 900 });
    await page.setUserAgent(DEFAULT_HEADERS["user-agent"]);
    await page.setExtraHTTPHeaders({ "accept-language": DEFAULT_HEADERS["accept-language"] });

    // First load to accept cookies
    await page.goto(hotel.url, { waitUntil: "domcontentloaded", timeout: 60000 });
    await new Promise((r) => setTimeout(r, 2000));
    const cookieBtn = await page.$("#onetrust-accept-btn-handler");
    if (cookieBtn) await cookieBtn.click().catch(() => {});
    await new Promise((r) => setTimeout(r, 500));

    for (const isoDate of dates) {
      try {
        const checkout = toISODate(addDays(new Date(isoDate), 1));
        const dateUrl = `${hotel.url}?checkin=${isoDate}&checkout=${checkout}&group_adults=2&no_rooms=1&group_children=0&selected_currency=EUR`;

        await page.goto(dateUrl, { waitUntil: "domcontentloaded", timeout: 45000 });
        await page.waitForSelector("#hprt-table, table.hprt-table", { timeout: 20000 }).catch(() => {});
        await new Promise((r) => setTimeout(r, 1500));

        const result = await extractMinPriceFromRoomTable(page);
        results[isoDate].price = result.price;
        results[isoDate].error = result.error;
      } catch (err) {
        results[isoDate].error = err instanceof Error ? err.message : "Error desconocido";
      }
    }
  } catch (err) {
    const msg = err instanceof Error ? err.message : "Error desconocido";
    for (const d of dates) {
      if (!results[d].price && !results[d].error) results[d].error = msg;
    }
  } finally {
    await page.close().catch(() => {});
  }

  return results;
}

async function generateCompetitionMatrix({ hotels, days = 15, startDate }) {
  const safeDays = Math.min(Math.max(Number(days) || 15, 1), 30);
  const start = startDate ? new Date(startDate) : new Date();
  if (Number.isNaN(start.getTime())) throw new Error("Fecha de inicio inválida.");

  const normalizedHotels = hotels.map((h) => ({
    name: String(h.name || "").trim(),
    url: String(h.url || "").trim(),
  }));
  const invalid = normalizedHotels.find((h) => !h.name || !h.url);
  if (invalid) throw new Error("Cada hotel debe tener nombre y URL.");

  const dates = Array.from({ length: safeDays }, (_, i) => toISODate(addDays(start, i)));

  // Parallel scraping: 3 hotels at a time (each opens its own browser tab)
  const hotelResults = await runPool(normalizedHotels, 3, async (hotel) => {
    const pricesByDate = await scrapeHotelPrices(hotel, dates);
    return {
      ...hotel,
      prices: dates.map((date) => pricesByDate[date]),
    };
  });

  const result = { dates, stayNights: 1, hotels: hotelResults };

  try {
    saveScrapingRun(new Date().toISOString(), hotelResults, dates);
  } catch (err) {
    console.error("Error guardando historial en SQLite:", err);
  }

  return result;
}

module.exports = { generateCompetitionMatrix };
