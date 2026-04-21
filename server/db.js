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
      `CREATE TABLE IF NOT EXISTS tareas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        estado TEXT NOT NULL DEFAULT 'pendiente',
        edificio TEXT NOT NULL DEFAULT '',
        asunto TEXT NOT NULL,
        importancia TEXT NOT NULL DEFAULT 'media',
        responsable TEXT NOT NULL DEFAULT '',
        orden INTEGER NOT NULL DEFAULT 0,
        fecha_limite TEXT DEFAULT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
      )`,
      `CREATE INDEX IF NOT EXISTS idx_tareas_responsable ON tareas(responsable)`,
      `CREATE INDEX IF NOT EXISTS idx_tareas_estado ON tareas(estado)`,
      `CREATE INDEX IF NOT EXISTS idx_tareas_orden ON tareas(orden)`,
      `CREATE TABLE IF NOT EXISTS tarea_historial (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tarea_id INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        autor TEXT NOT NULL DEFAULT '',
        texto TEXT NOT NULL,
        FOREIGN KEY (tarea_id) REFERENCES tareas(id) ON DELETE CASCADE
      )`,
      `CREATE INDEX IF NOT EXISTS idx_tarea_historial_tarea ON tarea_historial(tarea_id, created_at)`,
      `CREATE TABLE IF NOT EXISTS reviews (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT NOT NULL,
        external_id TEXT,
        review_date TEXT,
        author TEXT,
        text TEXT,
        rating REAL,
        rating_max REAL,
        scraped_at TEXT NOT NULL
      )`,
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_reviews_external_id ON reviews(external_id) WHERE external_id IS NOT NULL`,
      `CREATE INDEX IF NOT EXISTS idx_reviews_source_date ON reviews(source, review_date)`,
    ],
    "write",
  );

  // Schema migrations for existing installations (ignore errors when column already exists)
  const migrations = [
    `ALTER TABLE tareas ADD COLUMN fecha_limite TEXT DEFAULT NULL`,
  ];
  for (const sql of migrations) {
    try { await client.execute(sql); } catch {}
  }
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

const TAREA_ESTADOS = new Set(["pendiente", "en_proceso", "completado", "cancelado"]);
const TAREA_IMPORTANCIAS = new Set(["baja", "media", "alta"]);

function normalizeEstado(value) {
  const v = String(value || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "_");
  if (TAREA_ESTADOS.has(v)) return v;
  return null;
}

function normalizeImportancia(value) {
  const v = String(value || "")
    .trim()
    .toLowerCase();
  if (TAREA_IMPORTANCIAS.has(v)) return v;
  return null;
}

async function listTareasResumen() {
  const client = getClient();
  if (!client) return null;

  const rs = await client.execute(
    `SELECT estado, COUNT(*) AS total FROM tareas GROUP BY estado`,
  );

  const map = { pendiente: 0, en_proceso: 0, completado: 0, cancelado: 0 };
  let total = 0;
  for (const row of rs.rows) {
    const k = String(row.estado);
    map[k] = Number(row.total) || 0;
    total += map[k];
  }
  return { ...map, total };
}

async function listTareasResponsables() {
  const client = getClient();
  if (!client) return null;

  const rs = await client.execute(
    `SELECT DISTINCT TRIM(responsable) AS responsable
     FROM tareas
     WHERE TRIM(responsable) != ''
     ORDER BY responsable COLLATE NOCASE`,
  );

  return rs.rows.map((r) => r.responsable);
}

async function listTareas({ responsable, estado, ordenar = "orden", direccion = "asc" } = {}) {
  const client = getClient();
  if (!client) return null;

  const args = [];
  const where = [];

  if (responsable && String(responsable).trim()) {
    where.push("LOWER(TRIM(t.responsable)) = LOWER(TRIM(?))");
    args.push(String(responsable).trim());
  }

  const estadoStr = String(estado || "").trim().toLowerCase();
  if (estadoStr === "activas") {
    where.push("t.estado IN ('pendiente', 'en_proceso')");
  } else {
    const est = normalizeEstado(estado);
    if (est) {
      where.push("t.estado = ?");
      args.push(est);
    }
  }

  const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

  const dir = String(direccion).toLowerCase() === "desc" ? "DESC" : "ASC";
  let orderSql = `t.orden ${dir}, t.id ASC`;
  if (ordenar === "created") orderSql = `t.created_at ${dir}, t.id ASC`;
  else if (ordenar === "updated") orderSql = `t.updated_at ${dir}, t.id ASC`;
  else if (ordenar === "importancia")
    orderSql = `CASE t.importancia WHEN 'alta' THEN 3 WHEN 'media' THEN 2 WHEN 'baja' THEN 1 ELSE 0 END ${dir}, t.orden ASC, t.id ASC`;
  else if (ordenar === "asunto") orderSql = `t.asunto COLLATE NOCASE ${dir}, t.id ASC`;
  else if (ordenar === "orden") orderSql = `t.orden ${dir}, t.id ASC`;

  const rs = await client.execute({
    sql: `SELECT t.id, t.estado, t.edificio, t.asunto, t.importancia, t.responsable, t.orden,
                 t.fecha_limite, t.created_at, t.updated_at,
                 (SELECT h.texto FROM tarea_historial h
                  WHERE h.tarea_id = t.id ORDER BY h.created_at DESC LIMIT 1) AS ultimo_comentario
          FROM tareas t
          ${whereSql}
          ORDER BY ${orderSql}`,
    args,
  });

  return rs.rows;
}

async function getTareaById(id) {
  const client = getClient();
  if (!client) return null;

  const tid = Number(id);
  if (!Number.isInteger(tid) || tid < 1) return null;

  const tr = await client.execute({
    sql: `SELECT id, estado, edificio, asunto, importancia, responsable, orden, fecha_limite, created_at, updated_at
          FROM tareas WHERE id = ?`,
    args: [tid],
  });

  if (!tr.rows.length) return null;

  const hr = await client.execute({
    sql: `SELECT id, created_at, autor, texto
          FROM tarea_historial WHERE tarea_id = ? ORDER BY created_at ASC, id ASC`,
    args: [tid],
  });

  return { ...tr.rows[0], historial: hr.rows };
}

async function createTarea({
  estado = "pendiente",
  edificio = "",
  asunto,
  importancia = "media",
  responsable = "",
  fecha_limite = null,
  comentario_inicial = "",
  autor = "",
}) {
  const client = getClient();
  if (!client) return null;

  const subject = String(asunto || "").trim();
  if (!subject) throw new Error("El asunto es obligatorio.");

  const e = normalizeEstado(estado) || "pendiente";
  const imp = normalizeImportancia(importancia) || "media";
  const fl = fecha_limite ? String(fecha_limite).trim() : null;
  const now = new Date().toISOString();

  const maxRs = await client.execute(`SELECT COALESCE(MAX(orden), 0) + 1 AS next_orden FROM tareas`);
  const nextOrden = Number(maxRs.rows[0]?.next_orden) || 1;

  const ins = await client.execute({
    sql: `INSERT INTO tareas (estado, edificio, asunto, importancia, responsable, orden, fecha_limite, created_at, updated_at)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
          RETURNING id`,
    args: [e, String(edificio || "").trim(), subject, imp, String(responsable || "").trim(), nextOrden, fl, now, now],
  });

  const id = ins.rows[0]?.id;
  if (id == null) throw new Error("No se pudo crear la tarea.");

  const note = String(comentario_inicial || "").trim();
  if (note) {
    await client.execute({
      sql: `INSERT INTO tarea_historial (tarea_id, created_at, autor, texto) VALUES (?, ?, ?, ?)`,
      args: [id, now, String(autor || "").trim(), note],
    });
  }

  return getTareaById(id);
}

async function updateTarea(id, patch) {
  const client = getClient();
  if (!client) return null;

  const tid = Number(id);
  if (!Number.isInteger(tid) || tid < 1) return null;

  const allowed = ["estado", "edificio", "asunto", "importancia", "responsable", "orden", "fecha_limite"];
  const sets = [];
  const args = [];

  for (const key of allowed) {
    if (!(key in patch)) continue;
    if (key === "estado") {
      const v = normalizeEstado(patch.estado);
      if (!v) continue;
      sets.push("estado = ?");
      args.push(v);
    } else if (key === "importancia") {
      const v = normalizeImportancia(patch.importancia);
      if (!v) continue;
      sets.push("importancia = ?");
      args.push(v);
    } else if (key === "orden") {
      const n = Number(patch.orden);
      if (!Number.isFinite(n)) continue;
      sets.push("orden = ?");
      args.push(Math.round(n));
    } else if (key === "asunto") {
      const v = String(patch.asunto ?? "").trim();
      if (!v) throw new Error("El asunto no puede quedar vacío.");
      sets.push("asunto = ?");
      args.push(v);
    } else if (key === "edificio" || key === "responsable") {
      sets.push(`${key} = ?`);
      args.push(String(patch[key] ?? "").trim());
    } else if (key === "fecha_limite") {
      const v = patch.fecha_limite;
      sets.push("fecha_limite = ?");
      args.push(v ? String(v).trim() : null);
    }
  }

  if (!sets.length) return getTareaById(tid);

  const now = new Date().toISOString();
  sets.push("updated_at = ?");
  args.push(now);
  args.push(tid);

  await client.execute({
    sql: `UPDATE tareas SET ${sets.join(", ")} WHERE id = ?`,
    args,
  });

  return getTareaById(tid);
}

async function addTareaHistorial(tareaId, { texto, autor = "" }) {
  const client = getClient();
  if (!client) return null;

  const tid = Number(tareaId);
  if (!Number.isInteger(tid) || tid < 1) return null;

  const body = String(texto || "").trim();
  if (!body) throw new Error("El comentario no puede estar vacío.");

  const now = new Date().toISOString();

  await client.execute({
    sql: `INSERT INTO tarea_historial (tarea_id, created_at, autor, texto) VALUES (?, ?, ?, ?)`,
    args: [tid, now, String(autor || "").trim(), body],
  });

  await client.execute({
    sql: `UPDATE tareas SET updated_at = ? WHERE id = ?`,
    args: [now, tid],
  });

  return getTareaById(tid);
}

async function reordenarTareas(ids) {
  const client = getClient();
  if (!client) return null;

  if (!Array.isArray(ids) || !ids.length) throw new Error("Debe indicarse el nuevo orden.");

  const clean = ids.map((x) => Number(x)).filter((n) => Number.isInteger(n) && n > 0);
  if (!clean.length) throw new Error("Identificadores de tarea no válidos.");

  const now = new Date().toISOString();
  const stmts = clean.map((id, index) => ({
    sql: `UPDATE tareas SET orden = ?, updated_at = ? WHERE id = ?`,
    args: [(index + 1) * 10, now, id],
  }));

  await client.batch(stmts, "write");
  return true;
}

/**
 * Intercambia `orden` con la tarea inmediatamente superior o inferior en la lista global.
 * @param {"up"|"down"} direccion
 */
async function moverTareaRelativo(id, direccion) {
  const client = getClient();
  if (!client) return null;

  const tid = Number(id);
  if (!Number.isInteger(tid) || tid < 1) return null;
  if (direccion !== "up" && direccion !== "down") throw new Error("Dirección no válida.");

  const cur = await client.execute({
    sql: `SELECT id, orden FROM tareas WHERE id = ?`,
    args: [tid],
  });
  if (!cur.rows.length) return null;

  const ordenActual = Number(cur.rows[0].orden) || 0;

  const neighborSql =
    direccion === "up"
      ? `SELECT id, orden FROM tareas
         WHERE orden < ? OR (orden = ? AND id < ?)
         ORDER BY orden DESC, id DESC
         LIMIT 1`
      : `SELECT id, orden FROM tareas
         WHERE orden > ? OR (orden = ? AND id > ?)
         ORDER BY orden ASC, id ASC
         LIMIT 1`;

  const nbArgs = [ordenActual, ordenActual, tid];
  const nb = await client.execute({ sql: neighborSql, args: nbArgs });
  if (!nb.rows.length) return getTareaById(tid);

  const oid = Number(nb.rows[0].id);
  const ordenOtro = Number(nb.rows[0].orden) || 0;
  const now = new Date().toISOString();

  await client.batch(
    [
      {
        sql: `UPDATE tareas SET orden = ?, updated_at = ? WHERE id = ?`,
        args: [ordenOtro, now, tid],
      },
      {
        sql: `UPDATE tareas SET orden = ?, updated_at = ? WHERE id = ?`,
        args: [ordenActual, now, oid],
      },
    ],
    "write",
  );

  return getTareaById(tid);
}

/**
 * Guarda un array de reseñas en la BD, ignorando duplicados por external_id.
 * @param {Array} reviews
 * @returns {{ inserted: number }}
 */
async function saveReviews(reviews) {
  const client = getClient();
  if (!client || !reviews.length) return { inserted: 0 };

  const now = new Date().toISOString();
  let inserted = 0;

  for (const r of reviews) {
    try {
      if (r.external_id) {
        const existing = await client.execute({
          sql: `SELECT id FROM reviews WHERE external_id = ?`,
          args: [r.external_id],
        });
        if (existing.rows.length) continue;
      }

      await client.execute({
        sql: `INSERT INTO reviews (source, external_id, review_date, author, text, rating, rating_max, scraped_at)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        args: [
          r.source,
          r.external_id ?? null,
          r.review_date ?? null,
          r.author ?? null,
          r.text ?? null,
          r.rating ?? null,
          r.rating_max ?? null,
          now,
        ],
      });
      inserted++;
    } catch (err) {
      // Ignorar errores de unicidad; loguear otros
      if (!String(err.message).includes("UNIQUE")) {
        console.error("[db] Error insertando reseña:", err.message, r);
      }
    }
  }

  return { inserted };
}

/**
 * Devuelve todas las reseñas almacenadas, con filtros opcionales.
 * @param {{ source?: string, limit?: number, offset?: number }} opts
 */
async function getReviews({ source, limit = 500, offset = 0 } = {}) {
  const client = getClient();
  if (!client) return [];

  const where = source ? "WHERE source = ?" : "";
  const args = source ? [source, limit, offset] : [limit, offset];

  const rs = await client.execute({
    sql: `SELECT id, source, external_id, review_date, author, text, rating, rating_max, scraped_at
          FROM reviews
          ${where}
          ORDER BY review_date DESC, scraped_at DESC
          LIMIT ? OFFSET ?`,
    args,
  });

  return rs.rows;
}

/**
 * Devuelve la fecha (review_date) de la reseña más reciente por fuente.
 * { booking: 'YYYY-MM-DD' | null, google: null, tripadvisor: null }
 */
async function getLastReviewDates() {
  const client = getClient();
  if (!client) return { booking: null, google: null, tripadvisor: null };

  const rs = await client.execute(
    `SELECT source, MAX(review_date) AS last_date
     FROM reviews
     WHERE review_date IS NOT NULL
     GROUP BY source`,
  );

  const result = { booking: null, google: null, tripadvisor: null };
  for (const row of rs.rows) {
    result[row.source] = row.last_date;
  }
  return result;
}

/**
 * Devuelve el timestamp de la última vez que se scrapeó (scraped_at más reciente).
 */
async function getLastReviewScrapedAt() {
  const client = getClient();
  if (!client) return null;

  const rs = await client.execute(
    `SELECT MAX(scraped_at) AS last_at FROM reviews`,
  );
  return rs.rows[0]?.last_at ?? null;
}

module.exports = {
  initDb,
  saveScrapingRun,
  getHistoryBulk,
  getLatestRun,
  getScrapedHotelsToday,
  listTareas,
  listTareasResumen,
  listTareasResponsables,
  getTareaById,
  createTarea,
  updateTarea,
  addTareaHistorial,
  reordenarTareas,
  moverTareaRelativo,
  saveReviews,
  getReviews,
  getLastReviewDates,
  getLastReviewScrapedAt,
};
