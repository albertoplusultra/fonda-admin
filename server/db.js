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

/** Crea siempre un cliente nuevo (usar después de operaciones largas). */
function getFreshClient() {
  const url = process.env.TURSO_DATABASE_URL || process.env.DB_TURSO_DATABASE_URL;
  const authToken = process.env.TURSO_AUTH_TOKEN || process.env.DB_TURSO_AUTH_TOKEN;
  if (!url) return null;
  _client = createClient({ url, authToken }); // reemplaza el singleton también
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
      `DROP TABLE IF EXISTS reviews`,
      `CREATE TABLE IF NOT EXISTS booking_reviews (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        alojamiento TEXT NOT NULL,
        fecha_comentario TEXT,
        nombre_cliente TEXT,
        numero_reserva TEXT,
        titulo TEXT,
        comentario_positivo TEXT,
        comentario_negativo TEXT,
        puntuacion REAL,
        personal REAL,
        limpieza REAL,
        ubicacion REAL,
        instalaciones REAL,
        confort REAL,
        relacion_calidad_precio REAL,
        respuesta_alojamiento TEXT,
        external_id TEXT,
        scraped_at TEXT NOT NULL,
        resumen TEXT
      )`,
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_booking_reviews_reserva
        ON booking_reviews(alojamiento, numero_reserva)
        WHERE numero_reserva IS NOT NULL AND numero_reserva != ''`,
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_booking_reviews_ext
        ON booking_reviews(alojamiento, external_id)
        WHERE external_id IS NOT NULL AND external_id != ''`,
      `CREATE INDEX IF NOT EXISTS idx_booking_reviews_fecha
        ON booking_reviews(alojamiento, fecha_comentario)`,
      `CREATE TABLE IF NOT EXISTS google_reviews (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        alojamiento TEXT NOT NULL,
        author TEXT,
        author_description TEXT,
        review_date TEXT,
        rating REAL,
        helpful_count INTEGER,
        review TEXT,
        picture_included TEXT,
        language TEXT,
        message_original TEXT,
        language_code TEXT,
        reviewer_data TEXT,
        profile_url TEXT,
        review_url TEXT,
        photos TEXT,
        external_id TEXT,
        scraped_at TEXT NOT NULL,
        resumen TEXT
      )`,
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_google_reviews_url
        ON google_reviews(alojamiento, review_url)
        WHERE review_url IS NOT NULL AND review_url != ''`,
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_google_reviews_ext
        ON google_reviews(alojamiento, external_id)
        WHERE external_id IS NOT NULL AND external_id != ''`,
      `CREATE INDEX IF NOT EXISTS idx_google_reviews_fecha
        ON google_reviews(alojamiento, review_date)`,
    ],
    "write",
  );

  // Schema migrations for existing installations (ignore errors when column already exists)
  const migrations = [
    `ALTER TABLE tareas          ADD COLUMN fecha_limite TEXT DEFAULT NULL`,
    `ALTER TABLE booking_reviews ADD COLUMN resumen      TEXT DEFAULT NULL`,
    `ALTER TABLE google_reviews  ADD COLUMN resumen      TEXT DEFAULT NULL`,
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

const { DEFAULT_ALOJAMIENTO } = require("./alojamientos");

function toNumber(v) {
  if (v == null || v === "") return null;
  const n = parseFloat(String(v).replace(",", "."));
  return Number.isFinite(n) ? n : null;
}

function toText(v) {
  if (v == null) return null;
  const s = String(v).trim();
  return s === "" ? null : s;
}

/** Días equivalentes a 18 semanas (SQLite no tiene modificador `week` en date()). */
const OPINIONES_18W_DAYS = 18 * 7;

/**
 * Devuelve reseñas unificadas paginadas, ordenadas por fecha descendente.
 * Usa UNION ALL para que la paginación sea correcta entre las dos tablas.
 * @param {{ source?: string, alojamiento?: string, limit?: number, offset?: number, timeWindow?: 'month'|'week' }} opts
 */
async function getReviews({ source, alojamiento, limit = 50, offset = 0, timeWindow, fromDate, toDate, notaMin, notaMax } = {}) {
  const client = getClient();
  if (!client) return [];

  const conditions = [];
  const filterArgs = [];
  if (source) { conditions.push("source = ?"); filterArgs.push(source); }
  if (alojamiento) { conditions.push("alojamiento = ?"); filterArgs.push(alojamiento); }
  // Filtro de nota sobre escala normalizada 0-10 (Google se multiplica x2 en la subquery)
  if (notaMin != null) { conditions.push("rating_norm >= ?"); filterArgs.push(notaMin); }
  if (notaMax != null) { conditions.push("rating_norm <= ?"); filterArgs.push(notaMax); }
  if (fromDate && toDate) {
    conditions.push("date(review_date) BETWEEN ? AND ?");
    filterArgs.push(fromDate, toDate);
  } else if (fromDate) {
    conditions.push("date(review_date) >= ?");
    filterArgs.push(fromDate);
  } else if (toDate) {
    conditions.push("date(review_date) <= ?");
    filterArgs.push(toDate);
  } else if (timeWindow === "week") {
    conditions.push(`date(review_date) >= date('now', '-${OPINIONES_18W_DAYS} days')`);
  } else if (timeWindow === "month") {
    conditions.push("date(review_date) >= date('now', '-18 months')");
  }
  const whereClause = conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";

  const rs = await client.execute({
    sql: `SELECT source, alojamiento, review_date, author, rating, rating_max, rating_norm,
                 titulo, comentario_positivo, comentario_negativo, review_text, resumen, scraped_at,
                 personal, limpieza, ubicacion, instalaciones, confort, relacion_calidad_precio
          FROM (
            SELECT 'booking'        AS source,
                   alojamiento,
                   fecha_comentario AS review_date,
                   nombre_cliente   AS author,
                   puntuacion       AS rating,
                   10.0             AS rating_max,
                   puntuacion       AS rating_norm,
                   titulo,
                   comentario_positivo,
                   comentario_negativo,
                   NULL             AS review_text,
                   resumen,
                   scraped_at,
                   personal,
                   limpieza,
                   ubicacion,
                   instalaciones,
                   confort,
                   relacion_calidad_precio
            FROM booking_reviews
            UNION ALL
            SELECT 'google',
                   alojamiento,
                   review_date,
                   author,
                   rating,
                   5.0,
                   rating * 2.0     AS rating_norm,
                   NULL, NULL, NULL,
                   COALESCE(NULLIF(message_original, ''), review) AS review_text,
                   resumen,
                   scraped_at,
                   NULL, NULL, NULL, NULL, NULL, NULL
            FROM google_reviews
          ) combined
          ${whereClause}
          ORDER BY review_date DESC, scraped_at DESC
          LIMIT ? OFFSET ?`,
    args: [...filterArgs, limit > 0 ? limit : -1, offset],
  });

  return rs.rows.map((r) => {
    let text = null;
    if (r.source === "booking") {
      const parts = [];
      if (r.titulo) parts.push(r.titulo);
      if (r.comentario_positivo) parts.push(r.comentario_positivo);
      if (r.comentario_negativo) parts.push("— " + r.comentario_negativo);
      text = parts.join("\n") || null;
    } else {
      text = r.review_text;
    }
    const resumen = r.resumen != null && String(r.resumen).trim() !== "" ? String(r.resumen).trim() : null;
    const obj = {
      source: r.source,
      alojamiento: r.alojamiento,
      review_date: r.review_date,
      author: r.author,
      text,
      resumen,
      rating: r.rating != null ? Number(r.rating) : null,
      rating_max: Number(r.rating_max),
      scraped_at: r.scraped_at,
    };
    if (r.source === "booking") {
      obj.titulo              = r.titulo              || null;
      obj.comentario_positivo = r.comentario_positivo || null;
      obj.comentario_negativo = r.comentario_negativo || null;
      obj.personal            = r.personal            != null ? Number(r.personal)            : null;
      obj.limpieza            = r.limpieza            != null ? Number(r.limpieza)            : null;
      obj.ubicacion           = r.ubicacion           != null ? Number(r.ubicacion)           : null;
      obj.instalaciones       = r.instalaciones       != null ? Number(r.instalaciones)       : null;
      obj.confort             = r.confort             != null ? Number(r.confort)             : null;
      obj.relacion_calidad_precio = r.relacion_calidad_precio != null ? Number(r.relacion_calidad_precio) : null;
    }
    return obj;
  });
}

/**
 * Devuelve el total de reseñas y el desglose por fuente.
 * @param {{ source?: string, alojamiento?: string, timeWindow?: 'month'|'week' }} opts
 */
async function getReviewsTotal({ source, alojamiento, timeWindow } = {}) {
  const client = getClient();
  if (!client) return { total: 0, counts: {}, alojCounts: {} };

  const srcParts = [];
  const srcArgs = [];
  if (alojamiento) {
    srcParts.push("alojamiento = ?");
    srcArgs.push(alojamiento);
  }
  if (timeWindow === "week") {
    srcParts.push(`date(review_date) >= date('now', '-${OPINIONES_18W_DAYS} days')`);
  } else if (timeWindow === "month") {
    srcParts.push("date(review_date) >= date('now', '-18 months')");
  }
  const srcWhere = srcParts.length ? `WHERE ${srcParts.join(" AND ")}` : "";

  const alojParts = [];
  const alojArgs = [];
  if (source) {
    alojParts.push("source = ?");
    alojArgs.push(source);
  }
  if (timeWindow === "week") {
    alojParts.push(`date(review_date) >= date('now', '-${OPINIONES_18W_DAYS} days')`);
  } else if (timeWindow === "month") {
    alojParts.push("date(review_date) >= date('now', '-18 months')");
  }
  const alojWhere = alojParts.length ? `WHERE ${alojParts.join(" AND ")}` : "";

  const rsSrc = await client.execute({
    sql: `SELECT source, COUNT(*) AS n
          FROM (
            SELECT 'booking' AS source, alojamiento, fecha_comentario AS review_date FROM booking_reviews
            UNION ALL
            SELECT 'google', alojamiento, review_date FROM google_reviews
          ) combined
          ${srcWhere}
          GROUP BY source`,
    args: srcArgs,
  });

  const rsAloj = await client.execute({
    sql: `SELECT alojamiento, COUNT(*) AS n
          FROM (
            SELECT 'booking' AS source, alojamiento, fecha_comentario AS review_date FROM booking_reviews
            UNION ALL
            SELECT 'google', alojamiento, review_date FROM google_reviews
          ) combined
          ${alojWhere}
          GROUP BY alojamiento`,
    args: alojArgs,
  });

  const counts = {};
  let total = 0;
  for (const r of rsSrc.rows) {
    counts[r.source] = Number(r.n);
    total += Number(r.n);
  }

  const alojCounts = {};
  for (const r of rsAloj.rows) {
    alojCounts[r.alojamiento] = Number(r.n);
  }

  return { total, counts, alojCounts };
}

/**
 * Devuelve estadísticas mensuales: número de opiniones y nota media normalizada a /10.
 * @param {{ source?: string, alojamiento?: string, timeWindow?: 'month'|'week' }} opts
 */
async function getReviewsMonthly({ source, alojamiento, timeWindow } = {}) {
  const client = getClient();
  if (!client) return [];

  const bookingDate =
    timeWindow === "week"
      ? ` AND fecha_comentario IS NOT NULL AND date(fecha_comentario) >= date('now', '-${OPINIONES_18W_DAYS} days')`
      : timeWindow === "month"
        ? " AND fecha_comentario IS NOT NULL AND date(fecha_comentario) >= date('now', '-18 months')"
        : "";
  const googleDate =
    timeWindow === "week"
      ? ` AND review_date IS NOT NULL AND date(review_date) >= date('now', '-${OPINIONES_18W_DAYS} days')`
      : timeWindow === "month"
        ? " AND review_date IS NOT NULL AND date(review_date) >= date('now', '-18 months')"
        : "";

  const conditions = [];
  const filterArgs = [];
  if (source)      { conditions.push("source = ?");      filterArgs.push(source); }
  if (alojamiento) { conditions.push("alojamiento = ?"); filterArgs.push(alojamiento); }
  const whereClause = conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";

  const rs = await client.execute({
    sql: `SELECT month, COUNT(*) AS count, AVG(rating_norm) AS avg_rating
          FROM (
            SELECT 'booking'                              AS source,
                   alojamiento,
                   strftime('%Y-%m', fecha_comentario)    AS month,
                   (puntuacion / 10.0) * 10.0             AS rating_norm
            FROM booking_reviews
            WHERE puntuacion IS NOT NULL${bookingDate}
            UNION ALL
            SELECT 'google',
                   alojamiento,
                   strftime('%Y-%m', review_date),
                   (rating / 5.0) * 10.0
            FROM google_reviews
            WHERE rating IS NOT NULL${googleDate}
          ) combined
          ${whereClause}
          GROUP BY month
          ORDER BY month ASC`,
    args: filterArgs,
  });

  return rs.rows.map((r) => ({
    month: r.month,
    count: Number(r.count),
    avg_rating: r.avg_rating != null ? Math.round(Number(r.avg_rating) * 100) / 100 : null,
  }));
}

/**
 * Estadísticas por semana (año + número de semana %W de SQLite, domingo inicio de semana).
 * @param {{ source?: string, alojamiento?: string, timeWindow?: 'month'|'week' }} opts
 */
async function getReviewsWeekly({ source, alojamiento, timeWindow } = {}) {
  const client = getClient();
  if (!client) return [];

  /* Misma ventana que la vista mensual (18 meses): un corte de 18×7 días deja la gráfica vacía
   * si no hay reseñas en el último mes. La agregación es semanal; el tope de 18 barras va en la API. */
  const bookingDate =
    timeWindow === "week" || timeWindow === "month"
      ? " AND fecha_comentario IS NOT NULL AND date(fecha_comentario) >= date('now', '-18 months')"
      : "";
  const googleDate =
    timeWindow === "week" || timeWindow === "month"
      ? " AND review_date IS NOT NULL AND date(review_date) >= date('now', '-18 months')"
      : "";

  /* Semana calendario SQLite %W (domingo como inicio de semana 0–53), portable en libSQL/Turso.
   * Evita %G/%V que en algunos motores no devuelven ISO correctamente. */
  const weekExprBooking = `strftime('%Y', date(fecha_comentario)) || '-W' || printf('%02d', cast(strftime('%W', date(fecha_comentario)) AS int))`;
  const weekExprGoogle = `strftime('%Y', date(review_date)) || '-W' || printf('%02d', cast(strftime('%W', date(review_date)) AS int))`;

  const conditions = [];
  const filterArgs = [];
  if (source)      { conditions.push("source = ?");      filterArgs.push(source); }
  if (alojamiento) { conditions.push("alojamiento = ?"); filterArgs.push(alojamiento); }
  const whereClause = conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";

  const rs = await client.execute({
    sql: `SELECT week, COUNT(*) AS count, AVG(rating_norm) AS avg_rating
          FROM (
            SELECT 'booking'                              AS source,
                   alojamiento,
                   ${weekExprBooking}                     AS week,
                   (puntuacion / 10.0) * 10.0             AS rating_norm
            FROM booking_reviews
            WHERE puntuacion IS NOT NULL${bookingDate}
            UNION ALL
            SELECT 'google',
                   alojamiento,
                   ${weekExprGoogle},
                   (rating / 5.0) * 10.0
            FROM google_reviews
            WHERE rating IS NOT NULL${googleDate}
          ) combined
          ${whereClause}
          GROUP BY week
          HAVING week IS NOT NULL AND week != ''
          ORDER BY week ASC`,
    args: filterArgs,
  });

  return rs.rows.map((r) => ({
    week: r.week != null ? String(r.week) : null,
    count: Number(r.count),
    avg_rating: r.avg_rating != null ? Math.round(Number(r.avg_rating) * 100) / 100 : null,
  }));
}

/**
 * Timestamp de la última vez que se guardó algo (scraped_at más reciente).
 */
async function getLastReviewScrapedAt(alojamiento = DEFAULT_ALOJAMIENTO) {
  const client = getClient();
  if (!client) return null;

  const rs = await client.execute({
    sql: `SELECT MAX(scraped_at) AS last_at FROM (
           SELECT scraped_at FROM booking_reviews WHERE alojamiento = ?
           UNION ALL
           SELECT scraped_at FROM google_reviews WHERE alojamiento = ?
         )`,
    args: [alojamiento, alojamiento],
  });
  return rs.rows[0]?.last_at ?? null;
}

/**
 * Devuelve solo las filas de Booking que NO están ya en la BD.
 * Requiere numero_reserva — las filas sin él se descartan.
 */
async function filterNewBookingRows(alojamiento, rows) {
  const client = getClient();
  if (!client) return rows;

  // Descartar filas sin número de reserva
  const rowsConReserva = rows.filter((r) => String(r["Número de reserva"] || "").trim());
  if (!rowsConReserva.length) return [];

  const reservas = [...new Set(rowsConReserva.map((r) => String(r["Número de reserva"]).trim()))];
  const placeholders = reservas.map(() => "?").join(",");
  const { rows: existing } = await client.execute({
    sql: `SELECT numero_reserva FROM booking_reviews WHERE alojamiento=? AND numero_reserva IN (${placeholders})`,
    args: [alojamiento, ...reservas],
  });

  const existingSet = new Set(existing.map((r) => String(r.numero_reserva)));
  return rowsConReserva.filter((r) => !existingSet.has(String(r["Número de reserva"]).trim()));
}

/**
 * Devuelve solo las filas de Google que NO están ya en la BD.
 * Usa review_url como clave — las filas sin ella se descartan.
 */
async function filterNewGoogleRows(alojamiento, rows) {
  const client = getClient();
  if (!client) return rows;

  const rowsConUrl = rows.filter((r) => String(r["Review Url"] || "").trim());
  if (!rowsConUrl.length) return [];

  const urls = [...new Set(rowsConUrl.map((r) => String(r["Review Url"]).trim()))];
  const placeholders = urls.map(() => "?").join(",");
  const { rows: existing } = await client.execute({
    sql: `SELECT review_url FROM google_reviews WHERE alojamiento=? AND review_url IN (${placeholders})`,
    args: [alojamiento, ...urls],
  });

  const existingSet = new Set(existing.map((r) => String(r.review_url)));
  return rowsConUrl.filter((r) => !existingSet.has(String(r["Review Url"]).trim()));
}

/**
 * Importa filas del CSV oficial de Booking ("reviews.csv").
 * Cada fila debe tener las claves tal cual vienen en la cabecera del CSV.
 */
async function importBookingCsv(alojamiento, rows, client = null) {
  const db = client || getFreshClient();
  if (!db) return { inserted: 0, skipped: 0, errors: 0 };

  if (!rows.length) return { inserted: 0, skipped: 0, errors: 0 };

  const now = new Date().toISOString();

  const stmts = rows.map((r) => ({
    sql: `INSERT OR IGNORE INTO booking_reviews
          (alojamiento, fecha_comentario, nombre_cliente, numero_reserva, titulo,
           comentario_positivo, comentario_negativo, puntuacion,
           personal, limpieza, ubicacion, instalaciones, confort, relacion_calidad_precio,
           respuesta_alojamiento, scraped_at, resumen)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    args: [
      alojamiento,
      toText(r["Fecha del comentario"]),
      toText(r["Nombre del cliente"]),
      toText(r["Número de reserva"]),
      toText(r._titulo_es || r["Título del comentario"]),
      toText(r._pos_es ?? r["Comentario positivo"]),
      toText(r._neg_es ?? r["Comentario negativo"]),
      toNumber(r["Puntuación del comentario"]),
      toNumber(r["Personal"]),
      toNumber(r["Limpieza"]),
      toNumber(r["Ubicación"]),
      toNumber(r["Instalaciones y servicios"]),
      toNumber(r["Confort"]),
      toNumber(r["Relación calidad-precio"]),
      toText(r["Respuesta del alojamiento"]),
      now,
      toText(r._resumen),
    ],
  }));

  try {
    const results = await db.batch(stmts, "write");
    const inserted = results.reduce((n, r) => n + (r.rowsAffected || 0), 0);
    return { inserted, skipped: rows.length - inserted, errors: 0 };
  } catch (err) {
    console.error("[db] Error en batch Booking:", err.message);
    return { inserted: 0, skipped: 0, errors: rows.length };
  }
}

/**
 * Importa filas del CSV oficial de Google ("export_*.csv").
 * Cada fila debe tener las claves tal cual vienen en la cabecera del CSV.
 */
async function importGoogleCsv(alojamiento, rows, client = null) {
  const db = client || getFreshClient();
  if (!db) return { inserted: 0, skipped: 0, errors: 0 };

  if (!rows.length) return { inserted: 0, skipped: 0, errors: 0 };

  const now = new Date().toISOString();

  const stmts = rows.map((r) => {
    const helpful = toText(r["Helpful count"]);
    const helpfulNum = helpful == null ? null : parseInt(helpful, 10);
    return {
      sql: `INSERT OR IGNORE INTO google_reviews
            (alojamiento, author, author_description, review_date, rating, helpful_count,
             review, picture_included, language, message_original, language_code,
             reviewer_data, profile_url, review_url, photos, scraped_at, resumen)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      args: [
        alojamiento,
        toText(r["Author"]),
        toText(r["Author Description"]),
        toText(r["Date"]),
        toNumber(r["Rating"]),
        Number.isFinite(helpfulNum) ? helpfulNum : null,
        toText(r["Review"]),
        toText(r["Picture included"]),
        toText(r["Language"]),
        toText(r._text_es ?? r["Message Original"]),
        toText(r["Language code"]),
        toText(r["Reviewer Data"]),
        toText(r["Profile Url"]),
        toText(r["Review Url"]),
        toText(r["Photos"]),
        now,
        toText(r._resumen),
      ],
    };
  });

  try {
    const results = await db.batch(stmts, "write");
    const inserted = results.reduce((n, r) => n + (r.rowsAffected || 0), 0);
    return { inserted, skipped: rows.length - inserted, errors: 0 };
  } catch (err) {
    console.error("[db] Error en batch Google:", err.message);
    return { inserted: 0, skipped: 0, errors: rows.length };
  }
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
  getReviews,
  getReviewsTotal,
  getReviewsMonthly,
  getReviewsWeekly,
  getLastReviewScrapedAt,
  filterNewBookingRows,
  filterNewGoogleRows,
  importBookingCsv,
  importGoogleCsv,
  DEFAULT_ALOJAMIENTO,
};
