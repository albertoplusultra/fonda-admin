const path = require("path");
const express = require("express");
const multer = require("multer");
const cookieParser = require("cookie-parser");
const { router: authRouter, requireAuth } = require("./auth");
const OpenAI = require("openai");

const { generateCartasZip } = require("./cartas");
const { generateCompetitionMatrix } = require("./booking");
const { createClient } = require("@libsql/client");
const { initDb, getHistoryBulk, getLatestRun, getScrapedHotelsToday, getReviews, getReviewsTotal, getReviewsMonthly, getReviewsWeekly, getLastReviewScrapedAt, filterNewBookingRows, filterNewGoogleRows, importBookingCsv, importGoogleCsv } = require("./db");
const { DEFAULT_HOTELS } = require("./competitors");
const { listAlojamientos, getAlojamientoByName } = require("./alojamientos");
const { parseBookingCsv, parseGoogleCsv, detectCsvSource } = require("./reviewsCsv");
const { enrichBookingRows, enrichGoogleRows } = require("./aiReviews");
const tareasRouter = require("./tareasApi");

const app = express();
const upload = multer({ storage: multer.memoryStorage() });
const STATIC_DIR = path.join(__dirname, "static");

// Estado de progreso del import activo (solo uno a la vez)
let importProgress = { active: false, processed: 0, total: 0, source: "" };

app.use(cookieParser());
app.use(express.json({ limit: "32kb" }));

// Archivos estáticos públicos (logo, favicon, css…) — sin autenticación
app.use("/static", express.static(STATIC_DIR));

// Rutas de auth (públicas: /login, /auth/google, /auth/callback, /auth/logout)
app.use(authRouter);

// A partir de aquí, todo requiere sesión activa
app.use(requireAuth);

app.get("/api/me", (req, res) => res.json({ email: req.user.email, name: req.user.name }));

app.use("/api/tareas", tareasRouter);

initDb().catch((err) => console.error("Error inicializando BD:", err));

app.get("/", (_req, res) => {
  res.sendFile(path.join(STATIC_DIR, "index.html"));
});

app.post("/api/generar-cartas", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: "No se envio ningun archivo." });
    }

    if (!req.file.originalname.match(/\.xlsx?$/i)) {
      return res.status(400).json({ error: "Solo se aceptan archivos Excel (.xlsx)." });
    }

    const { zipBuffer, report } = await generateCartasZip(req.file.buffer);

    res.setHeader("Content-Type", "application/zip");
    res.setHeader("Content-Disposition", "attachment; filename=Cartas_Bienvenida.zip");
    res.setHeader("X-Cartas-Count", String(report.length));
    return res.status(200).send(zipBuffer);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Error inesperado";
    console.error("Error al generar cartas:", error);
    const isUserError =
      message.includes("No se encontro la columna") ||
      message.includes("No se encontró la columna") ||
      message.includes("Excel esta vacio") ||
      message.includes("Excel está vacío") ||
      message.includes("No se encontraron huespedes validos") ||
      message.includes("No se encontraron huéspedes válidos") ||
      message.includes("No se encontro el logotipo") ||
      message.includes("No se encontró el logotipo");

    return res.status(isUserError ? 422 : 500).json({ error: message });
  }
});

app.post("/api/precios-competencia/matriz", async (req, res) => {
  try {
    const hotels = Array.isArray(req.body?.hotels) ? req.body.hotels : [];
    if (!hotels.length) {
      return res.status(400).json({ error: "No se han enviado hoteles para analizar." });
    }

    const matrix = await generateCompetitionMatrix({
      hotels,
      days: req.body?.days ?? 15,
      startDate: req.body?.startDate,
    });

    return res.status(200).json(matrix);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Error inesperado";
    const stack = error instanceof Error ? error.stack : "";
    console.error("Error en /api/precios-competencia/matriz:", message, stack);
    return res.status(422).json({ error: message });
  }
});

app.get("/api/precios-competencia/ultimo", async (_req, res) => {
  try {
    const data = await getLatestRun();
    if (!data) return res.json(null);
    return res.json(data);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Error inesperado";
    console.error("Error obteniendo última ejecución:", error);
    return res.status(500).json({ error: message });
  }
});

app.get("/api/precios-competencia/historial", async (req, res) => {
  try {
    const hotelsParam = req.query.hotels;
    const datesParam = req.query.dates;

    if (!hotelsParam || !datesParam) {
      return res.status(400).json({ error: "Faltan parámetros hotels y/o dates." });
    }

    const hotels = hotelsParam.split(",").map((h) => h.trim()).filter(Boolean);
    const dates = datesParam.split(",").map((d) => d.trim()).filter(Boolean);

    if (!hotels.length || !dates.length) {
      return res.status(400).json({ error: "Los parámetros hotels y dates no pueden estar vacíos." });
    }

    const history = await getHistoryBulk(hotels, dates, 7);
    return res.json(history);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Error inesperado";
    console.error("Error obteniendo historial:", error);
    return res.status(500).json({ error: message });
  }
});

app.get("/api/precios-competencia/pendientes", async (_req, res) => {
  try {
    const done = await getScrapedHotelsToday();
    const pending = DEFAULT_HOTELS.filter((h) => !done.has(h.name));
    return res.json({ pending, done: DEFAULT_HOTELS.filter((h) => done.has(h.name)) });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Error inesperado";
    console.error("Error obteniendo pendientes:", error);
    return res.status(500).json({ error: message });
  }
});

// ─── Opiniones ──────────────────────────────────────────────────────────────

app.get("/api/alojamientos", (_req, res) => {
  return res.json({ alojamientos: listAlojamientos() });
});

app.get("/api/opiniones", async (req, res) => {
  try {
    const source      = req.query.source      || undefined;
    const alojamiento = req.query.alojamiento || undefined;
    const limit       = Math.min(parseInt(req.query.limit,  10) || 50, 200);
    const offset      = Math.max(parseInt(req.query.offset, 10) || 0,  0);
    const granRaw     = String(req.query.granularity || "month").toLowerCase();
    const timeWindow  = granRaw === "week" ? "week" : "month";

    const reviews = await getReviews({ source, alojamiento, limit, offset, timeWindow });
    const { total, counts, alojCounts } = await getReviewsTotal({ source, alojamiento, timeWindow });
    const lastScrapedAt = await getLastReviewScrapedAt();

    return res.json({
      reviews,
      total,
      counts,
      alojCounts,
      hasMore: offset + reviews.length < total,
      lastScrapedAt,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Error inesperado";
    console.error("Error obteniendo opiniones:", error);
    return res.status(500).json({ error: message });
  }
});

app.get("/api/opiniones/stats", async (req, res) => {
  try {
    const source      = req.query.source      || undefined;
    const alojamiento = req.query.alojamiento || undefined;
    const granRaw     = String(req.query.granularity || "month").toLowerCase();
    const granularity = granRaw === "week" ? "week" : "month";
    const rows =
      granularity === "week"
        ? await getReviewsWeekly({ source, alojamiento, timeWindow: granularity })
        : await getReviewsMonthly({ source, alojamiento, timeWindow: granularity });
    const periodsAll = rows.map((r) => ({
      period: granularity === "week" ? r.week : r.month,
      count: r.count,
      avg_rating: r.avg_rating,
    }));
    const periods =
      periodsAll.length > 18 ? periodsAll.slice(-18) : periodsAll;
    return res.json({ granularity, periods });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Error inesperado";
    console.error("Error obteniendo estadísticas:", error);
    return res.status(500).json({ error: message });
  }
});

app.post("/api/opiniones/ask", async (req, res) => {
  try {
    const pregunta    = String(req.body?.pregunta || "").trim();
    const source      = req.body?.source      || undefined;
    const alojamiento = req.body?.alojamiento || undefined;
    const limitReq    = parseInt(req.body?.limit, 10) || 0;
    const limit       = limitReq > 0 ? limitReq : 500;
    const usarResumen = String(req.body?.texto || "resumen") !== "completo";

    if (!pregunta) {
      return res.status(400).json({ error: "Falta la pregunta." });
    }

    if (!process.env.OPENAI_API_KEY) {
      return res.status(503).json({ error: "El asistente IA no está configurado (falta OPENAI_API_KEY)." });
    }

    const granAsk = String(req.body?.granularity || "month").toLowerCase();
    const timeWindow = granAsk === "week" ? "week" : "month";
    const reviews = await getReviews({ source, alojamiento, limit, offset: 0, timeWindow });

    if (!reviews.length) {
      return res.json({ respuesta: "No hay opiniones que coincidan con los filtros seleccionados." });
    }

    const textos = reviews
      .filter((r) => usarResumen ? r.resumen : r.text)
      .map((r, i) => {
        const fecha = r.review_date ? r.review_date.slice(0, 10) : "sin fecha";
        const fuente = r.source === "booking" ? "Booking" : "Google";
        const edificio = (r.alojamiento && String(r.alojamiento).trim()) || "—";
        const nota = r.rating != null ? ` | Nota: ${r.rating}/${r.rating_max}` : "";
        const contenido = usarResumen ? r.resumen : r.text.replace(/\n+/g, " ");
        return `[${i + 1}] (${fuente}, ${edificio}, ${fecha}${nota}) ${contenido}`;
      })
;

    if (!textos.length) {
      return res.json({ respuesta: "Las opiniones filtradas no tienen texto suficiente para analizar." });
    }

    const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

    const completion = await openai.chat.completions.create({
      model: "gpt-4o-mini",
      max_tokens: 600,
      messages: [
        {
          role: "system",
          content:
            "Eres un asistente que analiza opiniones de huéspedes de hoteles. " +
            "Cada opinión indica fuente (Booking/Google), edificio o alojamiento, fecha y a veces nota. " +
            "Responde siempre en español, de forma clara y concisa. " +
            "Basa tu respuesta únicamente en las opiniones proporcionadas.",
        },
        {
          role: "user",
          content:
            `Tienes ${textos.length} opiniones de huéspedes:\n\n` +
            textos.join("\n") +
            `\n\nPREGUNTA: ${pregunta}`,
        },
      ],
    });

    const respuesta = completion.choices[0]?.message?.content?.trim() || "Sin respuesta.";

    const usage = completion.usage;
    const coste = usage
      ? (usage.prompt_tokens * 0.15 + usage.completion_tokens * 0.60) / 1_000_000
      : null;

    return res.json({
      respuesta,
      total: reviews.length,
      analizadas: textos.length,
      tokens: usage ? { entrada: usage.prompt_tokens, salida: usage.completion_tokens, total: usage.total_tokens } : null,
      coste_usd: coste != null ? Math.round(coste * 1_000_000) / 1_000_000 : null,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Error inesperado";
    console.error("Error en asistente IA:", error);
    return res.status(500).json({ error: message });
  }
});

app.get("/api/opiniones/import-progress", (_req, res) => {
  res.json(importProgress);
});

app.post("/api/opiniones/import-csv", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: "Falta el archivo CSV." });
    }

    const alojamiento = String(req.body.alojamiento || "").trim();
    if (!alojamiento) {
      return res.status(400).json({ error: "Falta el alojamiento." });
    }
    if (!getAlojamientoByName(alojamiento)) {
      return res.status(400).json({ error: `Alojamiento desconocido: ${alojamiento}` });
    }

    const buffer = req.file.buffer;
    let source = String(req.body.source || "auto").trim().toLowerCase();
    if (source === "auto" || !["booking", "google"].includes(source)) {
      source = detectCsvSource(buffer);
      if (!source) {
        return res.status(400).json({
          error: "No se pudo detectar la fuente (Booking/Google) a partir de las cabeceras del CSV.",
        });
      }
    }

    // Cliente de BD compartido para todos los lotes (se crea tras el filtrado, antes de la IA)
    const dbClient = createClient({
      url: process.env.TURSO_DATABASE_URL || process.env.DB_TURSO_DATABASE_URL,
      authToken: process.env.TURSO_AUTH_TOKEN || process.env.DB_TURSO_AUTH_TOKEN,
    });

    let totInserted = 0, totSkipped = 0, totErrors = 0;

    if (source === "booking") {
      const rawRows = parseBookingCsv(buffer);
      const newRows = await filterNewBookingRows(alojamiento, rawRows);
      const skippedExisting = rawRows.length - newRows.length;
      totSkipped += skippedExisting;
      console.log(`[import] Booking: ${rawRows.length} en CSV, ${newRows.length} nuevas, ${skippedExisting} ya en BD`);
      importProgress = { active: true, processed: 0, total: newRows.length, source: "Booking" };

      if (newRows.length > 0) {
        await enrichBookingRows(newRows, {
          onProgress: (p, t) => { importProgress.processed = p; importProgress.total = t; },
          onBatch: async (batch) => {
            const r = await importBookingCsv(alojamiento, batch, dbClient);
            totInserted += r.inserted; totSkipped += r.skipped; totErrors += r.errors;
          },
        });
      }
      importProgress.active = false;
      return res.json({ ok: true, source, alojamiento, read: rawRows.length, inserted: totInserted, skipped: totSkipped, errors: totErrors });
    } else {
      const rawRows = parseGoogleCsv(buffer);
      const newRows = await filterNewGoogleRows(alojamiento, rawRows);
      const skippedExisting = rawRows.length - newRows.length;
      totSkipped += skippedExisting;
      console.log(`[import] Google: ${rawRows.length} en CSV, ${newRows.length} nuevas, ${skippedExisting} ya en BD`);
      importProgress = { active: true, processed: 0, total: newRows.length, source: "Google" };

      if (newRows.length > 0) {
        await enrichGoogleRows(newRows, {
          onProgress: (p, t) => { importProgress.processed = p; importProgress.total = t; },
          onBatch: async (batch) => {
            const r = await importGoogleCsv(alojamiento, batch, dbClient);
            totInserted += r.inserted; totSkipped += r.skipped; totErrors += r.errors;
          },
        });
      }
      importProgress.active = false;
      return res.json({ ok: true, source, alojamiento, read: rawRows.length, inserted: totInserted, skipped: totSkipped, errors: totErrors });
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : "Error inesperado";
    console.error("Error importando CSV:", error);
    return res.status(500).json({ error: message });
  }
});

// ────────────────────────────────────────────────────────────────────────────

app.get("/api/cron/scrape", async (req, res) => {
  const secret = process.env.CRON_SECRET;
  if (secret) {
    const auth = req.headers["authorization"];
    if (auth !== `Bearer ${secret}`) {
      return res.status(401).json({ error: "No autorizado." });
    }
  }

  try {
    console.log("[cron] Inicio scraping automático…");
    const done = await getScrapedHotelsToday();
    const pending = DEFAULT_HOTELS.filter((h) => !done.has(h.name));
    if (!pending.length) {
      console.log("[cron] Todos los hoteles ya scrapeados hoy.");
      return res.json({ ok: true, hotels: 0, message: "Ya scrapeados hoy." });
    }

    const results = [];
    for (const hotel of pending) {
      try {
        console.log(`[cron] Scraping ${hotel.name}…`);
        await generateCompetitionMatrix({ hotels: [hotel], days: 15 });
        results.push({ hotel: hotel.name, ok: true });
      } catch (err) {
        console.error(`[cron] Error en ${hotel.name}:`, err);
        results.push({ hotel: hotel.name, ok: false, error: err.message });
      }
    }

    const ok = results.filter((r) => r.ok).length;
    const failed = results.filter((r) => !r.ok).length;
    console.log(`[cron] Completado: ${ok} ok, ${failed} errores.`);
    return res.json({ ok: true, hotels: ok, failed, results, scrapedAt: new Date().toISOString() });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Error inesperado";
    console.error("[cron] Error en scraping automático:", error);
    return res.status(500).json({ error: message });
  }
});

module.exports = app;
