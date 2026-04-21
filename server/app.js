const path = require("path");
const express = require("express");
const multer = require("multer");
const cookieParser = require("cookie-parser");
const { router: authRouter, requireAuth } = require("./auth");

const { generateCartasZip } = require("./cartas");
const { generateCompetitionMatrix } = require("./booking");
const { initDb, getHistoryBulk, getLatestRun, getScrapedHotelsToday, saveReviews, getReviews, getLastReviewDates, getLastReviewScrapedAt } = require("./db");
const { DEFAULT_HOTELS } = require("./competitors");
const tareasRouter = require("./tareasApi");
const { scrapeAllReviews } = require("./opiniones");

const app = express();
const upload = multer({ storage: multer.memoryStorage() });
const STATIC_DIR = path.join(__dirname, "static");

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

app.get("/api/opiniones", async (req, res) => {
  try {
    const { source } = req.query;
    const reviews = await getReviews({ source: source || undefined });
    const lastScrapedAt = await getLastReviewScrapedAt();
    return res.json({ reviews, lastScrapedAt });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Error inesperado";
    console.error("Error obteniendo opiniones:", error);
    return res.status(500).json({ error: message });
  }
});

app.post("/api/opiniones/scrape", async (req, res) => {
  try {
    const onlyNew = req.body?.onlyNew !== false; // por defecto solo nuevas

    let fromDates = { booking: null, google: null, tripadvisor: null };
    if (onlyNew) {
      fromDates = await getLastReviewDates();
    }

    const { reviews, errors } = await scrapeAllReviews(fromDates);
    const { inserted } = await saveReviews(reviews);

    return res.json({
      ok: true,
      found: reviews.length,
      inserted,
      errors,
      scrapedAt: new Date().toISOString(),
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Error inesperado";
    console.error("Error scraping opiniones:", error);
    return res.status(500).json({ error: message });
  }
});

app.get("/api/opiniones/config", (_req, res) => {
  return res.json({
    booking: !!process.env.BOOKING_REVIEWS_URL,
    google: !!process.env.GOOGLE_REVIEWS_URL,
    tripadvisor: !!process.env.TRIPADVISOR_REVIEWS_URL,
  });
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
