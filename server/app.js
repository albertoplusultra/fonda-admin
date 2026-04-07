const path = require("path");
const express = require("express");
const multer = require("multer");

const { generateCartasZip } = require("./cartas");
const { generateCompetitionMatrix } = require("./booking");
const { initDb, getHistoryBulk, getLatestRun } = require("./db");
const { DEFAULT_HOTELS } = require("./competitors");

const app = express();
const upload = multer({ storage: multer.memoryStorage() });
const STATIC_DIR = path.join(__dirname, "static");

app.use(express.json({ limit: "32kb" }));
app.use("/static", express.static(STATIC_DIR));

initDb();

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
    return res.status(422).json({ error: message });
  }
});

app.get("/api/precios-competencia/ultimo", (_req, res) => {
  try {
    const data = getLatestRun();
    if (!data) return res.json(null);
    return res.json(data);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Error inesperado";
    console.error("Error obteniendo última ejecución:", error);
    return res.status(500).json({ error: message });
  }
});

app.get("/api/precios-competencia/historial", (req, res) => {
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

    const history = getHistoryBulk(hotels, dates, 7);
    return res.json(history);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Error inesperado";
    console.error("Error obteniendo historial:", error);
    return res.status(500).json({ error: message });
  }
});

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
    const matrix = await generateCompetitionMatrix({
      hotels: DEFAULT_HOTELS,
      days: 15,
    });
    const count = matrix.hotels.length;
    const dates = matrix.dates.length;
    console.log(`[cron] Scraping completado: ${count} hoteles, ${dates} fechas.`);
    return res.json({ ok: true, hotels: count, dates, scrapedAt: new Date().toISOString() });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Error inesperado";
    console.error("[cron] Error en scraping automático:", error);
    return res.status(500).json({ error: message });
  }
});

module.exports = app;
