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

// ── Planificación IA: interpreta la pregunta y devuelve parámetros de consulta ──
app.post("/api/opiniones/plan", async (req, res) => {
  console.log("[plan] recibida petición, body:", JSON.stringify(req.body));
  try {
    const pregunta    = String(req.body?.pregunta || "").trim();
    // Filtros manuales que ya están fijados (si los hay, se respetan tal cual)
    const forzarAloj   = req.body?.alojamiento || null;
    const forzarSource = req.body?.source      || null;
    const forzarTexto  = req.body?.texto       || null; // "resumen"|"completo"|"solo_nota"|"subcategorias"
    const forzarLimit  = parseInt(req.body?.limit, 10) || 0;

    if (!pregunta) return res.status(400).json({ error: "Falta la pregunta." });
    if (!process.env.OPENAI_API_KEY) {
      return res.status(503).json({ error: "El asistente IA no está configurado (falta OPENAI_API_KEY)." });
    }

    const hoy = new Date().toISOString().slice(0, 10);

    const esquema = `
ESQUEMA BASE DE DATOS (SQLite):

booking_reviews:
  alojamiento TEXT               — nombre del alojamiento
  fecha_comentario TEXT          — fecha de la opinión (YYYY-MM-DD)
  puntuacion REAL (0-10)         — nota global
  personal REAL (0-10)           — nota de personal/atención
  limpieza REAL (0-10)           — nota de limpieza
  ubicacion REAL (0-10)          — nota de ubicación
  instalaciones REAL (0-10)      — nota de instalaciones/equipamiento
  confort REAL (0-10)            — nota de confort
  relacion_calidad_precio REAL (0-10)
  titulo TEXT, comentario_positivo TEXT, comentario_negativo TEXT
  resumen TEXT                   — resumen generado por IA (≈2 frases)
  [SOLO Booking tiene subcategorías de puntuación]

google_reviews:
  alojamiento TEXT
  review_date TEXT (YYYY-MM-DD)
  rating REAL (0-5)              — nota global
  review TEXT                    — texto completo
  resumen TEXT                   — resumen generado por IA
  [Google NO tiene subcategorías de puntuación]

VALORES POSIBLES:
  alojamiento: "La Fonda de los Príncipes" | "Iconic Suites" | "Miosotis Suites" | "The Garden Suites"
  source:      "booking" | "google"

MODOS DE TEXTO (modoTexto):
  "resumen"       — campo resumen (texto corto ~2 frases). Usar por defecto para preguntas cualitativas generales.
  "completo"      — texto íntegro. Usar cuando se necesite máximo detalle del contenido: nombres de empleados mencionados, anécdotas concretas, frases literales.
  "solo_nota"     — solo puntuación global + fecha. Para tendencias de nota, evolución, comparativa de ratings globales.
  "subcategorias" — SOLO puntuaciones numéricas por categoría de Booking (personal, limpieza, ubicación, instalaciones, confort, calidad-precio). NO contiene texto ni nombres. Implica source="booking". Usar ÚNICAMENTE para comparar medias numéricas por categoría. NUNCA usar para preguntas sobre nombres de empleados o contenido cualitativo.

FILTRO DE NOTA (notaMin / notaMax):
  Usa notaMin y/o notaMax cuando la pregunta mencione "nota por encima de X", "nota por debajo de X", "malas notas", "bajas puntuaciones", etc.
  SIEMPRE usa escala 0-10, independientemente de la fuente. Google se normaliza internamente a 0-10 antes de filtrar.
  Ejemplo: "opiniones con nota por debajo de 5" → notaMax: 4.9
  Ejemplo: "opiniones con nota menor de 4" → notaMax: 3.9

REGLA CRÍTICA: Si la pregunta menciona empleados, personas concretas o nombres de staff, usar SIEMPRE modoTexto="completo" (source=null para buscar en ambas fuentes). "subcategorias" NO contiene nombres.
`;

    // Contexto de la pregunta anterior (para preguntas de seguimiento)
    const contextoAnterior = req.body?.contextoAnterior ? String(req.body.contextoAnterior).trim() : null;

    const instrucciones = [
      forzarAloj   ? `El filtro de alojamiento ya está fijado a: "${forzarAloj}". Usa ese valor.` : "",
      forzarSource ? `El filtro de fuente ya está fijado a: "${forzarSource}". Usa ese valor.` : "",
      forzarTexto  ? `El modo de texto ya está fijado a: "${forzarTexto}". Usa ese valor.` : "",
      forzarLimit  ? `El límite de opiniones ya está fijado a: ${forzarLimit}. Usa ese valor.` : "",
      contextoAnterior
        ? `CONTEXTO DE SEGUIMIENTO — La nueva pregunta viene después de esta conversación:\n${contextoAnterior}\n` +
          `Interpreta referencias implícitas (p.ej. "las críticas de ese edificio" → el edificio mencionado antes). ` +
          `Reutiliza el rango de fechas anterior salvo que la nueva pregunta indique otro. ` +
          `Elige el modoTexto más adecuado para la NUEVA pregunta (puede cambiar respecto al anterior).`
        : "",
    ].filter(Boolean).join("\n");

    const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

    const completion = await openai.chat.completions.create({
      model: "gpt-4o",
      max_tokens: 350,
      response_format: { type: "json_object" },
      messages: [
        {
          role: "system",
          content:
            `Eres un planificador de consultas para una base de datos de opiniones de hoteles. ` +
            `Hoy es ${hoy}. ` +
            `Dado el esquema y la pregunta del usuario, devuelve un JSON con los parámetros óptimos para consultar la BD. ` +
            `Responde SOLO con JSON válido, sin explicaciones.\n\n` +
            `REGLA DE FECHAS: si la pregunta hace referencia a un período ("este mes", "el mes pasado", "esta semana", "últimos 30 días", etc.), SIEMPRE fija fromDate y toDate con el rango exacto. ` +
            `Si la pregunta pide "las N últimas" sin período, deja fromDate y toDate en null y usa limit=N. ` +
            `Si la pregunta es sobre tendencias históricas ("cada mes", "evolución", "por meses"), usa un rango amplio (ej. últimos 18 meses).\n\n` +
            esquema +
            (instrucciones ? `\n\nFILTROS YA FIJADOS POR EL USUARIO (respétalos):\n${instrucciones}` : ""),
        },
        // Few-shot: empleados/nombres → completo
        {
          role: "user",
          content: `Pregunta: "¿Quiénes son los mejores empleados de la Fonda de los Príncipes en los últimos 30 días?"\n\nDevuelve un JSON con exactamente estos campos:\n{\n  "alojamiento": string|null,\n  "source": "booking"|"google"|null,\n  "fromDate": "YYYY-MM-DD"|null,\n  "toDate": "YYYY-MM-DD"|null,\n  "limit": number,\n  "modoTexto": "resumen"|"completo"|"solo_nota"|"subcategorias",\n  "resumenDetectado": string,\n  "razonamiento": string\n}`,
        },
        {
          role: "assistant",
          content: JSON.stringify({
            alojamiento: "La Fonda de los Príncipes",
            source: null,
            fromDate: new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10),
            toDate: hoy,
            limit: 0,
            modoTexto: "completo",
            resumenDetectado: "Empleados mencionados positivamente en La Fonda (últimos 30 días)",
            razonamiento: "Para encontrar nombres de empleados hay que leer el texto completo de las opiniones. 'subcategorias' solo contiene puntuaciones numéricas, nunca nombres. Se usan ambas fuentes para no perder menciones de Google.",
          }),
        },
        // Few-shot: subcategorías numéricas → subcategorias
        {
          role: "user",
          content: `Pregunta: "¿Cuál es la media de limpieza y personal en Booking este mes?"\n\nDevuelve un JSON con exactamente estos campos:\n{\n  "alojamiento": string|null,\n  "source": "booking"|"google"|null,\n  "fromDate": "YYYY-MM-DD"|null,\n  "toDate": "YYYY-MM-DD"|null,\n  "limit": number,\n  "modoTexto": "resumen"|"completo"|"solo_nota"|"subcategorias",\n  "resumenDetectado": string,\n  "razonamiento": string\n}`,
        },
        {
          role: "assistant",
          content: JSON.stringify({
            alojamiento: null,
            source: "booking",
            fromDate: hoy.slice(0, 7) + "-01",
            toDate: hoy,
            limit: 0,
            modoTexto: "subcategorias",
            resumenDetectado: "Medias de limpieza y personal en Booking este mes",
            razonamiento: "La pregunta pide medias numéricas de categorías específicas de Booking, que es exactamente lo que proporciona el modo 'subcategorias'.",
          }),
        },
        {
          role: "user",
          content:
            `Pregunta: "${pregunta}"\n\n` +
            `Devuelve un JSON con exactamente estos campos:\n` +
            `{\n` +
            `  "alojamiento": string|null,   // nombre exacto del alojamiento o null para todos\n` +
            `  "source": "booking"|"google"|null,  // null para ambas fuentes\n` +
            `  "fromDate": "YYYY-MM-DD"|null, // fecha inicio del rango, null si no aplica\n` +
            `  "toDate": "YYYY-MM-DD"|null,   // fecha fin del rango, null si no aplica\n` +
            `  "limit": number,               // IMPORTANTE: si se especifica fromDate/toDate usa SIEMPRE 0 (traer todos los del rango). Solo usa >0 si la pregunta dice explícitamente "los últimos N" sin rango de fechas.\n` +
            `  "modoTexto": "resumen"|"completo"|"solo_nota"|"subcategorias",\n` +
            `  "notaMin": number|null,         // nota mínima (inclusive) para filtrar en BD. Null si no aplica.\n` +
            `  "notaMax": number|null,         // nota máxima (inclusive) para filtrar en BD. Null si no aplica.\n` +
            `  "resumenDetectado": string,    // frase corta en español describiendo qué se va a consultar\n` +
            `  "razonamiento": string         // explica en 1-3 frases por qué elegiste cada parámetro (alojamiento, fechas, modo de texto, fuente)\n` +
            `}`,
        },
      ],
    });

    let params;
    try {
      params = JSON.parse(completion.choices[0]?.message?.content || "{}");
    } catch {
      params = {};
    }

    // Garantizar valores por defecto seguros
    const result = {
      alojamiento:      forzarAloj   || params.alojamiento   || null,
      source:           forzarSource || params.source        || null,
      fromDate:         params.fromDate  || null,
      toDate:           params.toDate    || null,
      limit:            forzarLimit  || (Number.isFinite(params.limit) ? params.limit : 0),
      modoTexto:        forzarTexto  || params.modoTexto     || "resumen",
      notaMin:          Number.isFinite(params.notaMin) ? params.notaMin : null,
      notaMax:          Number.isFinite(params.notaMax) ? params.notaMax : null,
      resumenDetectado: params.resumenDetectado || "Opiniones de todos los alojamientos",
      razonamiento:     params.razonamiento     || null,
    };

    // Si el modo es "subcategorias", forzar source=booking
    if (result.modoTexto === "subcategorias" && !forzarSource) {
      result.source = "booking";
    }

    return res.json(result);
  } catch (error) {
    const message = (error instanceof Error && error.message) ? error.message : String(error || "Error inesperado");
    console.error("[plan] ERROR:", message, error);
    return res.status(500).json({ error: message || "Error interno en planificación" });
  }
});

// ── Análisis IA: consulta opiniones y responde la pregunta ──
app.post("/api/opiniones/ask", async (req, res) => {
  try {
    const pregunta    = String(req.body?.pregunta || "").trim();
    const source      = req.body?.source      || undefined;
    const alojamiento = req.body?.alojamiento || undefined;
    const limitReq    = parseInt(req.body?.limit, 10) || 0;
    const limit       = limitReq > 0 ? limitReq : 500;
    const modoTexto   = String(req.body?.texto || "resumen");
    const fromDate    = req.body?.fromDate || undefined;
    const toDate      = req.body?.toDate   || undefined;
    const notaMin     = req.body?.notaMin != null ? Number(req.body.notaMin) : undefined;
    const notaMax     = req.body?.notaMax != null ? Number(req.body.notaMax) : undefined;

    if (!pregunta) {
      return res.status(400).json({ error: "Falta la pregunta." });
    }

    if (!process.env.OPENAI_API_KEY) {
      return res.status(503).json({ error: "El asistente IA no está configurado (falta OPENAI_API_KEY)." });
    }

    // El planificador IA ya fija las fechas adecuadas según la pregunta.
    // No aplicamos ninguna ventana de tiempo automática para no interferir.
    const timeWindow = undefined;

    // En modo subcategorias forzar source=booking
    const effectiveSource = (modoTexto === "subcategorias" && !source) ? "booking" : source;

    const reviews = await getReviews({
      source: effectiveSource,
      alojamiento,
      notaMin: Number.isFinite(notaMin) ? notaMin : undefined,
      notaMax: Number.isFinite(notaMax) ? notaMax : undefined,
      limit,
      offset: 0,
      timeWindow,
      fromDate,
      toDate,
    });

    if (!reviews.length) {
      return res.json({ respuesta: "No hay opiniones que coincidan con los filtros seleccionados." });
    }

    let textos;
    let systemPrompt;

    if (modoTexto === "solo_nota") {
      textos = reviews
        .filter((r) => r.rating != null)
        .map((r, i) => {
          const fecha = r.review_date ? r.review_date.slice(0, 10) : "sin fecha";
          const fuente = r.source === "booking" ? "Booking" : "Google";
          const edificio = (r.alojamiento && String(r.alojamiento).trim()) || "—";
          return `[${i + 1}] (${fuente}, ${edificio}, ${fecha}, Nota: ${r.rating}/${r.rating_max})`;
        });
      systemPrompt =
        "Eres un asistente que analiza puntuaciones de opiniones de huéspedes de hoteles. " +
        "Cada entrada indica fuente, edificio, fecha y nota global. No hay texto de opinión. " +
        "Responde siempre en español, de forma clara y concisa. " +
        "Basa tu respuesta únicamente en los datos proporcionados.";
    } else if (modoTexto === "subcategorias") {
      textos = reviews
        .filter((r) => r.source === "booking")
        .map((r, i) => {
          const fecha = r.review_date ? r.review_date.slice(0, 10) : "sin fecha";
          const edificio = (r.alojamiento && String(r.alojamiento).trim()) || "—";
          const cats = [
            r.rating          != null ? `Global:${r.rating}`                  : null,
            r.personal        != null ? `Personal:${r.personal}`              : null,
            r.limpieza        != null ? `Limpieza:${r.limpieza}`              : null,
            r.ubicacion       != null ? `Ubicación:${r.ubicacion}`            : null,
            r.instalaciones   != null ? `Instalaciones:${r.instalaciones}`    : null,
            r.confort         != null ? `Confort:${r.confort}`                : null,
            r.relacion_calidad_precio != null ? `CalidadPrecio:${r.relacion_calidad_precio}` : null,
          ].filter(Boolean).join(" | ");
          const resumenTexto = r.resumen ? ` | "${r.resumen}"` : "";
          return `[${i + 1}] (${edificio}, ${fecha}) ${cats}${resumenTexto}`;
        });
      systemPrompt =
        "Eres un asistente que analiza opiniones de Booking de hoteles. " +
        "Cada entrada indica edificio, fecha, puntuaciones por categoría (Personal, Limpieza, Ubicación, Instalaciones, Confort, CalidadPrecio, escala 0-10) y, cuando está disponible, un resumen textual de la opinión. " +
        "Para cada edificio: calcula la MEDIA de las categorías relevantes e indica el número de opiniones. " +
        "Además, usa los resúmenes textuales para añadir una breve valoración cualitativa de lo que dicen los clientes. " +
        "Incluye TODOS los edificios que aparezcan en los datos. Si un edificio tiene pocas opiniones, indícalo. " +
        "Responde siempre en español, de forma clara y concisa. " +
        "Basa tu respuesta únicamente en los datos proporcionados.";
    } else {
      const usarResumen = modoTexto !== "completo";
      textos = reviews
        .map((r, i) => {
          const fecha = r.review_date ? r.review_date.slice(0, 10) : "sin fecha";
          const fuente = r.source === "booking" ? "Booking" : "Google";
          const edificio = (r.alojamiento && String(r.alojamiento).trim()) || "—";
          const nota = r.rating != null ? ` | Nota: ${r.rating}/${r.rating_max}` : "";
          // Fallback: resumen → texto completo → campos título/positivo/negativo → solo metadata
          let contenido;
          if (usarResumen) {
            contenido = r.resumen
              || r.text
              || [r.titulo, r.comentario_positivo, r.comentario_negativo].filter(Boolean).join(" / ")
              || "(sin texto)";
          } else {
            contenido = r.text
              || [r.titulo, r.comentario_positivo, r.comentario_negativo].filter(Boolean).join(" / ")
              || r.resumen
              || "(sin texto)";
          }
          return `[${i + 1}] (${fuente}, ${edificio}, ${fecha}${nota}) ${contenido}`;
        });
      systemPrompt =
        "Eres un asistente que analiza opiniones de huéspedes de hoteles. " +
        "Cada opinión indica fuente (Booking/Google), edificio o alojamiento, fecha y a veces nota. " +
        "Si la pregunta es sobre empleados o personal concreto, extrae y lista los nombres propios " +
        "de empleados que aparezcan mencionados positivamente en las opiniones, agrupándolos por número de menciones. " +
        "Si no se mencionan nombres en las opiniones, indícalo claramente. " +
        "Responde siempre en español, de forma clara y concisa. " +
        "Basa tu respuesta únicamente en las opiniones proporcionadas.";
    }

    if (!textos.length) {
      return res.json({ respuesta: "Las opiniones filtradas no tienen datos suficientes para analizar." });
    }

    const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

    const historial = Array.isArray(req.body?.history) ? req.body.history : [];
    const datosCtx =
      `Tienes ${textos.length} registros:\n\n` +
      textos.join("\n");

    // Si hay historial anterior, lo incluimos como contexto en el system prompt.
    // Esto evita conflictos entre datos de distintos turnos (cada turno puede
    // tener diferente alojamiento o modo de texto).
    let systemFinal = systemPrompt;
    if (historial.length >= 2) {
      const resumenHistorial = historial
        .map((m) => (m.role === "user" ? `Pregunta previa: ${m.content}` : `Respuesta previa: ${m.content}`))
        .join("\n\n");
      systemFinal +=
        `\n\nCONTEXTO DE LA CONVERSACIÓN ANTERIOR (úsalo para interpretar la nueva pregunta):\n${resumenHistorial}`;
    }

    const mensajes = [
      { role: "system", content: systemFinal },
      { role: "user", content: `${datosCtx}\n\nPREGUNTA: ${pregunta}` },
    ];

    const completion = await openai.chat.completions.create({
      model: "gpt-4o-mini",
      max_tokens: 1200,
      messages: mensajes,
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
