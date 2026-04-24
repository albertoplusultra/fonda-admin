/**
 * Regenera el campo `resumen` de todas las reseñas de booking_reviews y google_reviews.
 * Usa el mismo modelo gpt-4o-mini en lotes de 10.
 * Reglas: máx. 25 palabras en español, mencionar empleado si se nombra.
 *
 * Uso: node -r dotenv/config scripts/regenerar-resumenes.js
 */

const { createClient } = require("@libsql/client");
const OpenAI = require("openai");
const fs = require("fs");
const path = require("path");

const PROGRESS_FILE = path.join(__dirname, ".regenerar-progress.json");

function loadProgress() {
  try { return JSON.parse(fs.readFileSync(PROGRESS_FILE, "utf8")); } catch { return {}; }
}
function saveProgress(data) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify(data), "utf8");
}

const BATCH_SIZE = 10;

function getDb() {
  return createClient({
    url: process.env.TURSO_DATABASE_URL,
    authToken: process.env.TURSO_AUTH_TOKEN,
  });
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function generateSummaries(items, attempt = 1) {
  const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
  const prompt = `Para cada elemento del siguiente array JSON, genera un resumen neutro de la opinión en un máximo de 25 palabras en español.
Reglas:
- Si se menciona un empleado por nombre o puesto (ej: "la recepcionista María", "el chico de limpieza Juan"), inclúyelo en el resumen.
- Si el texto está vacío devuelve "".
Devuelve SOLO {"items":[{"i":0,"resumen":"..."},...]} sin markdown.

Input:
${JSON.stringify(items)}`;

  try {
    const resp = await openai.chat.completions.create({
      model: "gpt-4o-mini",
      messages: [{ role: "user", content: prompt }],
      response_format: { type: "json_object" },
      temperature: 0.1,
    });
    const parsed = JSON.parse(resp.choices[0].message.content);
    return Array.isArray(parsed.items) ? parsed.items : [];
  } catch (err) {
    if (attempt < 4) {
      const wait = attempt * 3000;
      process.stdout.write(` [reintento ${attempt} en ${wait / 1000}s]`);
      await sleep(wait);
      return generateSummaries(items, attempt + 1);
    }
    throw err;
  }
}

async function processBatch(db, rows, textFn, tableName, idField = "id") {
  const items = rows.map((r, idx) => ({ i: idx, text: textFn(r) }));

  let enriched = [];
  try {
    enriched = await generateSummaries(items);
  } catch (err) {
    console.error(`  Error IA:`, err.message);
    return 0;
  }

  let updated = 0;
  for (let j = 0; j < rows.length; j++) {
    const e = enriched[j] || {};
    const resumen = (e.resumen || "").trim();
    try {
      await db.execute({
        sql: `UPDATE ${tableName} SET resumen = ? WHERE ${idField} = ?`,
        args: [resumen || null, rows[j].id],
      });
      updated++;
    } catch (err) {
      console.error(`  Error actualizando id ${rows[j].id}:`, err.message);
    }
  }
  return updated;
}

async function run() {
  const db = getDb();

  // Carga progreso guardado (o usa args de línea de comandos como override)
  const progress = loadProgress();
  const args = process.argv.slice(2);
  const resetFlag = args.includes("--reset");
  if (resetFlag) { saveProgress({}); Object.assign(progress, {}); }

  const lastBookingId = progress.lastBookingId || 0;
  const bookingDone   = progress.bookingDone   || false;
  const lastGoogleId  = progress.lastGoogleId  || 0;

  // ── Booking ────────────────────────────────────────────────────────────────
  if (!bookingDone) {
  const { rows: bookingRows } = await db.execute(
    lastBookingId > 0
      ? { sql: "SELECT id, titulo, comentario_positivo, comentario_negativo FROM booking_reviews WHERE id > ? ORDER BY id", args: [lastBookingId] }
      : "SELECT id, titulo, comentario_positivo, comentario_negativo FROM booking_reviews ORDER BY id"
  );
  console.log(`\nBooking: ${bookingRows.length} reseñas pendientes${lastBookingId > 0 ? ` (reanudando desde id ${lastBookingId})` : ""}`);

  let bookingUpdated = 0;
  for (let i = 0; i < bookingRows.length; i += BATCH_SIZE) {
    const batch = bookingRows.slice(i, i + BATCH_SIZE);
    process.stdout.write(`  Lote ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(bookingRows.length / BATCH_SIZE)} (id ${batch[0].id}–${batch[batch.length-1].id})…`);
    const n = await processBatch(
      db,
      batch,
      (r) => [r.titulo, r.comentario_positivo, r.comentario_negativo].filter(Boolean).join(" / "),
      "booking_reviews"
    );
    bookingUpdated += n;
    const lastId = batch[batch.length - 1].id;
    saveProgress({ lastBookingId: lastId, bookingDone: false, lastGoogleId });
    console.log(` ✓ ${n}`);
    await sleep(200);
  }
  saveProgress({ lastBookingId: 0, bookingDone: true, lastGoogleId });
  console.log(`Booking: ${bookingUpdated} actualizadas`);
  } else {
    console.log(`\nBooking: ya completado, saltando.`);
  }

  // ── Google ─────────────────────────────────────────────────────────────────
  const { rows: googleRows } = await db.execute(
    lastGoogleId > 0
      ? { sql: "SELECT id, message_original, review FROM google_reviews WHERE id > ? ORDER BY id", args: [lastGoogleId] }
      : "SELECT id, message_original, review FROM google_reviews ORDER BY id"
  );
  console.log(`\nGoogle: ${googleRows.length} reseñas pendientes${lastGoogleId > 0 ? ` (reanudando desde id ${lastGoogleId})` : ""}`);

  let googleUpdated = 0;
  for (let i = 0; i < googleRows.length; i += BATCH_SIZE) {
    const batch = googleRows.slice(i, i + BATCH_SIZE);
    process.stdout.write(`  Lote ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(googleRows.length / BATCH_SIZE)} (id ${batch[0].id}–${batch[batch.length-1].id})…`);
    const n = await processBatch(
      db,
      batch,
      (r) => r.message_original || r.review || "",
      "google_reviews"
    );
    googleUpdated += n;
    saveProgress({ lastBookingId: 0, bookingDone: true, lastGoogleId: batch[batch.length - 1].id });
    console.log(` ✓ ${n}`);
    await sleep(200);
  }
  console.log(`Google: ${googleUpdated} actualizadas`);

  saveProgress({ done: true });
  console.log("\nListo. Puedes borrar scripts/.regenerar-progress.json");
}

run().catch((err) => {
  console.error("Error:", err.message);
  process.exit(1);
});
