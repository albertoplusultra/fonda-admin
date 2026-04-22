/**
 * Regenera el campo `resumen` de todas las reseñas de booking_reviews y google_reviews.
 * Usa el mismo modelo gpt-4o-mini en lotes de 5.
 *
 * Uso: node -r dotenv/config scripts/regenerar-resumenes.js
 */

const { createClient } = require("@libsql/client");
const OpenAI = require("openai");

const BATCH_SIZE = 5;

function getDb() {
  return createClient({
    url: process.env.TURSO_DATABASE_URL,
    authToken: process.env.TURSO_AUTH_TOKEN,
  });
}

async function generateSummaries(items) {
  const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
  const prompt = `Para cada elemento del siguiente array JSON, genera un resumen neutro de la opinión en un máximo de 20 palabras en español. Si el texto está vacío devuelve "".
Devuelve SOLO {"items":[{"i":0,"resumen":"..."},...]} sin markdown.

Input:
${JSON.stringify(items)}`;

  const resp = await openai.chat.completions.create({
    model: "gpt-4o-mini",
    messages: [{ role: "user", content: prompt }],
    response_format: { type: "json_object" },
    temperature: 0.1,
  });

  const parsed = JSON.parse(resp.choices[0].message.content);
  return Array.isArray(parsed.items) ? parsed.items : [];
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

  // ── Booking ────────────────────────────────────────────────────────────────
  const { rows: bookingRows } = await db.execute(
    "SELECT id, comentario_positivo, comentario_negativo FROM booking_reviews ORDER BY id"
  );
  console.log(`\nBooking: ${bookingRows.length} reseñas`);

  let bookingUpdated = 0;
  for (let i = 0; i < bookingRows.length; i += BATCH_SIZE) {
    const batch = bookingRows.slice(i, i + BATCH_SIZE);
    process.stdout.write(`  Lote ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(bookingRows.length / BATCH_SIZE)} (${i + 1}-${Math.min(i + batch.length, bookingRows.length)})…`);
    const n = await processBatch(
      db,
      batch,
      (r) => [r.comentario_positivo, r.comentario_negativo].filter(Boolean).join(" / "),
      "booking_reviews"
    );
    bookingUpdated += n;
    console.log(` ✓ ${n}`);
  }
  console.log(`Booking: ${bookingUpdated} actualizadas`);

  // ── Google ─────────────────────────────────────────────────────────────────
  const { rows: googleRows } = await db.execute(
    "SELECT id, message_original, review FROM google_reviews ORDER BY id"
  );
  console.log(`\nGoogle: ${googleRows.length} reseñas`);

  let googleUpdated = 0;
  for (let i = 0; i < googleRows.length; i += BATCH_SIZE) {
    const batch = googleRows.slice(i, i + BATCH_SIZE);
    process.stdout.write(`  Lote ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(googleRows.length / BATCH_SIZE)} (${i + 1}-${Math.min(i + batch.length, googleRows.length)})…`);
    const n = await processBatch(
      db,
      batch,
      (r) => r.message_original || r.review || "",
      "google_reviews"
    );
    googleUpdated += n;
    console.log(` ✓ ${n}`);
  }
  console.log(`Google: ${googleUpdated} actualizadas`);

  console.log("\nListo.");
}

run().catch((err) => {
  console.error("Error:", err.message);
  process.exit(1);
});
