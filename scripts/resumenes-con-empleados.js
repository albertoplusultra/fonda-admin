/**
 * Script de uso único: detecta reseñas que mencionan empleados por nombre
 * y regenera solo su resumen incluyendo el nombre/puesto del empleado.
 *
 * Uso: node -r dotenv/config scripts/resumenes-con-empleados.js
 */

const { createClient } = require("@libsql/client");
const OpenAI = require("openai");
const fs = require("fs");
const path = require("path");

const BATCH_SIZE = 15;
const PROGRESS_FILE = path.join(__dirname, ".empleados-progress.json");

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function getDb() {
  return createClient({
    url: process.env.TURSO_DATABASE_URL,
    authToken: process.env.TURSO_AUTH_TOKEN,
  });
}

function loadProgress() {
  try { return JSON.parse(fs.readFileSync(PROGRESS_FILE, "utf8")); } catch { return {}; }
}
function saveProgress(data) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify(data), "utf8");
}

// Pre-filtro por texto: palabras con mayúscula en mitad de frase, o términos de personal
const KEYWORDS = [
  "recepcionista", "recepcion", "recepción", "conserje", "camarera", "camarero",
  "limpiadora", "limpiador", "cocinero", "cocinera", "gerente", "director",
  "personal", "empleada", "empleado", "trabajadora", "trabajador", "staff",
  "señorita", "señor ", "señora", "manager",
];

function textoPreFiltro(texto) {
  if (!texto) return false;
  const t = texto.toLowerCase();
  if (KEYWORDS.some((k) => t.includes(k))) return true;
  // Nombre propio: mayúscula precedida de espacio y seguida de letras (no inicio de frase)
  return /[a-záéíóúüñ]\s+[A-ZÁÉÍÓÚÜÑ][a-záéíóúüñ]{2,}/.test(texto);
}

async function detectarYResumir(items, attempt = 1) {
  const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

  const prompt = `Analiza cada opinión de hotel del siguiente array JSON. Para cada una:
1. Determina si se menciona un empleado por nombre o puesto específico (ej: "María en recepción", "el chico de limpieza Juan", "la recepcionista").
2. Si SÍ menciona empleado: genera un resumen neutro en español de máximo 25 palabras que incluya el nombre/puesto del empleado.
3. Si NO menciona empleado: devuelve "skip".

Responde con JSON: {"items":[{"i":0,"resumen":"...o skip"},...]} sin markdown.

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
      return detectarYResumir(items, attempt + 1);
    }
    throw err;
  }
}

async function run() {
  const db = getDb();
  const progress = loadProgress();

  // ── Booking ────────────────────────────────────────────────────────────────
  if (!progress.bookingDone) {
    const lastId = progress.lastBookingId || 0;
    const { rows } = await db.execute(
      lastId > 0
        ? { sql: "SELECT id, titulo, comentario_positivo, comentario_negativo FROM booking_reviews WHERE id > ? ORDER BY id", args: [lastId] }
        : "SELECT id, titulo, comentario_positivo, comentario_negativo FROM booking_reviews ORDER BY id"
    );

    // Pre-filtro local
    const candidatas = rows.filter((r) => {
      const t = [r.titulo, r.comentario_positivo, r.comentario_negativo].filter(Boolean).join(" ");
      return textoPreFiltro(t);
    });

    console.log(`\nBooking: ${rows.length} reseñas → ${candidatas.length} candidatas tras pre-filtro`);

    let updated = 0, skipped = 0;
    for (let i = 0; i < candidatas.length; i += BATCH_SIZE) {
      const batch = candidatas.slice(i, i + BATCH_SIZE);
      const total = Math.ceil(candidatas.length / BATCH_SIZE);
      process.stdout.write(`  Lote ${Math.floor(i / BATCH_SIZE) + 1}/${total} (id ${batch[0].id}–${batch[batch.length-1].id})…`);

      const items = batch.map((r, idx) => ({
        i: idx,
        text: [r.titulo, r.comentario_positivo, r.comentario_negativo].filter(Boolean).join(" / "),
      }));

      let enriched = [];
      try {
        enriched = await detectarYResumir(items);
      } catch (err) {
        console.error(`  Error IA: ${err.message}`);
        saveProgress({ ...progress, lastBookingId: batch[0].id });
        continue;
      }

      let batchUpdated = 0;
      for (let j = 0; j < batch.length; j++) {
        const e = enriched[j] || {};
        const resumen = (e.resumen || "").trim();
        if (!resumen || resumen === "skip") { skipped++; continue; }
        try {
          await db.execute({ sql: "UPDATE booking_reviews SET resumen = ? WHERE id = ?", args: [resumen, batch[j].id] });
          batchUpdated++;
        } catch (err) {
          console.error(`  Error DB id ${batch[j].id}: ${err.message}`);
        }
      }

      updated += batchUpdated;
      saveProgress({ ...progress, lastBookingId: batch[batch.length - 1].id });
      console.log(` ✓ ${batchUpdated} actualizadas, ${batch.length - batchUpdated} sin empleado`);
      await sleep(200);
    }

    console.log(`Booking: ${updated} resúmenes actualizados, ${skipped} sin empleado`);
    saveProgress({ ...progress, lastBookingId: 0, bookingDone: true });
  } else {
    console.log(`\nBooking: ya completado.`);
  }

  // ── Google ─────────────────────────────────────────────────────────────────
  if (!progress.googleDone) {
    const lastId = progress.lastGoogleId || 0;
    const { rows } = await db.execute(
      lastId > 0
        ? { sql: "SELECT id, message_original, review FROM google_reviews WHERE id > ? ORDER BY id", args: [lastId] }
        : "SELECT id, message_original, review FROM google_reviews ORDER BY id"
    );

    const candidatas = rows.filter((r) => textoPreFiltro(r.message_original || r.review || ""));

    console.log(`\nGoogle: ${rows.length} reseñas → ${candidatas.length} candidatas tras pre-filtro`);

    let updated = 0, skipped = 0;
    for (let i = 0; i < candidatas.length; i += BATCH_SIZE) {
      const batch = candidatas.slice(i, i + BATCH_SIZE);
      const total = Math.ceil(candidatas.length / BATCH_SIZE);
      process.stdout.write(`  Lote ${Math.floor(i / BATCH_SIZE) + 1}/${total} (id ${batch[0].id}–${batch[batch.length-1].id})…`);

      const items = batch.map((r, idx) => ({
        i: idx,
        text: r.message_original || r.review || "",
      }));

      let enriched = [];
      try {
        enriched = await detectarYResumir(items);
      } catch (err) {
        console.error(`  Error IA: ${err.message}`);
        saveProgress({ ...progress, lastGoogleId: batch[0].id });
        continue;
      }

      let batchUpdated = 0;
      for (let j = 0; j < batch.length; j++) {
        const e = enriched[j] || {};
        const resumen = (e.resumen || "").trim();
        if (!resumen || resumen === "skip") { skipped++; continue; }
        try {
          await db.execute({ sql: "UPDATE google_reviews SET resumen = ? WHERE id = ?", args: [resumen, batch[j].id] });
          batchUpdated++;
        } catch (err) {
          console.error(`  Error DB id ${batch[j].id}: ${err.message}`);
        }
      }

      updated += batchUpdated;
      saveProgress({ ...progress, lastGoogleId: batch[batch.length - 1].id });
      console.log(` ✓ ${batchUpdated} actualizadas, ${batch.length - batchUpdated} sin empleado`);
      await sleep(200);
    }

    console.log(`Google: ${updated} resúmenes actualizados, ${skipped} sin empleado`);
    saveProgress({ ...progress, lastGoogleId: 0, googleDone: true });
  } else {
    console.log(`\nGoogle: ya completado.`);
  }

  console.log("\nListo. Puedes borrar scripts/.empleados-progress.json");
}

run().catch((err) => {
  console.error("Error:", err.message);
  process.exit(1);
});
