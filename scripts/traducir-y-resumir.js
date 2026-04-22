/**
 * traducir-y-resumir.js
 * Para las reseñas que aún no tienen `resumen`:
 *   - Booking: traduce comentario_positivo y comentario_negativo al español
 *             y genera el resumen.
 *   - Google:  traduce review → message_original al español (si aún no está)
 *             y genera el resumen.
 *
 * Uso: node scripts/traducir-y-resumir.js
 */

require('dotenv').config({ override: true, path: require('path').resolve(__dirname, '..', '.env') });

const { createClient } = require('@libsql/client');
const OpenAI = require('openai');

const BATCH_SIZE = 10;

function getDb() {
  return createClient({
    url: process.env.TURSO_DATABASE_URL,
    authToken: process.env.TURSO_AUTH_TOKEN,
  });
}

function getOpenAI() {
  return new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
}

// ── Booking ──────────────────────────────────────────────────────────────────

async function processBookingBatch(db, rows) {
  const openai = getOpenAI();

  const items = rows.map((r, idx) => ({
    i: idx,
    pos: r.comentario_positivo || '',
    neg: r.comentario_negativo || '',
  }));

  const prompt = `Eres un asistente que procesa opiniones de clientes de un hotel.
Para cada elemento del array JSON:
- "pos": comentario positivo TRADUCIDO al español (si ya está en español déjalo; si está vacío devuelve "").
- "neg": comentario negativo TRADUCIDO al español (si ya está en español déjalo; si está vacío devuelve "").
- "resumen": resumen neutro de la opinión en un máximo de 20 palabras en español. Si no hay texto devuelve "".

Devuelve SOLO {"items":[...]} sin markdown.

Input:
${JSON.stringify(items)}`;

  let enriched = [];
  try {
    const resp = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [{ role: 'user', content: prompt }],
      response_format: { type: 'json_object' },
      temperature: 0.1,
    });
    const parsed = JSON.parse(resp.choices[0].message.content);
    enriched = Array.isArray(parsed.items) ? parsed.items : [];
  } catch (err) {
    console.error('  Error IA Booking:', err.message);
    return 0;
  }

  let updated = 0;
  for (let j = 0; j < rows.length; j++) {
    const e = enriched[j] || {};
    const pos = (e.pos || rows[j].comentario_positivo || '').trim() || null;
    const neg = (e.neg || rows[j].comentario_negativo || '').trim() || null;
    const resumen = (e.resumen || '').trim() || null;
    try {
      await db.execute({
        sql: `UPDATE booking_reviews SET comentario_positivo = ?, comentario_negativo = ?, resumen = ? WHERE id = ?`,
        args: [pos, neg, resumen, rows[j].id],
      });
      updated++;
    } catch (err) {
      console.error(`  Error actualizando Booking id ${rows[j].id}:`, err.message);
    }
  }
  return updated;
}

// ── Google ───────────────────────────────────────────────────────────────────

async function processGoogleBatch(db, rows) {
  const openai = getOpenAI();

  const items = rows.map((r, idx) => ({
    i: idx,
    text: r.review || '',
  }));

  const prompt = `Eres un asistente que procesa opiniones de clientes de Google.
Para cada elemento del array JSON:
- "text_es": el texto de la opinión TRADUCIDO al español (si ya está en español déjalo; si está vacío devuelve "").
- "resumen": resumen neutro en un máximo de 20 palabras en español. Si no hay texto devuelve "".

Devuelve SOLO {"items":[...]} sin markdown.

Input:
${JSON.stringify(items)}`;

  let enriched = [];
  try {
    const resp = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [{ role: 'user', content: prompt }],
      response_format: { type: 'json_object' },
      temperature: 0.1,
    });
    const parsed = JSON.parse(resp.choices[0].message.content);
    enriched = Array.isArray(parsed.items) ? parsed.items : [];
  } catch (err) {
    console.error('  Error IA Google:', err.message);
    return 0;
  }

  let updated = 0;
  for (let j = 0; j < rows.length; j++) {
    const e = enriched[j] || {};
    const textEs = (e.text_es || rows[j].review || '').trim() || null;
    const resumen = (e.resumen || '').trim() || null;
    try {
      await db.execute({
        sql: `UPDATE google_reviews SET message_original = ?, resumen = ? WHERE id = ?`,
        args: [textEs, resumen, rows[j].id],
      });
      updated++;
    } catch (err) {
      console.error(`  Error actualizando Google id ${rows[j].id}:`, err.message);
    }
  }
  return updated;
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function run() {
  const db = getDb();

  // ── Booking ────────────────────────────────────────────────────────────────
  const { rows: bookingRows } = await db.execute(
    `SELECT id, comentario_positivo, comentario_negativo
     FROM booking_reviews
     WHERE resumen IS NULL OR resumen = ''
     ORDER BY id`
  );
  console.log(`\nBooking pendientes: ${bookingRows.length}`);

  let bookingUpdated = 0;
  for (let i = 0; i < bookingRows.length; i += BATCH_SIZE) {
    const batch = bookingRows.slice(i, i + BATCH_SIZE);
    const total = bookingRows.length;
    process.stdout.write(
      `  Procesando ${i + 1}-${Math.min(i + batch.length, total)} de ${total}…`
    );
    const n = await processBookingBatch(db, batch);
    bookingUpdated += n;
    console.log(` ✓ ${n}`);
  }
  console.log(`Booking: ${bookingUpdated} actualizadas`);

  // ── Google ─────────────────────────────────────────────────────────────────
  const { rows: googleRows } = await db.execute(
    `SELECT id, review, message_original
     FROM google_reviews
     WHERE resumen IS NULL OR resumen = ''
     ORDER BY id`
  );
  console.log(`\nGoogle pendientes: ${googleRows.length}`);

  let googleUpdated = 0;
  for (let i = 0; i < googleRows.length; i += BATCH_SIZE) {
    const batch = googleRows.slice(i, i + BATCH_SIZE);
    const total = googleRows.length;
    process.stdout.write(
      `  Procesando ${i + 1}-${Math.min(i + batch.length, total)} de ${total}…`
    );
    const n = await processGoogleBatch(db, batch);
    googleUpdated += n;
    console.log(` ✓ ${n}`);
  }
  console.log(`Google: ${googleUpdated} actualizadas`);

  console.log('\n¡Listo!');
}

run().catch((err) => {
  console.error('Error:', err.message);
  process.exit(1);
});
