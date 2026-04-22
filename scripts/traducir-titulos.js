/**
 * traducir-titulos.js
 * Traduce al español el campo `titulo` de todas las booking_reviews
 * que tienen título en otro idioma (procesa todas para seguridad).
 *
 * Uso: node scripts/traducir-titulos.js
 */

require('dotenv').config({ override: true, path: require('path').resolve(__dirname, '..', '.env') });

const { createClient } = require('@libsql/client');
const OpenAI = require('openai');

const BATCH_SIZE = 15;

function getDb() {
  return createClient({ url: process.env.TURSO_DATABASE_URL, authToken: process.env.TURSO_AUTH_TOKEN });
}

async function translateBatch(rows) {
  const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

  const items = rows.map((r, idx) => ({ i: idx, titulo: r.titulo || '' }));

  const prompt = `Traduce al español el campo "titulo" de cada elemento del array JSON.
Si ya está en español, déjalo igual. Si está vacío devuelve "".
Devuelve SOLO {"items":[{"i":0,"titulo":"..."},...]} sin markdown.

Input:
${JSON.stringify(items)}`;

  const resp = await openai.chat.completions.create({
    model: 'gpt-4o-mini',
    messages: [{ role: 'user', content: prompt }],
    response_format: { type: 'json_object' },
    temperature: 0.1,
  });

  const parsed = JSON.parse(resp.choices[0].message.content);
  return Array.isArray(parsed.items) ? parsed.items : [];
}

async function run() {
  const db = getDb();

  const { rows } = await db.execute(
    `SELECT id, titulo FROM booking_reviews WHERE titulo IS NOT NULL AND titulo != '' ORDER BY id`
  );
  console.log(`\nBooking con título: ${rows.length}`);

  let updated = 0;
  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);
    process.stdout.write(`  Procesando ${i + 1}-${Math.min(i + batch.length, rows.length)} de ${rows.length}…`);

    let enriched = [];
    try {
      enriched = await translateBatch(batch);
    } catch (err) {
      console.error(' Error IA:', err.message);
      continue;
    }

    for (let j = 0; j < batch.length; j++) {
      const e = enriched[j] || {};
      const titulo = (e.titulo || batch[j].titulo || '').trim() || null;
      try {
        await db.execute({ sql: `UPDATE booking_reviews SET titulo = ? WHERE id = ?`, args: [titulo, batch[j].id] });
        updated++;
      } catch (err) {
        console.error(` Error id ${batch[j].id}:`, err.message);
      }
    }
    console.log(` ✓ ${enriched.length}`);
  }

  console.log(`\nActualizados: ${updated}\n¡Listo!`);
}

run().catch(err => { console.error('Error:', err.message); process.exit(1); });
