/**
 * Traduce y genera resumen para booking_reviews que tienen resumen NULL.
 */
require('dotenv').config({ override: true, path: require('path').resolve(__dirname, '..', '.env') });
const { createClient } = require('@libsql/client');
const OpenAI = require('openai');

const BATCH_SIZE = 10;
const db = createClient({ url: process.env.TURSO_DATABASE_URL, authToken: process.env.TURSO_AUTH_TOKEN });
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

async function main() {
  const { rows } = await db.execute(
    `SELECT id, titulo, comentario_positivo, comentario_negativo
     FROM booking_reviews WHERE resumen IS NULL ORDER BY id`
  );
  console.log(`Pendientes sin resumen: ${rows.length}`);
  if (!rows.length) { console.log('Nada que hacer.'); return; }

  let updated = 0;
  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);
    process.stdout.write(`  ${i + 1}-${Math.min(i + batch.length, rows.length)} de ${rows.length}…`);

    const items = batch.map((r, idx) => ({
      i: idx,
      titulo: r.titulo || '',
      pos: r.comentario_positivo || '',
      neg: r.comentario_negativo || '',
    }));

    const prompt = `Eres un asistente que procesa opiniones de clientes de un hotel.
Para cada elemento del array JSON:
- "titulo": título TRADUCIDO al español (si ya está déjalo; si vacío devuelve "").
- "pos": comentario positivo TRADUCIDO al español (idem).
- "neg": comentario negativo TRADUCIDO al español (idem).
- "resumen": resumen neutro en máximo 20 palabras en español. Si no hay texto devuelve "".
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
      console.error(' Error IA:', err.message);
      continue;
    }

    for (let j = 0; j < batch.length; j++) {
      const e = enriched[j] || {};
      const titulo  = (e.titulo || batch[j].titulo || '').trim() || null;
      const pos     = (e.pos    || batch[j].comentario_positivo || '').trim() || null;
      const neg     = (e.neg    || batch[j].comentario_negativo || '').trim() || null;
      const resumen = (e.resumen || '').trim() || null;
      try {
        await db.execute({
          sql: `UPDATE booking_reviews SET titulo=?, comentario_positivo=?, comentario_negativo=?, resumen=? WHERE id=?`,
          args: [titulo, pos, neg, resumen, batch[j].id],
        });
        updated++;
      } catch (err) {
        console.error(` Error id ${batch[j].id}:`, err.message);
      }
    }
    console.log(` ✓ ${enriched.length}`);
  }
  console.log(`\nActualizados: ${updated} — ¡Listo!`);
}
main().catch(console.error);
