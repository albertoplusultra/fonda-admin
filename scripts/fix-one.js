require('dotenv').config({ override: true, path: require('path').resolve(__dirname, '..', '.env') });
const { createClient } = require('@libsql/client');
const OpenAI = require('openai');

const db = createClient({ url: process.env.TURSO_DATABASE_URL, authToken: process.env.TURSO_AUTH_TOKEN });
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

async function main() {
  // Ver el estado actual de la opinión problemática
  const r = await db.execute(
    `SELECT id, numero_reserva, titulo, comentario_positivo, comentario_negativo, resumen
     FROM booking_reviews
     WHERE titulo LIKE '%Pobyt%' OR titulo LIKE '%oceniam%'`
  );
  console.log('Encontradas:', r.rows.length);
  r.rows.forEach(row => console.log(JSON.stringify({
    id: row.id, num: row.numero_reserva,
    titulo: row.titulo,
    pos: row.comentario_positivo,
    resumen: row.resumen,
  })));

  if (!r.rows.length) {
    console.log('No se encontró la opinión.');
    return;
  }

  // También buscar cualquier opinión con título en idioma extranjero (no empieza con char español/común)
  const pendientes = await db.execute(
    `SELECT id, titulo FROM booking_reviews
     WHERE titulo IS NOT NULL AND titulo != ''
     AND (
       titulo GLOB '*[ąćęłńóśźżĄĆĘŁŃÓŚŹŻ]*'  -- polaco
       OR titulo GLOB '*[àâäéèêëîïôùûüÿœæÀÂÄÉÈÊËÎÏÔÙÛÜŸŒÆ]*'  -- francés
       OR titulo GLOB '*[ΑΒΓΔΕΖΗΘΙΚΛΜΝΞΟΠΡΣΤΥΦΧΨΩαβγδεζηθικλμνξοπρστυφχψω]*'  -- griego
     )
     LIMIT 20`
  );
  console.log('\nOpiniones con posibles títulos no traducidos:', pendientes.rows.length);
  pendientes.rows.forEach(row => console.log(row.id, '|', row.titulo));
}
main().catch(console.error);
