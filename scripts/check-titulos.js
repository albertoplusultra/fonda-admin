require('dotenv').config({ override: true, path: require('path').resolve(__dirname, '..', '.env') });
const { createClient } = require('@libsql/client');
const client = createClient({ url: process.env.TURSO_DATABASE_URL, authToken: process.env.TURSO_AUTH_TOKEN });

async function main() {
  const r = await client.execute(
    `SELECT numero_reserva, titulo, comentario_positivo, comentario_negativo
     FROM booking_reviews
     WHERE titulo IS NOT NULL AND titulo != ''
     ORDER BY scraped_at DESC LIMIT 15`
  );
  r.rows.forEach(row => console.log(JSON.stringify({
    num: row.numero_reserva,
    titulo: (row.titulo || ''),
    pos: (row.comentario_positivo || '').slice(0, 40),
  })));
}
main().catch(console.error);
