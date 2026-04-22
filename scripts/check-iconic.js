require('dotenv').config({ override: true, path: require('path').resolve(__dirname, '..', '.env') });
const { createClient } = require('@libsql/client');
const client = createClient({ url: process.env.TURSO_DATABASE_URL, authToken: process.env.TURSO_AUTH_TOKEN });

async function main() {
  const r = await client.execute(
    `SELECT numero_reserva, alojamiento, comentario_positivo, comentario_negativo, resumen, scraped_at
     FROM booking_reviews
     WHERE alojamiento LIKE '%Iconic%'
     ORDER BY scraped_at DESC LIMIT 12`
  );
  r.rows.forEach(row => console.log(JSON.stringify({
    num: row.numero_reserva,
    pos: (row.comentario_positivo || '').slice(0, 70),
    neg: (row.comentario_negativo || '').slice(0, 50),
    resumen: row.resumen ? row.resumen.slice(0, 40) : null,
    scraped: row.scraped_at,
  })));
}
main().catch(console.error);
