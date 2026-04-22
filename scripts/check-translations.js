require('dotenv').config({ override: true, path: require('path').resolve(__dirname, '..', '.env') });
const { createClient } = require('@libsql/client');

const client = createClient({
  url: process.env.TURSO_DATABASE_URL,
  authToken: process.env.TURSO_AUTH_TOKEN,
});

async function main() {
  // Booking: sin resumen pero con texto
  const r1 = await client.execute(
    `SELECT numero_reserva, comentario_positivo, comentario_negativo, resumen
     FROM booking_reviews
     WHERE resumen IS NULL
       AND (comentario_positivo IS NOT NULL AND comentario_positivo != '')
     ORDER BY scraped_at DESC LIMIT 10`
  );
  console.log(`\n=== Booking sin resumen (${r1.rows.length}) ===`);
  r1.rows.forEach(r => console.log(
    r.numero_reserva, '|', (r.comentario_positivo||'').slice(0,70)
  ));

  // Booking: todos con texto, muestra los 5 últimos para ver si están en español
  const r2 = await client.execute(
    `SELECT numero_reserva, comentario_positivo, scraped_at
     FROM booking_reviews
     WHERE comentario_positivo IS NOT NULL AND comentario_positivo != ''
     ORDER BY scraped_at DESC LIMIT 5`
  );
  console.log(`\n=== Últimas 5 Booking con texto ===`);
  r2.rows.forEach(r => console.log(r.numero_reserva, '|', (r.comentario_positivo||'').slice(0,100)));

  // Google: sin resumen pero con review
  const r3 = await client.execute(
    `SELECT review_url, review, message_original, resumen
     FROM google_reviews
     WHERE resumen IS NULL
       AND (review IS NOT NULL AND review != '')
     ORDER BY scraped_at DESC LIMIT 10`
  );
  console.log(`\n=== Google sin resumen (${r3.rows.length}) ===`);
  r3.rows.forEach(r => console.log(
    (r.review_url||'').slice(-15), '|', (r.review||'').slice(0,70)
  ));
}

main().catch(console.error);
