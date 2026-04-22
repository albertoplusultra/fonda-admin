'use strict';

/**
 * Importa los CSVs oficiales de Booking y Google en Turso,
 * asignando `alojamiento = "La Fonda de los Príncipes"` por defecto.
 *
 * Uso:
 *   node scripts/import-reviews-csv.js [rutaBooking.csv] [rutaGoogle.csv] [alojamiento]
 *
 * Sin argumentos, busca los CSVs en la carpeta Downloads del usuario.
 */

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const os = require('os');

const { initDb, importBookingCsv, importGoogleCsv } = require('../server/db');
const { DEFAULT_ALOJAMIENTO } = require('../server/alojamientos');
const { parseBookingCsv, parseGoogleCsv } = require('../server/reviewsCsv');

const DOWNLOADS = path.join(os.homedir(), 'Downloads');

const DEFAULTS = {
  booking: path.join(DOWNLOADS, 'reviews.csv'),
  google: path.join(DOWNLOADS, 'export_20260421-220711.csv'),
};

async function main() {
  const [bookingArg, googleArg, alojamientoArg] = process.argv.slice(2);
  const bookingPath = bookingArg || DEFAULTS.booking;
  const googlePath = googleArg || DEFAULTS.google;
  const alojamiento = alojamientoArg || DEFAULT_ALOJAMIENTO;

  console.log('→ Alojamiento:', alojamiento);
  console.log('→ Booking CSV:', bookingPath);
  console.log('→ Google CSV:', googlePath);

  if (!process.env.TURSO_DATABASE_URL) {
    console.error('Falta TURSO_DATABASE_URL en .env');
    process.exit(1);
  }

  console.log('\nCreando/verificando esquema…');
  await initDb();

  if (fs.existsSync(bookingPath)) {
    console.log('\nImportando Booking…');
    const rows = parseBookingCsv(fs.readFileSync(bookingPath));
    console.log(`  · ${rows.length} filas leídas del CSV`);
    const res = await importBookingCsv(alojamiento, rows);
    console.log(`  · Insertadas: ${res.inserted}  · Duplicadas (saltadas): ${res.skipped}  · Errores: ${res.errors}`);
  } else {
    console.warn(`  (no existe ${bookingPath}, saltando Booking)`);
  }

  if (fs.existsSync(googlePath)) {
    console.log('\nImportando Google…');
    const rows = parseGoogleCsv(fs.readFileSync(googlePath));
    console.log(`  · ${rows.length} filas leídas del CSV`);
    const res = await importGoogleCsv(alojamiento, rows);
    console.log(`  · Insertadas: ${res.inserted}  · Duplicadas (saltadas): ${res.skipped}  · Errores: ${res.errors}`);
  } else {
    console.warn(`  (no existe ${googlePath}, saltando Google)`);
  }

  console.log('\nFin.');
}

main().catch((err) => {
  console.error('\nError fatal:', err);
  process.exit(1);
});
