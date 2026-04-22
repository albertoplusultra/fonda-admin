'use strict';

/**
 * Parser compartido para los CSVs oficiales de Booking y Google.
 * Usado tanto por el script de importación como por el endpoint de subida.
 */

const { parse } = require('csv-parse/sync');

const GOOGLE_COLUMNS = [
  'index',
  'blank',
  'Author',
  'Author Description',
  'Date',
  'Rating',
  'Helpful count',
  'Review',
  'Picture included',
  'Language',
  'Message Original',
  'Language code',
  'Reviewer Data',
  'Profile Url',
  'Review Url',
  'Photos',
];

function parseBookingCsv(input) {
  return parse(input, {
    columns: true,
    skip_empty_lines: true,
    bom: true,
    relax_quotes: true,
    relax_column_count: true,
    trim: false,
  });
}

function parseGoogleCsv(input) {
  return parse(input, {
    columns: GOOGLE_COLUMNS,
    from_line: 2,
    skip_empty_lines: true,
    bom: true,
    relax_quotes: true,
    relax_column_count: true,
    trim: false,
  });
}

/**
 * Detecta si el CSV es de Booking o Google a partir de sus cabeceras.
 * Devuelve 'booking', 'google' o null.
 */
function detectCsvSource(input) {
  try {
    const firstRow = parse(input, {
      to_line: 1,
      bom: true,
      relax_quotes: true,
      relax_column_count: true,
    });
    const headers = ((firstRow && firstRow[0]) || [])
      .map((h) => String(h || '').trim().toLowerCase());
    const joined = headers.join('|');
    if (joined.includes('fecha del comentario')) return 'booking';
    if (joined.includes('review url') || joined.includes('author description')) return 'google';
  } catch {
    // ignore
  }
  return null;
}

module.exports = {
  GOOGLE_COLUMNS,
  parseBookingCsv,
  parseGoogleCsv,
  detectCsvSource,
};
