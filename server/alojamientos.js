'use strict';

/**
 * Fuente única de alojamientos que gestiona la aplicación.
 */

const ALOJAMIENTOS = [
  { name: "La Fonda de los Príncipes", slug: "fonda-principes" },
  { name: "Iconic Suites",             slug: "iconic-suites" },
  { name: "Miosotis Suites",           slug: "miosotis-suites" },
  { name: "The Garden Suites",         slug: "garden-suites" },
];

const DEFAULT_ALOJAMIENTO = ALOJAMIENTOS[0].name;

function listAlojamientos() {
  return ALOJAMIENTOS.map(({ name, slug }) => ({ name, slug }));
}

function getAlojamientoByName(name) {
  if (!name) return null;
  const needle = String(name).trim().toLowerCase();
  return ALOJAMIENTOS.find((a) => a.name.toLowerCase() === needle) || null;
}

function getAlojamientoBySlug(slug) {
  if (!slug) return null;
  return ALOJAMIENTOS.find((a) => a.slug === String(slug).trim().toLowerCase()) || null;
}

module.exports = {
  ALOJAMIENTOS,
  DEFAULT_ALOJAMIENTO,
  listAlojamientos,
  getAlojamientoByName,
  getAlojamientoBySlug,
};
