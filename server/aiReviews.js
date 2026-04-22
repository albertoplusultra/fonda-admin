/**
 * aiReviews.js
 * Enriquece filas de opiniones antes de insertarlas:
 *  - Traduce el texto al español (si ya está en español lo deja igual)
 *  - Genera un resumen breve de la opinión (máx. 20 palabras)
 *
 * Callbacks disponibles:
 *   onProgress(processed, total) – se llama tras cada lote completado
 *   onBatch(enrichedBatch)       – se llama (await) tras cada lote, para
 *                                  insertar en BD de forma incremental
 */

const OpenAI = require("openai");

const BATCH_SIZE = 10;

function getOpenAI() {
  return new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
}

// ─── Booking ────────────────────────────────────────────────────────────────

async function enrichBookingRows(rows, { onProgress, onBatch } = {}) {
  const result = rows.map((r) => ({
    ...r,
    _titulo_es: r["Título del comentario"] || "",
    _pos_es: r["Comentario positivo"] || "",
    _neg_es: r["Comentario negativo"] || "",
    _resumen: "",
  }));

  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);

    const items = batch.map((r, idx) => ({
      i: idx,
      titulo: r["Título del comentario"] || "",
      pos: r["Comentario positivo"] || "",
      neg: r["Comentario negativo"] || "",
    }));

    const prompt = `Eres un asistente que procesa opiniones de clientes de un hotel.
Para cada elemento del array JSON que recibes, devuelve un objeto con los mismos índices.
Cada objeto de respuesta tiene:
- "titulo": el título de la opinión TRADUCIDO al español (si ya está en español déjalo igual; si está vacío devuelve "").
- "pos": el comentario positivo TRADUCIDO al español (si ya está en español déjalo igual; si está vacío devuelve "").
- "neg": el comentario negativo TRADUCIDO al español (si ya está en español déjalo igual; si está vacío devuelve "").
- "resumen": resumen neutro de la opinión en un máximo de 20 palabras. Si no hay texto devuelve "".

Devuelve SOLO {"items":[...]} sin ningún markdown.

Input:
${JSON.stringify(items)}`;

    console.log(`[aiReviews] Booking lote ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(rows.length / BATCH_SIZE)} (filas ${i + 1}-${Math.min(i + batch.length, rows.length)})`);
    try {
      const openai = getOpenAI();
      const resp = await openai.chat.completions.create({
        model: "gpt-4o-mini",
        messages: [{ role: "user", content: prompt }],
        response_format: { type: "json_object" },
        temperature: 0.1,
      });

      const parsed = JSON.parse(resp.choices[0].message.content);
      const enriched = Array.isArray(parsed.items) ? parsed.items : [];

      for (let j = 0; j < batch.length; j++) {
        const e = enriched[j] || {};
        result[i + j]._titulo_es = e.titulo || batch[j]["Título del comentario"] || "";
        result[i + j]._pos_es = e.pos || batch[j]["Comentario positivo"] || "";
        result[i + j]._neg_es = e.neg || batch[j]["Comentario negativo"] || "";
        result[i + j]._resumen = e.resumen || "";
      }
    } catch (err) {
      console.error(`[aiReviews] Error en lote Booking ${i}-${i + batch.length - 1}:`, err.message);
    }

    const enrichedBatch = result.slice(i, i + batch.length);
    if (onBatch) await onBatch(enrichedBatch);
    if (onProgress) onProgress(Math.min(i + batch.length, rows.length), rows.length);
  }

  return result;
}

// ─── Google ─────────────────────────────────────────────────────────────────

async function enrichGoogleRows(rows, { onProgress, onBatch } = {}) {
  const result = rows.map((r) => {
    const original = r["Message Original"] || r["Review"] || "";
    return { ...r, _text_es: original, _resumen: "" };
  });

  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);

    const items = batch.map((r, idx) => ({
      i: idx,
      text: r["Message Original"] || r["Review"] || "",
    }));

    const prompt = `Eres un asistente que procesa opiniones de clientes de Google.
Para cada elemento del array JSON que recibes, devuelve un objeto con los mismos índices.
Cada objeto de respuesta tiene:
- "text_es": el texto de la opinión TRADUCIDO al español (si ya está en español déjalo igual; si está vacío devuelve "").
- "resumen": resumen neutro de la opinión en un máximo de 20 palabras. Si no hay texto devuelve "".

Devuelve SOLO {"items":[...]} sin ningún markdown.

Input:
${JSON.stringify(items)}`;

    console.log(`[aiReviews] Google lote ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(rows.length / BATCH_SIZE)} (filas ${i + 1}-${Math.min(i + batch.length, rows.length)})`);
    try {
      const openai = getOpenAI();
      const resp = await openai.chat.completions.create({
        model: "gpt-4o-mini",
        messages: [{ role: "user", content: prompt }],
        response_format: { type: "json_object" },
        temperature: 0.1,
      });

      const parsed = JSON.parse(resp.choices[0].message.content);
      const enriched = Array.isArray(parsed.items) ? parsed.items : [];

      for (let j = 0; j < batch.length; j++) {
        const e = enriched[j] || {};
        const fallback = batch[j]["Message Original"] || batch[j]["Review"] || "";
        result[i + j]._text_es = e.text_es || fallback;
        result[i + j]._resumen = e.resumen || "";
      }
    } catch (err) {
      console.error(`[aiReviews] Error en lote Google ${i}-${i + batch.length - 1}:`, err.message);
    }

    const enrichedBatch = result.slice(i, i + batch.length);
    if (onBatch) await onBatch(enrichedBatch);
    if (onProgress) onProgress(Math.min(i + batch.length, rows.length), rows.length);
  }

  return result;
}

module.exports = { enrichBookingRows, enrichGoogleRows };
