'use strict';

const { getBrowser } = require('./booking');

const SOURCE_BOOKING = 'booking';
const SOURCE_GOOGLE = 'google';
const SOURCE_TRIPADVISOR = 'tripadvisor';

// ─── Utilidades de fecha ────────────────────────────────────────────────────

const MESES_ES = {
  enero: '01', febrero: '02', marzo: '03', abril: '04',
  mayo: '05', junio: '06', julio: '07', agosto: '08',
  septiembre: '09', octubre: '10', noviembre: '11', diciembre: '12',
};

/**
 * Convierte textos de fecha en español a "YYYY-MM-DD".
 * Formatos soportados:
 *   "14 de enero de 2024" / "enero de 2024" / "2024-01-14"
 */
function parseSpanishDate(text) {
  if (!text) return null;
  const s = text.trim().toLowerCase().replace(/\s+/g, ' ');

  // YYYY-MM-DD
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.slice(0, 10);

  // DD de MES de YYYY
  const full = s.match(/(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})/);
  if (full) {
    const m = MESES_ES[full[2]];
    if (m) return `${full[3]}-${m}-${full[1].padStart(2, '0')}`;
  }

  // MES de YYYY
  const partial = s.match(/(\w+)\s+de\s+(\d{4})/);
  if (partial) {
    const m = MESES_ES[partial[1]];
    if (m) return `${partial[2]}-${m}-01`;
  }

  return null;
}

/**
 * Convierte textos relativos de Google ("hace 2 semanas", "hace 1 mes"…) a "YYYY-MM-DD".
 */
function parseRelativeDate(text) {
  if (!text) return null;
  const s = text.trim().toLowerCase();

  const now = new Date();
  const match = (pattern) => s.match(pattern);

  let m;
  if ((m = match(/hace\s+(\d+)\s+semana/))) {
    const d = new Date(now); d.setDate(d.getDate() - Number(m[1]) * 7);
    return d.toISOString().slice(0, 10);
  }
  if ((m = match(/hace\s+(\d+)\s+mes/))) {
    const d = new Date(now); d.setMonth(d.getMonth() - Number(m[1]));
    return d.toISOString().slice(0, 10);
  }
  if ((m = match(/hace\s+(\d+)\s+año/))) {
    const d = new Date(now); d.setFullYear(d.getFullYear() - Number(m[1]));
    return d.toISOString().slice(0, 10);
  }
  if ((m = match(/hace\s+(\d+)\s+día/))) {
    const d = new Date(now); d.setDate(d.getDate() - Number(m[1]));
    return d.toISOString().slice(0, 10);
  }
  if (/hace\s+una?\s+semana/.test(s)) {
    const d = new Date(now); d.setDate(d.getDate() - 7);
    return d.toISOString().slice(0, 10);
  }
  if (/hace\s+un\s+mes/.test(s)) {
    const d = new Date(now); d.setMonth(d.getMonth() - 1);
    return d.toISOString().slice(0, 10);
  }
  if (/hace\s+un\s+año/.test(s)) {
    const d = new Date(now); d.setFullYear(d.getFullYear() - 1);
    return d.toISOString().slice(0, 10);
  }

  // Intentar parsear como español también
  return parseSpanishDate(text);
}

// ─── Scrapers ───────────────────────────────────────────────────────────────

/**
 * Scraper de Booking.com.
 * URL esperada: página de reseñas del hotel, ej.
 *   https://www.booking.com/hotel/es/fonda-principes.es.html
 *   o directamente la reviewlist:
 *   https://www.booking.com/reviewlist.es.html?cc1=es&pagename=fonda-principes
 */
async function scrapeBookingReviews(hotelUrl, { fromDate = null } = {}) {
  const browser = await getBrowser();
  const page = await browser.newPage();
  const reviews = [];

  try {
    await page.setUserAgent(
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    );
    await page.setExtraHTTPHeaders({ 'accept-language': 'es-ES,es;q=0.9' });

    await page.goto(hotelUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });

    // Aceptar cookies si aparece el banner
    try {
      const cookieBtn = await page.$(
        '#onetrust-accept-btn-handler, [data-gdpr-consent-accept], button[id*="accept"]',
      );
      if (cookieBtn) { await cookieBtn.click(); await new Promise(r => setTimeout(r, 1000)); }
    } catch {}

    // Esperar contenedor de reseñas
    await page.waitForSelector(
      '[data-testid="review-card"], .c-review-block, .review_item, #reviews_no_target',
      { timeout: 12000 },
    ).catch(() => {});

    const raw = await page.evaluate(() => {
      const items = [];

      // Booking usa distintos layouts según si es la página de reseñas o el listado
      const selectors = [
        '[data-testid="review-card"]',
        '.c-review-block',
        '.review_item',
      ];

      let cards = [];
      for (const sel of selectors) {
        const found = [...document.querySelectorAll(sel)];
        if (found.length) { cards = found; break; }
      }

      for (const card of cards.slice(0, 100)) {
        // Puntuación
        const scoreEl = card.querySelector(
          '[data-testid="review-score"] .ac4a7896c7, .c-score, .bui-review-score__badge, .review-score-badge',
        ) || card.querySelector('[class*="score"]');
        const scoreText = scoreEl ? scoreEl.textContent.trim().replace(',', '.') : null;
        const score = scoreText ? parseFloat(scoreText) : null;

        // Fecha
        const dateEl = card.querySelector(
          '[data-testid="review-date"], .c-review-block__date, .review_item_date, [class*="date"]',
        );
        const dateText = dateEl ? dateEl.textContent.trim() : null;

        // Texto positivo
        const posEl = card.querySelector(
          '[data-testid="review-positive-text"], .c-review__body--positive, .review_pos',
        );
        const posText = posEl ? posEl.textContent.replace(/^[👍✓+]/, '').trim() : null;

        // Texto negativo
        const negEl = card.querySelector(
          '[data-testid="review-negative-text"], .c-review__body--negative, .review_neg',
        );
        const negText = negEl ? negEl.textContent.replace(/^[👎✗-]/, '').trim() : null;

        // Autor
        const authorEl = card.querySelector(
          '[data-testid="review-author"], .bui-avatar-block__title, .reviewer_name, [class*="author"]',
        );
        const author = authorEl ? authorEl.textContent.trim() : null;

        // ID externo
        const extId = card.dataset.reviewId || card.getAttribute('data-id') || null;

        const text = [posText, negText].filter(Boolean).join('\n') || null;
        items.push({ scoreText, dateText, text, author, extId });
      }

      return items;
    });

    for (const r of raw) {
      const reviewDate = parseSpanishDate(r.dateText);
      if (fromDate && reviewDate && reviewDate <= fromDate) continue;

      reviews.push({
        source: SOURCE_BOOKING,
        external_id: r.extId ? `booking:${r.extId}` : null,
        review_date: reviewDate,
        author: r.author || null,
        text: r.text || null,
        rating: r.scoreText ? parseFloat(r.scoreText.replace(',', '.')) : null,
        rating_max: 10,
      });
    }
  } finally {
    await page.close().catch(() => {});
  }

  return reviews;
}

/**
 * Scraper de Google Maps.
 * URL esperada: página de Google Maps del establecimiento,
 *   ej. https://www.google.com/maps/place/Fonda+de+los+Pr%C3%ADncipes/...
 */
async function scrapeGoogleReviews(mapsUrl, { fromDate = null } = {}) {
  const browser = await getBrowser();
  const page = await browser.newPage();
  const reviews = [];

  try {
    await page.setUserAgent(
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    );
    await page.setExtraHTTPHeaders({ 'accept-language': 'es-ES,es;q=0.9' });

    await page.goto(mapsUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });

    // Aceptar cookies de Google si aparece
    try {
      const acceptBtn = await page.$('button[aria-label*="Accept"], button[aria-label*="Aceptar"]');
      if (acceptBtn) { await acceptBtn.click(); await new Promise(r => setTimeout(r, 1500)); }
    } catch {}

    // Hacer clic en la pestaña de reseñas
    try {
      const reviewTab = await page.$('[data-tab-index="1"], [aria-label*="Reseñas"], button[jsaction*="pane.rating"]');
      if (reviewTab) { await reviewTab.click(); await new Promise(r => setTimeout(r, 2000)); }
    } catch {}

    // Esperar las tarjetas de reseña
    await page.waitForSelector(
      'div[data-review-id], .jftiEf, [data-feature-id], [class*="review"]',
      { timeout: 10000 },
    ).catch(() => {});

    // Expandir reseñas largas ("Más")
    try {
      const moreButtons = await page.$$('[jsaction*="pane.review.expandReview"], [aria-label="Ver más"], button[class*="more"]');
      for (const btn of moreButtons.slice(0, 20)) {
        await btn.click().catch(() => {});
        await new Promise(r => setTimeout(r, 200));
      }
    } catch {}

    // Scroll para cargar más reseñas
    try {
      const reviewsPanel = await page.$('div[role="feed"], .m6QErb[data-scroll-hide]');
      if (reviewsPanel) {
        for (let i = 0; i < 5; i++) {
          await page.evaluate((el) => el.scrollTop += 1000, reviewsPanel);
          await new Promise(r => setTimeout(r, 800));
        }
      }
    } catch {}

    const raw = await page.evaluate(() => {
      const items = [];
      const cards = [
        ...document.querySelectorAll('[data-review-id]'),
        ...document.querySelectorAll('.jftiEf'),
        ...document.querySelectorAll('[class*="review-full-text"]').map(el => el.closest('[data-feature-id]')),
      ].filter(Boolean);

      const seen = new Set();
      for (const card of cards.slice(0, 100)) {
        const key = card.dataset?.reviewId || card.dataset?.featureId || card.innerHTML.slice(0, 50);
        if (seen.has(key)) continue;
        seen.add(key);

        // Puntuación: estrellas
        const starsEl = card.querySelector('[aria-label*="estrella"], [aria-label*="star"]');
        const starsLabel = starsEl ? starsEl.getAttribute('aria-label') : null;
        const starsMatch = starsLabel ? starsLabel.match(/(\d[,.]?\d*)/) : null;
        const rating = starsMatch ? parseFloat(starsMatch[1].replace(',', '.')) : null;

        // Fecha
        const dateEl = card.querySelector('.rsqaWe, [class*="date"], span[jsaction]');
        const dateText = dateEl ? dateEl.textContent.trim() : null;

        // Texto
        const textEl = card.querySelector('.wiI7pd, [class*="review-full-text"], [data-expandable-section] span');
        const text = textEl ? textEl.textContent.trim() : null;

        // Autor
        const authorEl = card.querySelector('.d4r55, [class*="author"], [class*="display-name"]');
        const author = authorEl ? authorEl.textContent.trim() : null;

        const extId = card.dataset?.reviewId || null;

        items.push({ rating, dateText, text, author, extId });
      }

      return items;
    });

    for (const r of raw) {
      const reviewDate = parseRelativeDate(r.dateText);
      if (fromDate && reviewDate && reviewDate <= fromDate) continue;

      reviews.push({
        source: SOURCE_GOOGLE,
        external_id: r.extId ? `google:${r.extId}` : null,
        review_date: reviewDate,
        author: r.author || null,
        text: r.text || null,
        rating: r.rating,
        rating_max: 5,
      });
    }
  } finally {
    await page.close().catch(() => {});
  }

  return reviews;
}

/**
 * Scraper de TripAdvisor.
 * URL esperada: página de reseñas del hotel en TripAdvisor,
 *   ej. https://www.tripadvisor.es/Hotel_Review-g187496-d1234567-Reviews-Nombre.html
 */
async function scrapeTripAdvisorReviews(taUrl, { fromDate = null } = {}) {
  const browser = await getBrowser();
  const page = await browser.newPage();
  const reviews = [];

  try {
    await page.setUserAgent(
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    );
    await page.setExtraHTTPHeaders({ 'accept-language': 'es-ES,es;q=0.9' });

    await page.goto(taUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });

    // Aceptar cookies
    try {
      const cookieBtn = await page.$('#onetrust-accept-btn-handler, [id*="accept"]');
      if (cookieBtn) { await cookieBtn.click(); await new Promise(r => setTimeout(r, 1000)); }
    } catch {}

    // Esperar tarjetas de reseñas
    await page.waitForSelector(
      '[data-automation="reviewCard"], .reviewSelector, .review-container',
      { timeout: 12000 },
    ).catch(() => {});

    // Expandir reseñas completas
    try {
      const moreLinks = await page.$$('span[onclick*="readMore"], .taLnk[onclick*="READ_MORE"], button[class*="more"]');
      for (const btn of moreLinks.slice(0, 20)) {
        await btn.click().catch(() => {});
        await new Promise(r => setTimeout(r, 200));
      }
    } catch {}

    const raw = await page.evaluate(() => {
      const items = [];

      const selectors = [
        '[data-automation="reviewCard"]',
        '.reviewSelector',
        '.review-container',
        '[class*="review_"]',
      ];

      let cards = [];
      for (const sel of selectors) {
        const found = [...document.querySelectorAll(sel)];
        if (found.length > 0) { cards = found; break; }
      }

      for (const card of cards.slice(0, 100)) {
        // Puntuación: buscar elementos con bubble o circle de TripAdvisor
        const ratingEl = card.querySelector(
          '[class*="ui_bubble_rating"], [class*="bubbles"], svg[class*="rating"]',
        );
        let rating = null;
        if (ratingEl) {
          const cls = ratingEl.className || '';
          const m = cls.match(/bubble_(\d+)/);
          if (m) rating = parseInt(m[1]) / 10;
          if (!rating) {
            const ariaLabel = ratingEl.getAttribute('aria-label') || '';
            const am = ariaLabel.match(/(\d[,.]?\d*)/);
            if (am) rating = parseFloat(am[1].replace(',', '.'));
          }
        }

        // Fecha
        const dateEl = card.querySelector(
          '[data-automation="reviewedDate"], .ratingDate, span[class*="date"]',
        );
        const dateText = dateEl
          ? (dateEl.getAttribute('title') || dateEl.textContent.trim())
          : null;

        // Texto
        const textEl = card.querySelector(
          '[data-automation="reviewBody"] span, .partial_entry, .reviewSelector span[class]',
        );
        const text = textEl ? textEl.textContent.trim() : null;

        // Título
        const titleEl = card.querySelector(
          '[data-automation="reviewTitle"], .noQuotes, .title',
        );
        const title = titleEl ? titleEl.textContent.trim() : null;

        // Autor
        const authorEl = card.querySelector(
          '[class*="member_info"] .username, .info_text .username, [class*="reviewer"] [class*="name"]',
        );
        const author = authorEl ? authorEl.textContent.trim() : null;

        // ID externo
        const extId = card.dataset?.reviewid || card.getAttribute('data-reviewid') || null;

        const fullText = [title, text].filter(Boolean).join(' — ') || null;
        items.push({ rating, dateText, fullText, author, extId });
      }

      return items;
    });

    for (const r of raw) {
      const reviewDate = parseSpanishDate(r.dateText);
      if (fromDate && reviewDate && reviewDate <= fromDate) continue;

      reviews.push({
        source: SOURCE_TRIPADVISOR,
        external_id: r.extId ? `tripadvisor:${r.extId}` : null,
        review_date: reviewDate,
        author: r.author || null,
        text: r.fullText || null,
        rating: r.rating,
        rating_max: 5,
      });
    }
  } finally {
    await page.close().catch(() => {});
  }

  return reviews;
}

// ─── Función principal ───────────────────────────────────────────────────────

/**
 * Raspa las tres plataformas configuradas.
 * @param {Object} fromDates  - { booking: 'YYYY-MM-DD' | null, google: ..., tripadvisor: ... }
 * @returns {{ reviews: Array, errors: Array }}
 */
async function scrapeAllReviews(fromDates = {}) {
  const allReviews = [];
  const errors = [];

  const config = [
    {
      key: SOURCE_BOOKING,
      url: process.env.BOOKING_REVIEWS_URL,
      label: 'Booking.com',
      fn: scrapeBookingReviews,
    },
    {
      key: SOURCE_GOOGLE,
      url: process.env.GOOGLE_REVIEWS_URL,
      label: 'Google Maps',
      fn: scrapeGoogleReviews,
    },
    {
      key: SOURCE_TRIPADVISOR,
      url: process.env.TRIPADVISOR_REVIEWS_URL,
      label: 'TripAdvisor',
      fn: scrapeTripAdvisorReviews,
    },
  ];

  for (const { key, url, label, fn } of config) {
    if (!url) {
      console.log(`[opiniones] ${label}: URL no configurada (${key.toUpperCase()}_REVIEWS_URL). Omitiendo.`);
      continue;
    }

    try {
      console.log(`[opiniones] Scraping ${label}…`);
      const fromDate = fromDates[key] || null;
      const reviews = await fn(url, { fromDate });
      console.log(`[opiniones] ${label}: ${reviews.length} reseñas obtenidas.`);
      allReviews.push(...reviews);
    } catch (err) {
      console.error(`[opiniones] Error scraping ${label}:`, err.message);
      errors.push({ source: key, label, error: err.message });
    }
  }

  return { reviews: allReviews, errors };
}

module.exports = { scrapeAllReviews, SOURCE_BOOKING, SOURCE_GOOGLE, SOURCE_TRIPADVISOR };
