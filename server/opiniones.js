'use strict';

/**
 * Módulo de opiniones:
 * - Booking.com: fetch + parsing HTML (no necesita Puppeteer)
 * - Google Maps + TripAdvisor: Puppeteer (JS necesario)
 */

const SOURCE_BOOKING     = 'booking';
const SOURCE_GOOGLE      = 'google';
const SOURCE_TRIPADVISOR = 'tripadvisor';

// ─── Utilidades de fecha ────────────────────────────────────────────────────

const MESES_ES = {
  enero:'01', febrero:'02', marzo:'03', abril:'04',
  mayo:'05', junio:'06', julio:'07', agosto:'08',
  septiembre:'09', octubre:'10', noviembre:'11', diciembre:'12',
};

function parseSpanishDate(text) {
  if (!text) return null;
  const s = text.trim().toLowerCase().replace(/\s+/g, ' ').replace(/^comentó en:\s*/i, '');

  if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.slice(0, 10);

  const full = s.match(/(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})/);
  if (full) {
    const m = MESES_ES[full[2]];
    if (m) return `${full[3]}-${m}-${full[1].padStart(2, '0')}`;
  }

  const partial = s.match(/(\w+)\s+de\s+(\d{4})/);
  if (partial) {
    const m = MESES_ES[partial[1]];
    if (m) return `${partial[2]}-${m}-01`;
  }

  const ym = s.match(/(\d{4})-(\d{2})/);
  if (ym) return `${ym[1]}-${ym[2]}-01`;

  return null;
}

function parseRelativeDate(text) {
  if (!text) return null;
  const s = text.trim().toLowerCase();
  const now = new Date();
  let m;
  if ((m = s.match(/hace\s+(\d+)\s+semana/)))  { const d = new Date(now); d.setDate(d.getDate() - Number(m[1]) * 7); return d.toISOString().slice(0, 10); }
  if ((m = s.match(/hace\s+(\d+)\s+mes/)))      { const d = new Date(now); d.setMonth(d.getMonth() - Number(m[1])); return d.toISOString().slice(0, 10); }
  if ((m = s.match(/hace\s+(\d+)\s+año/)))      { const d = new Date(now); d.setFullYear(d.getFullYear() - Number(m[1])); return d.toISOString().slice(0, 10); }
  if ((m = s.match(/hace\s+(\d+)\s+día/)))      { const d = new Date(now); d.setDate(d.getDate() - Number(m[1])); return d.toISOString().slice(0, 10); }
  if (/hace\s+una?\s+semana/.test(s))           { const d = new Date(now); d.setDate(d.getDate() - 7); return d.toISOString().slice(0, 10); }
  if (/hace\s+un\s+mes/.test(s))                { const d = new Date(now); d.setMonth(d.getMonth() - 1); return d.toISOString().slice(0, 10); }
  if (/hace\s+un\s+año/.test(s))                { const d = new Date(now); d.setFullYear(d.getFullYear() - 1); return d.toISOString().slice(0, 10); }
  return parseSpanishDate(text);
}

function decodeHtml(str) {
  return str
    .replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&nbsp;/g, ' ')
    .replace(/&middot;/g, '·').replace(/&hellip;/g, '…').replace(/&ntilde;/g, 'ñ')
    .replace(/&[a-z]+;/gi, '').replace(/&#(\d+);/g, (_, n) => String.fromCharCode(Number(n)));
}

function stripTags(html) {
  return html.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
}

// ─── Booking.com (fetch + HTML parser) ──────────────────────────────────────

async function scrapeBookingReviews(hotelUrl, { fromDate = null } = {}) {
  console.log('[opiniones] Booking: fetching', hotelUrl);

  // Construir URL de reviewlist
  let pagename = '';
  const m = hotelUrl.match(/booking\.com\/(?:reviewlist[^/]*pagename=([^&]+)|hotel\/[a-z]+\/([^.?#/]+))/);
  if (m) pagename = m[1] || m[2];

  const urls = pagename
    ? [`https://www.booking.com/reviewlist.es.html?cc1=es&pagename=${pagename}&rows=75&offset=0`, hotelUrl]
    : [hotelUrl];

  let html = '';
  for (const url of urls) {
    try {
      const res = await fetch(url, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'es-ES,es;q=0.9',
          'Accept-Encoding': 'identity',
          'Referer': 'https://www.booking.com/',
          'Cookie': 'pcm_consent=analytical=0&confirmed=1',
        },
        redirect: 'follow',
        signal: AbortSignal.timeout(25000),
      });
      if (res.ok && res.headers.get('content-type')?.includes('html')) {
        html = await res.text();
        if (html.includes('c-review-block') || html.includes('c-review__body')) break;
      }
    } catch (e) {
      console.warn('[opiniones] Booking fetch error:', e.message);
    }
  }

  if (!html) throw new Error('No se pudo obtener la página de Booking');

  const reviews = [];

  // Parsear por bloques de reseña (cada <li class="review_list_new_item_block"> es una reseña)
  const blockSep = html.split(/<li[^>]*class="review_list_new_item_block"/);
  console.log(`[opiniones] Booking: ${blockSep.length - 1} bloques encontrados`);

  for (const block of blockSep.slice(1, 76)) {
    // Autor
    const authorM = block.match(/class="bui-avatar-block__title">([\s\S]*?)<\/span>/);
    const author = authorM ? decodeHtml(stripTags(authorM[1])) : null;

    // Puntuación (del aria-label)
    const scoreM = block.match(/bui-review-score__badge"[^>]*aria-label="[^"]*?:\s*([0-9,.]+)[^"]*"/);
    const rating = scoreM ? parseFloat(scoreM[1].replace(',', '.')) : null;

    // Fecha de comentario ("Comentó en: ...")
    const dateM = block.match(/class="c-review-block__date">\s*Comentó en:\s*([\s\S]*?)\s*<\/span>/);
    const reviewDate = dateM ? parseSpanishDate(dateM[1].trim()) : null;
    if (fromDate && reviewDate && reviewDate <= fromDate) continue;

    // Textos (puede haber 2: positivo y negativo)
    const textParts = [...block.matchAll(/class="c-review__body"[^>]*>([\s\S]*?)<\/span>/g)]
      .map(m => decodeHtml(stripTags(m[1])).trim())
      .filter(Boolean);
    const text = textParts.join('\n') || null;

    if (!text && !rating) continue;

    reviews.push({
      source: SOURCE_BOOKING,
      external_id: null,
      review_date: reviewDate,
      author,
      text,
      rating,
      rating_max: 10,
    });
  }

  console.log(`[opiniones] Booking: ${reviews.length} reseñas.`);
  return reviews;
}

// ─── Google Maps (Puppeteer) ─────────────────────────────────────────────────

async function scrapeGoogleReviews(mapsUrl, { fromDate = null } = {}) {
  console.log('[opiniones] Google: scraping', mapsUrl);
  const { getBrowser } = require('./booking');
  const browser = await getBrowser();
  const page = await browser.newPage();
  const reviews = [];

  try {
    await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36');
    await page.setExtraHTTPHeaders({ 'accept-language': 'es-ES,es;q=0.9' });

    await page.goto(mapsUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });

    // Aceptar cookies si aparece
    try {
      await page.waitForSelector('button[aria-label*="Accept"], button[aria-label*="Aceptar"], form[action*="consent"] button', { timeout: 5000 });
      const acceptBtn = await page.$('button[aria-label*="Accept"], button[aria-label*="Aceptar"], form[action*="consent"] button:last-child');
      if (acceptBtn) { await acceptBtn.click(); await new Promise(r => setTimeout(r, 1500)); }
    } catch {}

    // Navegar a la pestaña de reseñas
    try {
      await page.waitForSelector('[data-tab-index="1"], [aria-label*="Reseña"], [aria-label*="Review"]', { timeout: 8000 });
      const tab = await page.$('[data-tab-index="1"], [aria-label*="Reseña"], [aria-label*="Review"]');
      if (tab) { await tab.click(); await new Promise(r => setTimeout(r, 2500)); }
    } catch {}

    // Esperar tarjetas de reseñas
    await page.waitForSelector('.jftiEf, [data-review-id]', { timeout: 10000 }).catch(() => {});

    // Ordenar por recientes
    try {
      const sortBtn = await page.$('[aria-label*="Ordenar"], [data-value*="sort"]');
      if (sortBtn) {
        await sortBtn.click();
        await new Promise(r => setTimeout(r, 1000));
        const newestOption = await page.$('[data-index="1"], [data-value*="newest"]');
        if (newestOption) { await newestOption.click(); await new Promise(r => setTimeout(r, 2000)); }
      }
    } catch {}

    // Expandir reseñas largas
    try {
      const moreButtons = await page.$$('button[aria-label*="Más"], [data-expandable-section] button');
      for (const btn of moreButtons.slice(0, 30)) {
        await btn.click().catch(() => {});
        await new Promise(r => setTimeout(r, 100));
      }
    } catch {}

    // Scroll para cargar más
    try {
      const feed = await page.$('div[role="feed"]');
      if (feed) {
        for (let i = 0; i < 8; i++) {
          await page.evaluate(el => el.scrollTop += 800, feed);
          await new Promise(r => setTimeout(r, 600));
        }
        // Expandir botones "Más" tras scroll
        const moreBtns = await page.$$('[data-expandable-section] button, [jsaction*="pane.review.expandReview"]');
        for (const btn of moreBtns.slice(0, 30)) await btn.click().catch(() => {});
        await new Promise(r => setTimeout(r, 500));
      }
    } catch {}

    const raw = await page.evaluate(() => {
      const items = [];
      const cards = [...document.querySelectorAll('.jftiEf, [data-review-id]')];
      const seen = new Set();
      for (const card of cards.slice(0, 100)) {
        const key = card.dataset?.reviewId || card.dataset?.featureId || card.innerHTML.slice(0, 60);
        if (seen.has(key)) continue;
        seen.add(key);

        const starsEl = card.querySelector('[aria-label*="estrella"], [aria-label*="star"], [role="img"]');
        const starsLabel = starsEl ? starsEl.getAttribute('aria-label') : null;
        const starsMatch = starsLabel ? starsLabel.match(/(\d[,.]?\d*)/) : null;
        const rating = starsMatch ? parseFloat(starsMatch[1].replace(',', '.')) : null;

        const dateEl = card.querySelector('.rsqaWe, [class*="date"]');
        const dateText = dateEl ? dateEl.textContent.trim() : null;

        const textEl = card.querySelector('.wiI7pd, [class*="review-full-text"], [data-expandable-section] span[lang]');
        const text = textEl ? textEl.textContent.trim() : null;

        const authorEl = card.querySelector('.d4r55, [class*="display-name"]');
        const author = authorEl ? authorEl.textContent.trim() : null;

        items.push({ rating, dateText, text, author, extId: card.dataset?.reviewId || null });
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

  console.log(`[opiniones] Google: ${reviews.length} reseñas.`);
  return reviews;
}

// ─── TripAdvisor (Puppeteer) ─────────────────────────────────────────────────

async function scrapeTripAdvisorReviews(taUrl, { fromDate = null } = {}) {
  console.log('[opiniones] TripAdvisor: scraping', taUrl);
  const { getBrowser } = require('./booking');
  const browser = await getBrowser();
  const page = await browser.newPage();
  const reviews = [];

  try {
    await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36');
    await page.setExtraHTTPHeaders({ 'accept-language': 'es-ES,es;q=0.9', 'Referer': 'https://www.google.com/' });

    await page.goto(taUrl, { waitUntil: 'domcontentloaded', timeout: 35000 });

    // Aceptar cookies
    try {
      await page.waitForSelector('#onetrust-accept-btn-handler, [id*="accept"], [class*="cookieAccept"]', { timeout: 6000 });
      const btn = await page.$('#onetrust-accept-btn-handler, [id*="accept"]');
      if (btn) { await btn.click(); await new Promise(r => setTimeout(r, 1500)); }
    } catch {}

    // Esperar reseñas
    await page.waitForSelector('[data-automation="reviewCard"], .reviewSelector, [class*="review-container"]', { timeout: 15000 }).catch(() => {});
    await new Promise(r => setTimeout(r, 1000));

    // Expandir textos
    try {
      const moreLinks = await page.$$('[data-automation="expandReview"], [onclick*="readMore"], .taLnk');
      for (const btn of moreLinks.slice(0, 25)) await btn.click().catch(() => {});
      await new Promise(r => setTimeout(r, 500));
    } catch {}

    const raw = await page.evaluate(() => {
      const items = [];
      const cards = [
        ...document.querySelectorAll('[data-automation="reviewCard"]'),
        ...document.querySelectorAll('.reviewSelector'),
        ...document.querySelectorAll('[class*="review-container"]'),
      ];

      const selectors = [
        { sel: '[data-automation="reviewCard"]', key: 'new' },
        { sel: '.reviewSelector', key: 'old' },
      ];

      let found = [];
      for (const { sel } of selectors) {
        found = [...document.querySelectorAll(sel)];
        if (found.length) break;
      }

      const seen = new Set();
      for (const card of found.slice(0, 100)) {
        const uid = card.dataset?.reviewid || card.innerHTML.slice(0, 80);
        if (seen.has(uid)) continue;
        seen.add(uid);

        // Puntuación
        let rating = null;
        const ratingEl = card.querySelector('[class*="ui_bubble_rating"], [class*="bubbles"], [data-automation="bubbleRating"]');
        if (ratingEl) {
          const cls = (ratingEl.className || '');
          const bm = cls.match(/bubble_(\d+)/);
          if (bm) rating = parseInt(bm[1]) / 10;
          if (!rating) {
            const al = ratingEl.getAttribute('aria-label') || '';
            const am = al.match(/(\d[,.]?\d*)/);
            if (am) rating = parseFloat(am[1].replace(',', '.'));
          }
        }

        // Fecha
        const dateEl = card.querySelector('[data-automation="reviewedDate"], .ratingDate, [class*="date"]');
        const dateText = dateEl ? (dateEl.getAttribute('title') || dateEl.textContent.trim()) : null;

        // Texto
        const textEl = card.querySelector('[data-automation="reviewBody"] span, .partial_entry, .reviewSelector .entry');
        const text = textEl ? textEl.textContent.trim() : null;

        // Título
        const titleEl = card.querySelector('[data-automation="reviewTitle"], .noQuotes');
        const title = titleEl ? titleEl.textContent.trim() : null;

        // Autor
        const authorEl = card.querySelector('[class*="member_info"] .username, .info_text .username, [class*="memberName"]');
        const author = authorEl ? authorEl.textContent.trim() : null;

        const fullText = [title, text].filter(Boolean).join(' — ') || null;
        items.push({ rating, dateText, fullText, author, extId: card.dataset?.reviewid || null });
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

  console.log(`[opiniones] TripAdvisor: ${reviews.length} reseñas.`);
  return reviews;
}

// ─── Función principal ───────────────────────────────────────────────────────

async function scrapeAllReviews(fromDates = {}) {
  const allReviews = [];
  const errors = [];

  const config = [
    { key: SOURCE_BOOKING,     url: process.env.BOOKING_REVIEWS_URL,     label: 'Booking.com',  fn: scrapeBookingReviews },
    { key: SOURCE_GOOGLE,      url: process.env.GOOGLE_REVIEWS_URL,      label: 'Google Maps',  fn: scrapeGoogleReviews },
    { key: SOURCE_TRIPADVISOR, url: process.env.TRIPADVISOR_REVIEWS_URL, label: 'TripAdvisor',  fn: scrapeTripAdvisorReviews },
  ];

  for (const { key, url, label, fn } of config) {
    if (!url) {
      console.log(`[opiniones] ${label}: URL no configurada. Omitiendo.`);
      continue;
    }
    try {
      console.log(`[opiniones] Scraping ${label}…`);
      const fromDate = fromDates[key] || null;
      const result = await fn(url, { fromDate });
      console.log(`[opiniones] ${label}: ${result.length} reseñas.`);
      allReviews.push(...result);
    } catch (err) {
      console.error(`[opiniones] Error en ${label}:`, err.message);
      errors.push({ source: key, label, error: err.message });
    }
  }

  return { reviews: allReviews, errors };
}

module.exports = { scrapeAllReviews, SOURCE_BOOKING, SOURCE_GOOGLE, SOURCE_TRIPADVISOR };
