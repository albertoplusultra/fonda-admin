const fs = require("fs");
const path = require("path");
const XLSX = require("xlsx");
const archiver = require("archiver");

const ASSETS_DIR = path.join(__dirname, "assets");
const FONT_PATH = path.join(ASSETS_DIR, "Lucida-Calligraphy.ttf");
const LOGO_CANDIDATES = [
  path.join(ASSETS_DIR, "logo_fonda.svg"),
  path.join(ASSETS_DIR, "logo_fonda.png"),
  path.join(ASSETS_DIR, "logo_opt.jpg"),
  path.join(ASSETS_DIR, "logo_opt.png"),
];

const FEMALE_NAMES = new Set([
  "maria", "ana", "laura", "sofia", "lucia", "paula", "valeria", "martina",
  "daniela", "emilia", "jimena", "georgina", "carmen", "isabel", "elizabeth",
  "jennifer", "linda", "susan", "jessica", "sarah", "karen", "nancy", "emily",
  "rachel", "anna", "elsa", "aila", "becky", "bibiane", "marta", "elena",
  "cristina", "beatriz", "rosa", "julia", "teresa", "alicia", "clara", "patricia",
]);

const MASCULINE_NAMES = new Set([
  "manuel", "jose", "antonio", "francisco", "juan", "david", "javier", "daniel",
  "pablo", "hussain", "michael", "washington", "nasir", "junior", "raony",
  "kulwinder", "leon", "walter", "julien", "bradley", "noam", "denis", "luis",
  "dave", "leendert", "andres", "alberto", "carlos", "miguel", "angel", "rafael",
  "fernando", "pedro", "sergio", "marcos", "jorge", "raul",
]);

const SPANISH_KEYWORDS = [
  "garcia", "fernandez", "gonzalez", "rodriguez", "lopez", "martinez", "sanchez",
  "perez", "martin", "jimenez", "ruiz", "hernandez", "diaz", "moreno", "muñoz",
  "alvarez", "romero", "alonso", "gutierrez", "navarro", "torres", "ramirez",
  "serrano", "molina", "morales", "ortiz", "delgado", "castro", "rubio", "marin",
  "ortega", "cruz", "guerrero", "reyes", "prieto", "vazquez", "ramos", "pascual",
  "blanco", "suarez", "manuel", "maria", "jose", "antonio", "francisco", "juan",
  "david", "javier", "daniel", "pablo", "carmen", "isabel", "laura", "sofia",
  "ana", "lucia", "paula", "valeria", "martina", "daniela", "emilia", "aragones",
  "pozas", "jimena", "georgina",
];

const ENGLISH_KEYWORDS = [
  "smith", "johnson", "williams", "brown", "jones", "miller", "davis", "wilson",
  "moore", "taylor", "thomas", "jackson", "white", "harris", "martin", "thompson",
  "tom", "john", "james", "mary", "patricia", "jennifer", "linda", "elizabeth",
  "susan", "jessica", "sarah", "karen", "nancy", "hussain", "michael", "mc",
  "washington", "marel", "mariko", "nasir", "junior", "raony", "kulwinder",
  "leon", "walter", "julien", "bradley", "noam", "bibiane", "denis", "luis",
  "rachel", "dave", "anna", "elsa", "aila", "alexandre", "leendert", "becky",
];

function imgToBase64(filePath) {
  if (!fs.existsSync(filePath)) return "";
  const data = fs.readFileSync(filePath);
  let mime = "image/png";
  const headerText = data.toString("utf8", 0, 512).trimStart().toLowerCase();
  if (headerText.startsWith("<svg") || headerText.startsWith("<?xml")) {
    mime = "image/svg+xml";
  } else if (data.length >= 4 && data.toString("ascii", 1, 4) === "PNG") {
    mime = "image/png";
  } else if (data.length >= 2 && data[0] === 0x47 && data[1] === 0x49) {
    mime = "image/gif";
  } else if (data.length >= 4 && data[0] === 0xff && data[1] === 0xd8 && data[2] === 0xff) {
    mime = "image/jpeg";
  } else if (
    data.length >= 12 &&
    data.toString("ascii", 0, 4) === "RIFF" &&
    data.toString("ascii", 8, 12) === "WEBP"
  ) {
    mime = "image/webp";
  }
  return `data:${mime};base64,${data.toString("base64")}`;
}

function fontToBase64() {
  if (!fs.existsSync(FONT_PATH)) return "";
  const data = fs.readFileSync(FONT_PATH);
  return `data:font/truetype;base64,${data.toString("base64")}`;
}

function resolveLogoPath() {
  for (const logoPath of LOGO_CANDIDATES) {
    if (fs.existsSync(logoPath)) return logoPath;
  }
  throw new Error("No se encontró el logotipo en server/assets.");
}

async function launchBrowser() {
  // En Vercel usamos chromium serverless; en local usamos puppeteer completo.
  if (process.env.VERCEL) {
    const puppeteerCore = require("puppeteer-core");
    const chromium = require("@sparticuz/chromium");

    return puppeteerCore.launch({
      args: chromium.args,
      executablePath: await chromium.executablePath(),
      headless: true,
    });
  }

  const puppeteer = require("puppeteer");
  return puppeteer.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });
}

function detectLanguage(fullName) {
  const lower = fullName.toLowerCase();
  for (const kw of SPANISH_KEYWORDS) {
    if (lower.includes(kw)) return "es";
  }
  for (const kw of ENGLISH_KEYWORDS) {
    if (lower.includes(kw)) return "en";
  }
  return "en";
}

function detectGender(firstName) {
  const lower = firstName.toLowerCase();
  if (FEMALE_NAMES.has(lower)) return "F";
  if (MASCULINE_NAMES.has(lower)) return "M";
  if (lower.endsWith("a")) return "F";
  return "M";
}

function getCartaBody(firstName, gender, language) {
  if (language === "es") {
    const saludo = gender === "F" ? `Estimada ${firstName},` : `Estimado ${firstName},`;
    const bienvenida = gender === "F"
      ? "Bienvenida a la Fonda de los Príncipes, mi sueño."
      : "Bienvenido a la Fonda de los Príncipes, mi sueño.";
    return `
<p>${saludo}</p>
<p>${bienvenida}</p>
<p>Un sueño que recupera el nombre original del siglo XIX para conectar con el pasado y homenajear la rica historia de la Puerta del Sol.</p>
<p>La Fonda de los Príncipes abrió sus puertas por primera vez el 1 de octubre de 1861 como uno de los primeros y más lujosos alojamientos de la Puerta del Sol. Con el paso del tiempo fue adquiriendo distintos nombres: Hotel de Los Príncipes, Hotel de la Paix, Hotel Americano, Pensión Americana y, finalmente, hasta su cierre en marzo de 2020, Hostal Americano.</p>
<p>Tras adquirirlo en 2024 y después de una gran reforma, el 5 de enero de 2026, la noche de Reyes, casi 165 años después de la inauguración original, renace la nueva Fonda de los Príncipes.</p>
<p>Gracias por ser parte de esta historia.</p>`;
  }

  const saludo = `Dear ${firstName},`;
  const bienvenida = "Welcome to La Fonda de los Príncipes, my dream.";
  return `
<p>${saludo}</p>
<p>${bienvenida}</p>
<p>A dream that revives the original 19th-century name to connect with the past and honor the rich history of Puerta del Sol.</p>
<p>La Fonda de los Príncipes first opened its doors on October 1, 1861, as one of the first and most luxurious accommodations in Puerta del Sol. Over time, it acquired various names: Hotel de Los Príncipes, Hotel de la Paix, Hotel Americano, Pensión Americana, and finally, until its closure in March 2020, Hostal Americano.</p>
<p>After acquiring it in 2024 and undergoing a major renovation, on January 5, 2026, on Three Kings' Day, almost 165 years after its original inauguration, the new Fonda de los Príncipes is reborn.</p>
<p>Thank you for being a part of this story.</p>`;
}

function buildHtml(firstName, logoB64, fontB64, gender, language) {
  const body = getCartaBody(firstName, gender, language);

  const fontFace = fontB64
    ? `@font-face { font-family: 'LucidaCalligraphy'; src: url('${fontB64}') format('truetype'); }`
    : "";

  const logoBlock = `<div class="logo-container"><img src="${logoB64}"></div>`;

  return `<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
${fontFace}
@page { size: A4; margin: 2.0cm 2.5cm 2.0cm 2.5cm; }
body {
  font-family: 'LucidaCalligraphy', 'Segoe Script', cursive;
  font-size: 16pt;
  line-height: 1.6;
  color: #1a1a1a;
  text-align: justify;
}
.logo-container { text-align: center; margin-bottom: 30pt; }
.logo-container img { width: 85%; max-width: 450pt; }
.logo-container h1 { font-size: 24pt; color: #2c1810; }
p { margin-bottom: 15pt; }
</style>
</head>
<body>
${logoBlock}
${body}
</body>
</html>`;
}

function parseExcel(buffer) {
  const workbook = XLSX.read(buffer, { type: "buffer" });
  const sheetName = workbook.SheetNames[0];
  const sheet = workbook.Sheets[sheetName];
  const rows = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: "" });

  if (!rows.length) throw new Error("El archivo Excel está vacío.");

  const normalize = (value) =>
    String(value || "")
      .trim()
      .toUpperCase()
      .normalize("NFD")
      .replace(/\p{Diacritic}/gu, "");

  let headerRowIndex = -1;
  let bestScore = -1;
  for (let i = 0; i < Math.min(rows.length, 30); i += 1) {
    const normalizedRow = rows[i].map(normalize);
    const hasTitular = normalizedRow.includes("TITULAR");
    const hasNombre = normalizedRow.includes("NOMBRE");
    const hasHab = normalizedRow.includes("HABITACION") || normalizedRow.includes("HAB");
    const hasPreCheckin = normalizedRow.includes("PRE-CHECKIN");

    let score = 0;
    if (hasTitular) score += 10;
    if (hasHab) score += 5;
    if (hasPreCheckin) score += 2;
    if (!hasTitular && hasNombre) score += 1;

    if (score > bestScore) {
      bestScore = score;
      headerRowIndex = i;
    }
  }

  if (headerRowIndex === -1 || bestScore <= 0) {
    throw new Error("No se encontró la fila de encabezados con la columna 'TITULAR' o 'NOMBRE'.");
  }

  const headerRow = rows[headerRowIndex].map(normalize);
  const idxTitular = headerRow.findIndex((h) => h === "TITULAR" || h === "NOMBRE");
  const idxHab = headerRow.findIndex((h) => h === "HABITACION" || h === "HAB");

  if (idxTitular === -1) {
    throw new Error("No se encontró la columna 'TITULAR' o 'NOMBRE'.");
  }

  const guests = [];
  for (let i = headerRowIndex + 1; i < rows.length; i += 1) {
    const row = rows[i];
    const fullName = String(row[idxTitular] || "").trim();
    if (!fullName) continue;
    if (!/\p{L}/u.test(fullName)) continue;

    const hab = idxHab !== -1 ? String(row[idxHab] || "N-A").trim() : "N-A";
    const firstName = (fullName.includes(",") ? fullName.split(",")[1] : fullName)
      .trim()
      .split(" ")[0];
    if (!firstName) continue;

    const capitalized = firstName.charAt(0).toUpperCase() + firstName.slice(1).toLowerCase();

    guests.push({
      fullName,
      firstName: capitalized,
      habitacion: hab || "N-A",
      language: detectLanguage(fullName),
      gender: detectGender(capitalized),
    });
  }

  if (!guests.length) throw new Error("No se encontraron huéspedes válidos en el Excel.");

  return guests;
}

async function generateCartasZip(excelBuffer) {
  const guests = parseExcel(excelBuffer);
  const logoPath = resolveLogoPath();
  const logoB64 = imgToBase64(logoPath);
  const fontB64 = fontToBase64();

  const browser = await launchBrowser();

  const report = [];

  try {
    const page = await browser.newPage();
    page.setDefaultNavigationTimeout(0);
    page.setDefaultTimeout(0);

    const pdfBuffers = [];
    for (const guest of guests) {
      const html = buildHtml(guest.firstName, logoB64, fontB64, guest.gender, guest.language);
      await page.setContent(html, { waitUntil: "domcontentloaded" });

      const pdfBufferRaw = await page.pdf({
        format: "A4",
        margin: { top: "2cm", bottom: "2cm", left: "2.5cm", right: "2.5cm" },
        printBackground: true,
      });
      const pdfBuffer = Buffer.from(pdfBufferRaw);

      const filename = `Carta_Hab${guest.habitacion}_${guest.firstName}_${guest.language.toUpperCase()}.pdf`
        .replace(/\//g, "-");

      pdfBuffers.push({ filename, buffer: pdfBuffer });
      report.push({
        nombre: guest.fullName,
        habitacion: guest.habitacion,
        idioma: guest.language.toUpperCase(),
        genero: guest.gender,
        archivo: filename,
      });
    }

    const zipBuffer = await new Promise((resolve, reject) => {
      const buffers = [];
      const archive = archiver("zip", { zlib: { level: 9 } });
      archive.on("data", (chunk) => buffers.push(chunk));
      archive.on("end", () => resolve(Buffer.concat(buffers)));
      archive.on("error", reject);

      for (const { filename, buffer } of pdfBuffers) {
        archive.append(buffer, { name: filename });
      }
      archive.finalize();
    });

    return { zipBuffer, report };
  } finally {
    await browser.close();
  }
}

module.exports = { generateCartasZip };
