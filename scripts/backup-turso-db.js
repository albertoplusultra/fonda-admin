/**
 * Copia de seguridad de la base Turso (libSQL remota).
 * Exporta cada tabla a JSONL en una carpeta bajo backups/.
 *
 * Uso: node scripts/backup-turso-db.js
 */
require("dotenv").config({ path: require("path").resolve(__dirname, "..", ".env") });
const fs = require("fs");
const path = require("path");
const { createClient } = require("@libsql/client");

const TABLES = [
  "price_history",
  "tareas",
  "tarea_historial",
  "booking_reviews",
  "google_reviews",
];

const BATCH = 400;

function getClient() {
  const url = process.env.TURSO_DATABASE_URL || process.env.DB_TURSO_DATABASE_URL;
  const authToken = process.env.TURSO_AUTH_TOKEN || process.env.DB_TURSO_AUTH_TOKEN;
  if (!url) {
    console.error("Falta TURSO_DATABASE_URL (o DB_TURSO_DATABASE_URL) en .env");
    process.exit(1);
  }
  return createClient({ url, authToken });
}

function rowToObject(row) {
  if (row && typeof row === "object" && typeof row.toJSON === "function") return row.toJSON();
  return { ...row };
}

async function exportTable(client, table, outDir) {
  const filePath = path.join(outDir, `${table}.jsonl`);
  const stream = fs.createWriteStream(filePath, { flags: "w" });
  let total = 0;
  let offset = 0;
  for (;;) {
    const rs = await client.execute({
      sql: `SELECT * FROM "${table}" LIMIT ? OFFSET ?`,
      args: [BATCH, offset],
    });
    const rows = rs.rows || [];
    if (!rows.length) break;
    for (const row of rows) {
      stream.write(`${JSON.stringify(rowToObject(row))}\n`);
      total += 1;
    }
    offset += rows.length;
    if (rows.length < BATCH) break;
  }
  await new Promise((resolve, reject) => {
    stream.end((err) => (err ? reject(err) : resolve()));
  });
  return total;
}

async function main() {
  const client = getClient();
  const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const root = path.resolve(__dirname, "..", "backups", `fonda-admin-${stamp}`);
  fs.mkdirSync(root, { recursive: true });

  const summary = { exportedAt: new Date().toISOString(), tables: [] };

  for (const table of TABLES) {
    process.stdout.write(`Exportando ${table}… `);
    const n = await exportTable(client, table, root);
    console.log(`${n} filas`);
    summary.tables.push({ name: table, rows: n, file: `${table}.jsonl` });
  }

  fs.writeFileSync(path.join(root, "manifest.json"), JSON.stringify(summary, null, 2), "utf8");
  console.log("\nCopia lista en:\n", root);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
