const { readFileSync } = require("fs");
const { resolve } = require("path");

const envPath = resolve(__dirname, "..", ".env");
try {
  const lines = readFileSync(envPath, "utf8").split("\n");
  for (const line of lines) {
    const match = line.match(/^\s*([A-Z_][A-Z0-9_]*)\s*=\s*(.*)\s*$/);
    if (match && !(match[1] in process.env)) {
      process.env[match[1]] = match[2];
    }
  }
} catch {}

const app = require("./app");
const PORT = process.env.PORT || 8000;
app.listen(PORT, () => {
  console.log(`Servidor listo en http://localhost:${PORT}`);
});
