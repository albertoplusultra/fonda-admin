const path = require("path");
const express = require("express");
const multer = require("multer");

const { generateCartasZip } = require("./cartas");

const app = express();
const upload = multer({ storage: multer.memoryStorage() });

const STATIC_DIR = path.join(__dirname, "static");

app.use("/static", express.static(STATIC_DIR));

app.get("/", (_req, res) => {
  res.sendFile(path.join(STATIC_DIR, "index.html"));
});

app.post("/api/generar-cartas", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: "No se envió ningún archivo." });
    }

    if (!req.file.originalname.match(/\.xlsx?$/i)) {
      return res.status(400).json({ error: "Solo se aceptan archivos Excel (.xlsx)." });
    }

    const { zipBuffer, report } = await generateCartasZip(req.file.buffer);

    res.setHeader("Content-Type", "application/zip");
    res.setHeader("Content-Disposition", "attachment; filename=Cartas_Bienvenida.zip");
    res.setHeader("X-Cartas-Count", String(report.length));
    return res.status(200).send(zipBuffer);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Error inesperado";
    // Logging del error para diagnóstico en terminal.
    console.error("Error al generar cartas:", error);
    const isUserError =
      message.includes("No se encontró la columna") ||
      message.includes("Excel está vacío") ||
      message.includes("No se encontraron huéspedes válidos");

    return res.status(isUserError ? 422 : 500).json({ error: message });
  }
});

const PORT = process.env.PORT || 8000;
app.listen(PORT, () => {
  console.log(`Servidor listo en http://localhost:${PORT}`);
});
