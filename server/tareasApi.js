const express = require("express");
const {
  listTareas,
  listTareasResumen,
  listTareasResponsables,
  getTareaById,
  createTarea,
  updateTarea,
  addTareaHistorial,
  reordenarTareas,
  moverTareaRelativo,
} = require("./db");

const router = express.Router();

function noDb(res) {
  return res.status(503).json({
    error:
      "La base de datos no está configurada. Añade TURSO_DATABASE_URL (y TURSO_AUTH_TOKEN si aplica) para guardar las tareas.",
  });
}

router.get("/resumen", async (_req, res) => {
  try {
    const data = await listTareasResumen();
    if (data === null) return noDb(res);
    return res.json(data);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Error inesperado";
    console.error("Error en GET /api/tareas/resumen:", error);
    return res.status(500).json({ error: message });
  }
});

router.get("/responsables", async (_req, res) => {
  try {
    const rows = await listTareasResponsables();
    if (rows === null) return noDb(res);
    return res.json(rows);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Error inesperado";
    console.error("Error en GET /api/tareas/responsables:", error);
    return res.status(500).json({ error: message });
  }
});

router.post("/reordenar", async (req, res) => {
  try {
    const ids = req.body?.ids;
    const ok = await reordenarTareas(ids);
    if (ok === null) return noDb(res);
    const rows = await listTareas({
      responsable: req.body?.responsable,
      estado: req.body?.estado,
      ordenar: "orden",
      direccion: "asc",
    });
    if (rows === null) return noDb(res);
    return res.json(rows);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Error inesperado";
    const code = message.includes("orden") || message.includes("Identificadores") ? 422 : 500;
    console.error("Error en POST /api/tareas/reordenar:", error);
    return res.status(code).json({ error: message });
  }
});

router.post("/:id/mover", async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!Number.isInteger(id) || id < 1) {
      return res.status(400).json({ error: "Identificador de tarea no válido." });
    }
    const dir = String(req.body?.direccion || "");
    if (dir !== "arriba" && dir !== "abajo") {
      return res.status(400).json({ error: 'Use direccion: "arriba" o "abajo".' });
    }
    const moved = await moverTareaRelativo(id, dir === "arriba" ? "up" : "down");
    if (moved === null) return noDb(res);
    if (!moved) return res.status(404).json({ error: "Tarea no encontrada." });
    return res.json(moved);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Error inesperado";
    console.error("Error en POST /api/tareas/:id/mover:", error);
    return res.status(500).json({ error: message });
  }
});

router.get("/", async (req, res) => {
  try {
    const ordenar = String(req.query.ordenar || "orden");
    const direccion = String(req.query.direccion || "asc");
    const rows = await listTareas({
      responsable: req.query.responsable,
      estado: req.query.estado,
      ordenar,
      direccion,
    });
    if (rows === null) return noDb(res);
    return res.json(rows);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Error inesperado";
    console.error("Error en GET /api/tareas:", error);
    return res.status(500).json({ error: message });
  }
});

router.post("/", async (req, res) => {
  try {
    const body = req.body || {};
    const created = await createTarea({
      estado: body.estado,
      edificio: body.edificio,
      asunto: body.asunto,
      importancia: body.importancia,
      responsable: body.responsable,
      fecha_limite: body.fecha_limite,
      comentario_inicial: body.comentario_inicial,
      autor: body.autor,
    });
    if (created === null) return noDb(res);
    return res.status(201).json(created);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Error inesperado";
    const code = message.includes("obligatorio") ? 422 : 500;
    console.error("Error en POST /api/tareas:", error);
    return res.status(code).json({ error: message });
  }
});

router.get("/:id", async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!Number.isInteger(id) || id < 1) {
      return res.status(400).json({ error: "Identificador de tarea no válido." });
    }
    const row = await getTareaById(id);
    if (row === null) return noDb(res);
    if (!row) return res.status(404).json({ error: "Tarea no encontrada." });
    return res.json(row);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Error inesperado";
    console.error("Error en GET /api/tareas/:id:", error);
    return res.status(500).json({ error: message });
  }
});

router.patch("/:id", async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!Number.isInteger(id) || id < 1) {
      return res.status(400).json({ error: "Identificador de tarea no válido." });
    }
    const updated = await updateTarea(id, req.body || {});
    if (updated === null) return noDb(res);
    if (!updated) return res.status(404).json({ error: "Tarea no encontrada." });
    return res.json(updated);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Error inesperado";
    const code = message.includes("vacío") ? 422 : 500;
    console.error("Error en PATCH /api/tareas/:id:", error);
    return res.status(code).json({ error: message });
  }
});

router.post("/:id/historial", async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!Number.isInteger(id) || id < 1) {
      return res.status(400).json({ error: "Identificador de tarea no válido." });
    }
    const body = req.body || {};
    const updated = await addTareaHistorial(id, { texto: body.texto, autor: body.autor });
    if (updated === null) return noDb(res);
    if (!updated) return res.status(404).json({ error: "Tarea no encontrada." });
    return res.status(201).json(updated);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Error inesperado";
    const code = message.includes("vacío") ? 422 : 500;
    console.error("Error en POST /api/tareas/:id/historial:", error);
    return res.status(code).json({ error: message });
  }
});

module.exports = router;
