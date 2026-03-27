# Fonda de los Príncipes — Admin

Aplicación de administración para generar cartas de bienvenida personalizadas para los huéspedes.

## Requisitos

- Node.js 20+ (recomendado 22+)

## Instalación

```bash
npm install
```

## Assets

Coloca en `server/assets/`:
- `Lucida-Calligraphy.ttf` — fuente para las cartas (ya incluida)
- `logo_fonda.svg` — logo oficial (prioridad 1, recomendado para máxima nitidez)
- `logo_fonda.png` — alternativa si no hay SVG

## Ejecución

```bash
npm run dev
```

Abrir http://localhost:8000 en el navegador.

## Uso

1. Subir un archivo Excel (.xlsx) con columnas **TITULAR** y **HABITACIÓN**
2. Pulsar **Publicar cartas**
3. Se descarga automáticamente un ZIP con un PDF personalizado por huésped

El sistema detecta automáticamente el idioma (ES/EN) y género para personalizar el saludo.

## Deploy en Vercel

Este repositorio ya está preparado para Vercel con:
- endpoint serverless en `api/index.js`
- configuración en `vercel.json`
- soporte de Puppeteer en Vercel con `@sparticuz/chromium` + `puppeteer-core`

Pasos:
1. Importar el repo en Vercel.
2. Framework Preset: `Other`.
3. Build Command: vacío.
4. Output Directory: vacío.
5. Install Command: `npm install`.

La ruta principal es `/` y la API de generación es `/api/generar-cartas`.
