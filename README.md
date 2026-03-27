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
- `logo_opt.jpg` — logo de la Fonda (opcional, se muestra un texto si no existe)

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
