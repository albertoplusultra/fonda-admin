'use strict';

const { Router } = require('express');
const jwt = require('jsonwebtoken');

const ALLOWED_DOMAINS = [
  'plusultracapital.com',
  'plusultraproperties.com',
  'lafondadelosprincipes.com',
];

const COOKIE_NAME = 'fonda_session';
const COOKIE_MAX_AGE = 7 * 24 * 60 * 60 * 1000; // 7 días en ms
const SCOPES = 'openid email profile';

// ── Helpers ───────────────────────────────────────────────────────────────────

function getConfig() {
  const clientId     = process.env.GOOGLE_CLIENT_ID;
  const clientSecret = process.env.GOOGLE_CLIENT_SECRET;
  const baseUrl      = process.env.BASE_URL || 'http://localhost:8000';
  const jwtSecret    = process.env.JWT_SECRET;

  if (!clientId || !clientSecret || !jwtSecret) {
    throw new Error(
      'Faltan variables de entorno: GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET y/o JWT_SECRET',
    );
  }

  return { clientId, clientSecret, baseUrl, jwtSecret };
}

function redirectUri(baseUrl) {
  return `${baseUrl}/auth/callback`;
}

// ── Middleware de autenticación ───────────────────────────────────────────────

function requireAuth(req, res, next) {
  const token = req.cookies?.[COOKIE_NAME];
  if (!token) return res.redirect('/login');

  try {
    req.user = jwt.verify(token, process.env.JWT_SECRET);
    next();
  } catch {
    res.clearCookie(COOKIE_NAME);
    res.redirect('/login');
  }
}

// ── Router de auth ────────────────────────────────────────────────────────────

const router = Router();

// Página de login
router.get('/login', (_req, res) => {
  res.sendFile(require('path').join(__dirname, 'static', 'login.html'));
});

// Iniciar flujo OAuth → redirigir a Google
router.get('/auth/google', (_req, res) => {
  let cfg;
  try { cfg = getConfig(); } catch (e) {
    return res.status(500).send(`Error de configuración: ${e.message}`);
  }

  const params = new URLSearchParams({
    client_id:     cfg.clientId,
    redirect_uri:  redirectUri(cfg.baseUrl),
    response_type: 'code',
    scope:         SCOPES,
    access_type:   'online',
    prompt:        'select_account',
  });

  res.redirect(`https://accounts.google.com/o/oauth2/v2/auth?${params}`);
});

// Callback de Google → verificar dominio → setear cookie JWT
router.get('/auth/callback', async (req, res) => {
  const { code, error } = req.query;

  if (error || !code) {
    return res.redirect('/login?error=cancelled');
  }

  let cfg;
  try { cfg = getConfig(); } catch (e) {
    return res.status(500).send(`Error de configuración: ${e.message}`);
  }

  try {
    // Intercambiar código por tokens
    const tokenRes = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        code,
        client_id:     cfg.clientId,
        client_secret: cfg.clientSecret,
        redirect_uri:  redirectUri(cfg.baseUrl),
        grant_type:    'authorization_code',
      }),
    });

    const tokens = await tokenRes.json();
    if (!tokenRes.ok) throw new Error(tokens.error_description || 'Error al obtener token');

    // Obtener datos del usuario
    const userRes = await fetch('https://www.googleapis.com/oauth2/v3/userinfo', {
      headers: { Authorization: `Bearer ${tokens.access_token}` },
    });
    const user = await userRes.json();
    if (!userRes.ok) throw new Error('Error al obtener perfil de usuario');

    // Verificar dominio
    const email = (user.email || '').toLowerCase();
    const domain = email.split('@')[1] || '';
    if (!ALLOWED_DOMAINS.includes(domain)) {
      return res.redirect('/login?error=domain');
    }

    // Crear cookie JWT de 7 días
    const payload = { email, name: user.name, picture: user.picture };
    const token = jwt.sign(payload, cfg.jwtSecret, { expiresIn: '7d' });

    res.cookie(COOKIE_NAME, token, {
      httpOnly: true,
      sameSite: 'lax',
      secure:   process.env.NODE_ENV === 'production',
      maxAge:   COOKIE_MAX_AGE,
    });

    res.redirect('/');
  } catch (err) {
    console.error('[auth] Error en callback:', err);
    res.redirect('/login?error=server');
  }
});

// Cerrar sesión
router.get('/auth/logout', (req, res) => {
  res.clearCookie(COOKIE_NAME);
  res.redirect('/login');
});

module.exports = { router, requireAuth };
