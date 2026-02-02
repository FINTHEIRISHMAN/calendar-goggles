#!/usr/bin/env python3
"""
REST API Server (stdlib only — no Flask needed)

Serves the bourbon release calendar data and the frontend
using Python's built-in http.server module.

Endpoints:
    GET  /api/releases          - All releases (with query-string filters)
    GET  /api/releases/<id>     - Single release detail
    GET  /api/months            - Month summary (counts per month)
    GET  /api/distilleries      - List of distilleries
    GET  /api/stats             - Dashboard stats
    GET  /                      - Frontend SPA (static files)
"""
import sys
import os
import json
import re
import time
import collections
from http.server import HTTPServer, SimpleHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

sys.path.insert(0, os.path.dirname(__file__))

from lib.db import (
    get_db,
    get_all_releases,
    get_release_by_id,
    get_month_summary,
    get_distilleries,
    get_stats,
)

PORT = int(os.environ.get('PORT', 3000))
STATIC_DIR = os.path.realpath(os.path.join(os.path.dirname(__file__), 'static'))

# ── Rate Limiting ─────────────────────────────────────────────────────────────
# Simple in-memory rate limiter: max 60 requests per minute per IP
RATE_LIMIT_WINDOW = 60  # seconds
RATE_LIMIT_MAX = 60     # requests per window
_rate_store = collections.defaultdict(list)


def _is_rate_limited(ip):
    """Check if an IP has exceeded the rate limit."""
    now = time.time()
    # Clean old entries
    _rate_store[ip] = [t for t in _rate_store[ip] if now - t < RATE_LIMIT_WINDOW]
    if len(_rate_store[ip]) >= RATE_LIMIT_MAX:
        return True
    _rate_store[ip].append(now)
    return False


# ── Input Validation ──────────────────────────────────────────────────────────
# Whitelist of allowed characters for query params
_SAFE_PARAM_RE = re.compile(r'^[a-zA-Z0-9 \-_.,\'\"()&]+$')
MAX_PARAM_LENGTH = 200
MAX_SEARCH_LENGTH = 100

# Allowed release ID format: hex string, max 16 chars
_RELEASE_ID_RE = re.compile(r'^[a-f0-9]{1,16}$')


def _sanitize_param(value, max_len=MAX_PARAM_LENGTH):
    """Sanitize a query parameter: strip, truncate, validate characters."""
    if not value:
        return None
    v = str(value).strip()[:max_len]
    if not _SAFE_PARAM_RE.match(v):
        return None
    return v


def _safe_float(value):
    """Safely convert to float, returning None on failure."""
    try:
        f = float(value)
        if f < 0 or f > 100000:
            return None
        return f
    except (ValueError, TypeError):
        return None


def _safe_int(value):
    """Safely convert to int, returning None on failure."""
    try:
        i = int(value)
        if i < 1900 or i > 2100:
            return None
        return i
    except (ValueError, TypeError):
        return None


# ── Security Headers ──────────────────────────────────────────────────────────
SECURITY_HEADERS = {
    'X-Content-Type-Options': 'nosniff',
    'X-Frame-Options': 'DENY',
    'X-XSS-Protection': '1; mode=block',
    'Referrer-Policy': 'strict-origin-when-cross-origin',
    'Permissions-Policy': 'camera=(), microphone=(), geolocation=()',
    'Content-Security-Policy': (
        "default-src 'self'; "
        "script-src 'self'; "
        "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
        "font-src 'self' https://fonts.gstatic.com; "
        "img-src 'self' data:; "
        "connect-src 'self'; "
        "frame-ancestors 'none'"
    ),
    'Strict-Transport-Security': 'max-age=31536000; includeSubDomains',
}


class BourbonHandler(SimpleHTTPRequestHandler):
    """Custom handler that serves both API endpoints and static files."""

    # Suppress server version disclosure
    server_version = 'BourbonCalendar'
    sys_version = ''

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=STATIC_DIR, **kwargs)

    def _add_security_headers(self):
        """Add all security headers to the response."""
        for header, value in SECURITY_HEADERS.items():
            self.send_header(header, value)

    def do_GET(self):
        # Rate limiting
        client_ip = self.client_address[0]
        if _is_rate_limited(client_ip):
            self._json_response({'error': 'Rate limit exceeded. Try again later.'}, status=429)
            return

        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        # Flatten query params (take first value)
        params = {k: v[0] for k, v in query.items()}

        # API routes
        if path == '/api/releases':
            self._json_response(self._get_releases(params))
        elif path.startswith('/api/releases/'):
            release_id = path.split('/api/releases/')[-1]
            # Validate release ID format
            if not _RELEASE_ID_RE.match(release_id):
                self._json_response({'error': 'Invalid release ID format'}, status=400)
                return
            self._json_response(self._get_release(release_id))
        elif path == '/api/months':
            self._json_response(self._get_months())
        elif path == '/api/distilleries':
            self._json_response(self._get_distilleries())
        elif path == '/api/stats':
            self._json_response(self._get_stats())
        else:
            # ── Secure Static File Serving ──
            # Normalize path and prevent directory traversal
            if path == '/':
                self.path = '/index.html'
                self._serve_static()
                return

            # Resolve the real path and ensure it's inside STATIC_DIR
            requested = os.path.realpath(os.path.join(STATIC_DIR, path.lstrip('/')))
            if not requested.startswith(STATIC_DIR + os.sep) and requested != STATIC_DIR:
                # Path traversal attempt — serve index.html instead
                self.path = '/index.html'
                self._serve_static()
                return

            if os.path.isfile(requested):
                self._serve_static()
            else:
                # SPA fallback
                self.path = '/index.html'
                self._serve_static()

    # Block all other HTTP methods
    def do_POST(self):
        self._method_not_allowed()

    def do_PUT(self):
        self._method_not_allowed()

    def do_DELETE(self):
        self._method_not_allowed()

    def do_PATCH(self):
        self._method_not_allowed()

    def _method_not_allowed(self):
        self.send_response(405)
        self.send_header('Content-Type', 'application/json')
        self._add_security_headers()
        self.end_headers()
        self.wfile.write(json.dumps({'error': 'Method not allowed'}).encode('utf-8'))

    def _serve_static(self):
        """Serve static files with security headers."""
        super().do_GET()

    def end_headers(self):
        """Inject security headers into every response."""
        self._add_security_headers()
        super().end_headers()

    def _json_response(self, data, status=200):
        body = json.dumps(data, default=str).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', len(body))
        self.send_header('Cache-Control', 'no-store' if status != 200 else 'public, max-age=30')
        self.end_headers()
        self.wfile.write(body)

    def _get_releases(self, params):
        try:
            conn = get_db()
            filters = {}

            # Validate and sanitize each filter
            month = _sanitize_param(params.get('month'))
            if month:
                filters['month'] = month

            type_val = _sanitize_param(params.get('type'))
            if type_val:
                filters['type'] = type_val

            distillery = _sanitize_param(params.get('distillery'))
            if distillery:
                filters['distillery'] = distillery

            min_proof = _safe_float(params.get('minProof'))
            if min_proof is not None:
                filters['minProof'] = str(min_proof)

            max_proof = _safe_float(params.get('maxProof'))
            if max_proof is not None:
                filters['maxProof'] = str(max_proof)

            max_price = _safe_float(params.get('maxPrice'))
            if max_price is not None:
                filters['maxPrice'] = str(max_price)

            year = _safe_int(params.get('year'))
            if year is not None:
                filters['year'] = str(year)

            search = _sanitize_param(params.get('search'), MAX_SEARCH_LENGTH)
            if search:
                filters['search'] = search

            releases = get_all_releases(conn, filters)
            conn.close()

            # Parse sources string into list
            for r in releases:
                r['sources'] = r['sources'].split(',') if r.get('sources') else []

            return {'count': len(releases), 'releases': releases}
        except Exception:
            return {'error': 'Failed to load releases', 'count': 0, 'releases': []}

    def _get_release(self, release_id):
        try:
            conn = get_db()
            release = get_release_by_id(conn, release_id)
            conn.close()
            if not release:
                return {'error': 'Release not found'}
            return release
        except Exception:
            return {'error': 'Failed to load release'}

    def _get_months(self):
        try:
            conn = get_db()
            months = get_month_summary(conn)
            conn.close()
            return months
        except Exception:
            return {'error': 'Failed to load months'}

    def _get_distilleries(self):
        try:
            conn = get_db()
            distilleries = get_distilleries(conn)
            conn.close()
            return distilleries
        except Exception:
            return {'error': 'Failed to load distilleries'}

    def _get_stats(self):
        try:
            conn = get_db()
            stats = get_stats(conn)
            conn.close()
            return stats
        except Exception:
            return {'error': 'Failed to load stats'}

    # Suppress default access logging for API calls
    def log_message(self, format, *args):
        if '/api/' in str(args[0]) if args else False:
            return
        super().log_message(format, *args)


def main():
    server = HTTPServer(('', PORT), BourbonHandler)

    print()
    print('═══════════════════════════════════════════════════')
    print('  🥃 Bourbon Release Calendar API')
    print('═══════════════════════════════════════════════════')
    print(f'  Server:    http://localhost:{PORT}')
    print(f'  API:       http://localhost:{PORT}/api/releases')
    print(f'  Frontend:  http://localhost:{PORT}')
    print(f'  Security:  Headers ✓  Rate-limit ✓  Path-safe ✓')
    print('═══════════════════════════════════════════════════')
    print('  Press Ctrl+C to stop')
    print()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('\n  Server stopped.')
        server.server_close()


if __name__ == '__main__':
    main()
