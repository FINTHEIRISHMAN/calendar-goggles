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
STATIC_DIR = os.path.join(os.path.dirname(__file__), 'static')


class BourbonHandler(SimpleHTTPRequestHandler):
    """Custom handler that serves both API endpoints and static files."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=STATIC_DIR, **kwargs)

    def do_GET(self):
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
            self._json_response(self._get_release(release_id))
        elif path == '/api/months':
            self._json_response(self._get_months())
        elif path == '/api/distilleries':
            self._json_response(self._get_distilleries())
        elif path == '/api/stats':
            self._json_response(self._get_stats())
        else:
            # Serve static files — fallback to index.html for SPA routing
            if path == '/':
                self.path = '/index.html'
            # Check if file exists
            file_path = os.path.join(STATIC_DIR, self.path.lstrip('/'))
            if os.path.isfile(file_path):
                super().do_GET()
            else:
                # SPA fallback
                self.path = '/index.html'
                super().do_GET()

    def _json_response(self, data, status=200):
        body = json.dumps(data, default=str).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', len(body))
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(body)

    def _get_releases(self, params):
        try:
            conn = get_db()
            filters = {
                'month': params.get('month'),
                'type': params.get('type'),
                'distillery': params.get('distillery'),
                'minProof': params.get('minProof'),
                'maxProof': params.get('maxProof'),
                'maxPrice': params.get('maxPrice'),
                'year': params.get('year'),
                'search': params.get('search'),
            }
            # Strip None values
            filters = {k: v for k, v in filters.items() if v}

            releases = get_all_releases(conn, filters)
            conn.close()

            # Parse sources string into list
            for r in releases:
                r['sources'] = r['sources'].split(',') if r.get('sources') else []

            return {'count': len(releases), 'releases': releases}
        except Exception as e:
            return {'error': str(e), 'count': 0, 'releases': []}

    def _get_release(self, release_id):
        try:
            conn = get_db()
            release = get_release_by_id(conn, release_id)
            conn.close()
            if not release:
                return {'error': 'Release not found'}
            return release
        except Exception as e:
            return {'error': str(e)}

    def _get_months(self):
        try:
            conn = get_db()
            months = get_month_summary(conn)
            conn.close()
            return months
        except Exception as e:
            return {'error': str(e)}

    def _get_distilleries(self):
        try:
            conn = get_db()
            distilleries = get_distilleries(conn)
            conn.close()
            return distilleries
        except Exception as e:
            return {'error': str(e)}

    def _get_stats(self):
        try:
            conn = get_db()
            stats = get_stats(conn)
            conn.close()
            return stats
        except Exception as e:
            return {'error': str(e)}

    # Suppress default logging
    def log_message(self, format, *args):
        if '/api/' in str(args[0]) if args else False:
            return  # Suppress API noise
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
