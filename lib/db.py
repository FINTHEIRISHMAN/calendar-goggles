"""
Database layer — SQLite
Handles schema creation, CRUD, and query helpers for bourbon releases.
"""
import sqlite3
import json
import os

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'bourbon.db')


def get_db():
    """Get a connection to the SQLite database, creating schema if needed."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    _init_schema(conn)
    return conn


def _init_schema(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS releases (
            id               TEXT PRIMARY KEY,
            product_name     TEXT NOT NULL,
            distillery       TEXT,
            type             TEXT DEFAULT 'bourbon',
            proof            REAL,
            abv              REAL,
            age_years        REAL,
            msrp             REAL,
            bottle_size_ml   INTEGER DEFAULT 750,
            release_month    TEXT,
            release_date     TEXT,
            release_year     INTEGER DEFAULT 2026,
            batch            TEXT,
            finish           TEXT,
            mashbill         TEXT,
            notes            TEXT,
            is_limited       INTEGER DEFAULT 0,
            is_new           INTEGER DEFAULT 0,
            image_url        TEXT,
            source_url       TEXT,
            created_at       TEXT DEFAULT (datetime('now')),
            updated_at       TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS release_sources (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            release_id   TEXT NOT NULL,
            source_name  TEXT NOT NULL,
            source_url   TEXT,
            scraped_at   TEXT DEFAULT (datetime('now')),
            raw_data     TEXT,
            FOREIGN KEY (release_id) REFERENCES releases(id) ON DELETE CASCADE,
            UNIQUE(release_id, source_name)
        );

        CREATE TABLE IF NOT EXISTS scrape_logs (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            source_name TEXT NOT NULL,
            status      TEXT NOT NULL,
            count       INTEGER DEFAULT 0,
            errors      TEXT,
            started_at  TEXT DEFAULT (datetime('now')),
            finished_at TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_releases_month ON releases(release_month);
        CREATE INDEX IF NOT EXISTS idx_releases_distillery ON releases(distillery);
        CREATE INDEX IF NOT EXISTS idx_releases_type ON releases(type);
        CREATE INDEX IF NOT EXISTS idx_releases_year ON releases(release_year);
        CREATE INDEX IF NOT EXISTS idx_release_sources_release ON release_sources(release_id);
    """)


# ── CRUD helpers ──────────────────────────────────────────────────────────────

def upsert_release(conn, release: dict):
    """Insert or update a release. Fields in `release` overwrite only if non-None."""
    conn.execute("""
        INSERT INTO releases (
            id, product_name, distillery, type, proof, abv, age_years, msrp,
            bottle_size_ml, release_month, release_date, release_year, batch,
            finish, mashbill, notes, is_limited, is_new, image_url, source_url
        ) VALUES (
            :id, :product_name, :distillery, :type, :proof, :abv, :age_years, :msrp,
            :bottle_size_ml, :release_month, :release_date, :release_year, :batch,
            :finish, :mashbill, :notes, :is_limited, :is_new, :image_url, :source_url
        )
        ON CONFLICT(id) DO UPDATE SET
            product_name   = COALESCE(:product_name, releases.product_name),
            distillery     = COALESCE(:distillery, releases.distillery),
            type           = COALESCE(:type, releases.type),
            proof          = COALESCE(:proof, releases.proof),
            abv            = COALESCE(:abv, releases.abv),
            age_years      = COALESCE(:age_years, releases.age_years),
            msrp           = COALESCE(:msrp, releases.msrp),
            bottle_size_ml = COALESCE(:bottle_size_ml, releases.bottle_size_ml),
            release_month  = COALESCE(:release_month, releases.release_month),
            release_date   = COALESCE(:release_date, releases.release_date),
            batch          = COALESCE(:batch, releases.batch),
            finish         = COALESCE(:finish, releases.finish),
            mashbill       = COALESCE(:mashbill, releases.mashbill),
            notes          = COALESCE(:notes, releases.notes),
            is_limited     = COALESCE(:is_limited, releases.is_limited),
            is_new         = COALESCE(:is_new, releases.is_new),
            image_url      = COALESCE(:image_url, releases.image_url),
            source_url     = COALESCE(:source_url, releases.source_url),
            updated_at     = datetime('now')
    """, release)


def add_source(conn, release_id, source_name, source_url, raw_data):
    conn.execute("""
        INSERT OR IGNORE INTO release_sources (release_id, source_name, source_url, raw_data)
        VALUES (?, ?, ?, ?)
    """, (release_id, source_name, source_url, json.dumps(raw_data, default=str)))


def log_scrape(conn, source_name, status, count=0, errors=None):
    conn.execute("""
        INSERT INTO scrape_logs (source_name, status, count, errors, finished_at)
        VALUES (?, ?, ?, ?, datetime('now'))
    """, (source_name, status, count, errors))


# ── Query helpers ─────────────────────────────────────────────────────────────

def get_all_releases(conn, filters=None):
    filters = filters or {}
    sql = """
        SELECT r.*, GROUP_CONCAT(DISTINCT rs.source_name) as sources
        FROM releases r
        LEFT JOIN release_sources rs ON r.id = rs.release_id
    """
    conditions = []
    params = []

    if filters.get('month'):
        conditions.append('r.release_month = ?')
        params.append(filters['month'])
    if filters.get('type'):
        conditions.append('r.type = ?')
        params.append(filters['type'])
    if filters.get('distillery'):
        conditions.append('r.distillery LIKE ?')
        params.append(f"%{filters['distillery']}%")
    if filters.get('minProof'):
        conditions.append('r.proof >= ?')
        params.append(float(filters['minProof']))
    if filters.get('maxProof'):
        conditions.append('r.proof <= ?')
        params.append(float(filters['maxProof']))
    if filters.get('maxPrice'):
        conditions.append('r.msrp <= ?')
        params.append(float(filters['maxPrice']))
    if filters.get('year'):
        conditions.append('r.release_year = ?')
        params.append(int(filters['year']))
    if filters.get('search'):
        conditions.append('(r.product_name LIKE ? OR r.distillery LIKE ? OR r.notes LIKE ?)')
        s = f"%{filters['search']}%"
        params.extend([s, s, s])

    if conditions:
        sql += ' WHERE ' + ' AND '.join(conditions)

    sql += ' GROUP BY r.id ORDER BY r.release_month ASC, r.product_name ASC'
    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def get_release_by_id(conn, release_id):
    row = conn.execute('SELECT * FROM releases WHERE id = ?', (release_id,)).fetchone()
    if not row:
        return None
    release = dict(row)
    sources = conn.execute(
        'SELECT * FROM release_sources WHERE release_id = ?', (release_id,)
    ).fetchall()
    release['source_details'] = [dict(s) for s in sources]
    return release


def get_month_summary(conn):
    return [dict(r) for r in conn.execute("""
        SELECT release_month, COUNT(*) as count
        FROM releases
        WHERE release_year = 2026
        GROUP BY release_month
        ORDER BY
          CASE release_month
            WHEN 'January 2026' THEN 1
            WHEN 'February 2026' THEN 2
            WHEN 'March 2026' THEN 3
            WHEN 'April 2026' THEN 4
            WHEN 'May 2026' THEN 5
            WHEN 'June 2026' THEN 6
            WHEN 'July 2026' THEN 7
            WHEN 'August 2026' THEN 8
            WHEN 'September 2026' THEN 9
            WHEN 'October 2026' THEN 10
            WHEN 'November 2026' THEN 11
            WHEN 'December 2026' THEN 12
            ELSE 13
          END
    """).fetchall()]


def get_distilleries(conn):
    return [dict(r) for r in conn.execute("""
        SELECT DISTINCT distillery, COUNT(*) as count
        FROM releases
        WHERE distillery IS NOT NULL
        GROUP BY distillery
        ORDER BY count DESC
    """).fetchall()]


def get_stats(conn):
    total = conn.execute('SELECT COUNT(*) as c FROM releases').fetchone()['c']
    sources = conn.execute('SELECT COUNT(DISTINCT source_name) as c FROM release_sources').fetchone()['c']
    avg_proof = conn.execute('SELECT AVG(proof) as avg FROM releases WHERE proof IS NOT NULL').fetchone()['avg']
    avg_price = conn.execute('SELECT AVG(msrp) as avg FROM releases WHERE msrp IS NOT NULL').fetchone()['avg']
    last = conn.execute('SELECT MAX(finished_at) as last FROM scrape_logs').fetchone()['last']
    return {
        'totalReleases': total,
        'totalSources': sources,
        'avgProof': avg_proof,
        'avgPrice': avg_price,
        'lastScrape': last,
    }
