"""
Normalization & Deduplication Engine

Cleans, standardizes, and deduplicates bourbon release data
from multiple sources into a unified schema.
"""
import hashlib
import re

# ── ID Generation ─────────────────────────────────────────────────────────────

def generate_id(product_name, release_year=2026):
    normalized = re.sub(r'[^a-z0-9]', '', product_name.lower())
    return hashlib.sha256(f"{normalized}-{release_year}".encode()).hexdigest()[:16]


# ── Proof / ABV parsing ──────────────────────────────────────────────────────

def parse_proof(raw):
    if not raw:
        return None, None
    s = str(raw).strip()

    # "112.9 Proof"
    m = re.search(r'([\d.]+)\s*proof', s, re.I)
    if m:
        proof = float(m.group(1))
        return proof, round(proof / 2, 1)

    # "63% ABV" or "63%"
    m = re.search(r'([\d.]+)\s*%?\s*(?:abv)?', s, re.I)
    if m:
        val = float(m.group(1))
        if val > 100:
            return val, round(val / 2, 1)
        return round(val * 2, 1), val

    # Just a number
    m = re.search(r'([\d.]+)', s)
    if m:
        val = float(m.group(1))
        if val > 100:
            return val, round(val / 2, 1)
        return round(val * 2, 1), val

    return None, None


# ── Age parsing ───────────────────────────────────────────────────────────────

def parse_age(raw):
    if not raw:
        return None
    s = str(raw).strip()

    # Range: "aged 7-20 years" → take the max
    m = re.search(r'(\d+)\s*[-–]\s*(\d+)\s*(?:years?|yr|yrs)', s, re.I)
    if m:
        return int(m.group(2))

    # Simple: "12 years", "12-year", "12yr"
    m = re.search(r'(\d+)\s*[-–]?\s*(?:years?|yr|yrs|year-old|yo)', s, re.I)
    if m:
        return int(m.group(1))

    # Just a number that could be age
    m = re.match(r'^(\d+)$', s)
    if m:
        val = int(m.group(1))
        if 2 <= val <= 50:
            return val

    return None


# ── Price parsing ─────────────────────────────────────────────────────────────

def parse_price(raw):
    if not raw:
        return None
    s = str(raw).strip()
    if re.search(r'tbd|tba|n/a|pending|unknown', s, re.I):
        return None
    m = re.search(r'\$?\s*([\d,.]+)', s.replace(',', ''))
    if m:
        return float(m.group(1))
    return None


# ── Type classification ───────────────────────────────────────────────────────

TYPE_MAP = {
    'bourbon': 'bourbon',
    'kentucky straight bourbon': 'bourbon',
    'straight bourbon': 'bourbon',
    'rye': 'rye',
    'straight rye': 'rye',
    'rye whiskey': 'rye',
    'wheat whiskey': 'wheat',
    'wheated bourbon': 'bourbon',
    'tennessee whiskey': 'tennessee',
    'tennessee': 'tennessee',
    'single malt': 'single_malt',
    'american single malt': 'single_malt',
    'scotch': 'scotch',
    'japanese whisky': 'japanese',
    'blended': 'blend',
    'blend': 'blend',
}


def classify_type(raw):
    if not raw:
        return 'bourbon'
    s = str(raw).lower().strip()
    for pattern, type_val in TYPE_MAP.items():
        if pattern in s:
            return type_val
    if 'rye' in s:
        return 'rye'
    if 'wheat' in s:
        return 'wheat'
    if any(w in s for w in ('scotch', 'highland', 'speyside', 'islay')):
        return 'scotch'
    return 'bourbon'


# ── Month normalization ───────────────────────────────────────────────────────

MONTHS = [
    'January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December'
]


def normalize_month(raw, year=2026):
    if not raw:
        return None
    s = str(raw).strip()

    # Already formatted: "January 2026"
    for m in MONTHS:
        if re.search(rf'{m}\s*{year}', s, re.I):
            return f"{m} {year}"

    # Just month name: "January", "Jan"
    for m in MONTHS:
        if s.lower().startswith(m.lower()[:3]):
            return f"{m} {year}"

    # Numeric: "01/2026"
    nm = re.search(r'(\d{1,2})\s*[/\-]\s*(\d{2,4})', s)
    if nm:
        idx = int(nm.group(1)) - 1
        if 0 <= idx < 12:
            return f"{MONTHS[idx]} {year}"

    # Q1, Q2, etc
    qm = re.search(r'q(\d)', s, re.I)
    if qm:
        q = int(qm.group(1))
        idx = (q - 1) * 3
        if 0 <= idx < 12:
            return f"{MONTHS[idx]} {year}"

    return None


# ── Distillery extraction ─────────────────────────────────────────────────────

KNOWN_DISTILLERIES = {
    'buffalo trace': 'Buffalo Trace',
    'wild turkey': 'Wild Turkey',
    'heaven hill': 'Heaven Hill',
    'jim beam': 'Jim Beam',
    'beam suntory': 'Jim Beam',
    "maker's mark": "Maker's Mark",
    'makers mark': "Maker's Mark",
    'woodford reserve': 'Woodford Reserve',
    'four roses': 'Four Roses',
    'knob creek': 'Jim Beam',
    "booker's": 'Jim Beam',
    'bookers': 'Jim Beam',
    "baker's": 'Jim Beam',
    'basil hayden': 'Jim Beam',
    'old forester': 'Brown-Forman',
    "jack daniel's": "Jack Daniel's",
    'jack daniel': "Jack Daniel's",
    'george dickel': 'George Dickel',
    'barrell': 'Barrell Craft Spirits',
    "angel's envy": "Angel's Envy",
    'angels envy': "Angel's Envy",
    "michter's": "Michter's",
    'michter': "Michter's",
    'e.h. taylor': 'Buffalo Trace',
    'eagle rare': 'Buffalo Trace',
    'george t. stagg': 'Buffalo Trace',
    'stagg': 'Buffalo Trace',
    "blanton's": 'Buffalo Trace',
    'blanton': 'Buffalo Trace',
    'weller': 'Buffalo Trace',
    'pappy van winkle': 'Buffalo Trace',
    'van winkle': 'Buffalo Trace',
    'sazerac': 'Buffalo Trace',
    'elijah craig': 'Heaven Hill',
    'evan williams': 'Heaven Hill',
    'larceny': 'Heaven Hill',
    'henry mckenna': 'Heaven Hill',
    "parker's heritage": 'Heaven Hill',
    'old fitzgerald': 'Heaven Hill',
    'rebel': 'Lux Row',
    'blood oath': 'Lux Row',
    'little book': 'Jim Beam',
    'redwood empire': 'Redwood Empire',
    "belle meade": "Nelson's Green Brier",
    'whistlepig': 'WhistlePig',
    'high west': 'High West',
    'bardstown bourbon': 'Bardstown Bourbon Company',
    'king of kentucky': 'Brown-Forman',
    'old elk': 'Old Elk',
    'rabbit hole': 'Rabbit Hole',
    'still austin': 'Still Austin',
    'kentucky owl': 'Kentucky Owl',
    'smoke wagon': 'Smoke Wagon',
    'new riff': 'New Riff',
    'wilderness trail': 'Wilderness Trail',
    'castle & key': 'Castle & Key',
    'starlight': 'Starlight',
    'colonel e.h. taylor': 'Buffalo Trace',
    'thomas h. handy': 'Buffalo Trace',
    'william larue weller': 'Buffalo Trace',
}


def extract_distillery(product_name):
    if not product_name:
        return None
    lower = product_name.lower()
    for pattern, distillery in KNOWN_DISTILLERIES.items():
        if pattern in lower:
            return distillery
    return None


# ── Full normalization pipeline ───────────────────────────────────────────────

def normalize_release(raw: dict):
    """Normalize a raw scraped release dict into a canonical schema."""
    product_name = (raw.get('product_name') or raw.get('name') or '').strip()
    if not product_name:
        return None

    proof, abv = parse_proof(raw.get('proof') or raw.get('abv'))
    age = parse_age(raw.get('age') or raw.get('age_years') or product_name)
    price = parse_price(raw.get('msrp') or raw.get('price'))
    month = normalize_month(raw.get('release_month') or raw.get('month'))
    type_val = classify_type(raw.get('type') or product_name)
    distillery = raw.get('distillery') or extract_distillery(product_name)

    return {
        'id': generate_id(product_name),
        'product_name': product_name,
        'distillery': distillery,
        'type': type_val,
        'proof': proof,
        'abv': abv,
        'age_years': age,
        'msrp': price,
        'bottle_size_ml': raw.get('bottle_size_ml') or 750,
        'release_month': month,
        'release_date': raw.get('release_date'),
        'release_year': raw.get('release_year', 2026),
        'batch': raw.get('batch'),
        'finish': raw.get('finish'),
        'mashbill': raw.get('mashbill'),
        'notes': raw.get('notes'),
        'is_limited': 1 if raw.get('is_limited') else 0,
        'is_new': 1 if raw.get('is_new') else 0,
        'image_url': raw.get('image_url'),
        'source_url': raw.get('source_url'),
    }


# ── Deduplication ─────────────────────────────────────────────────────────────

def deduplicate_releases(releases):
    """
    Deduplicate a list of normalized release dicts.
    Uses ID-based dedup first, then fuzzy name matching.
    """
    if not releases:
        return []

    # Phase 1: ID-based dedup (exact match on generated hash)
    seen_ids = {}
    for r in releases:
        rid = r['id']
        if rid in seen_ids:
            # Merge: fill in None fields from the new entry
            existing = seen_ids[rid]
            for key, val in r.items():
                if existing.get(key) is None and val is not None:
                    existing[key] = val
        else:
            seen_ids[rid] = dict(r)

    # Phase 2: Fuzzy name matching for near-duplicates
    deduped = list(seen_ids.values())

    try:
        from thefuzz import fuzz
        final = []
        skip_indices = set()

        for i, r1 in enumerate(deduped):
            if i in skip_indices:
                continue
            merged = dict(r1)
            for j in range(i + 1, len(deduped)):
                if j in skip_indices:
                    continue
                r2 = deduped[j]
                score = fuzz.token_sort_ratio(
                    r1['product_name'].lower(),
                    r2['product_name'].lower()
                )
                if score >= 85:
                    # Merge: prefer non-None values from r2
                    for key, val in r2.items():
                        if merged.get(key) is None and val is not None:
                            merged[key] = val
                    skip_indices.add(j)
            final.append(merged)
        return final

    except ImportError:
        # If thefuzz not available, just return ID-deduped results
        return deduped
