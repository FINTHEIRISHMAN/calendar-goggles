"""
Breaking Bourbon Release Calendar Scraper

Source: https://www.breakingbourbon.com/release-calendar

This site uses JavaScript rendering, so we attempt a static fetch
with Cheerio-like parsing, looking for embedded JSON data or
structured HTML that renders server-side.
"""
import re
import json
import requests
from bs4 import BeautifulSoup

SOURCE_NAME = 'breaking-bourbon'
SOURCE_URL = 'https://www.breakingbourbon.com/release-calendar'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

MONTH_NAMES = [
    'january', 'february', 'march', 'april', 'may', 'june',
    'july', 'august', 'september', 'october', 'november', 'december'
]


def scrape():
    """Scrape Breaking Bourbon release calendar."""
    print(f"[{SOURCE_NAME}] Starting scrape...")

    # Try to find embedded JSON data first (Squarespace/React sites often embed it)
    releases = _try_json_extract()
    if releases:
        print(f"[{SOURCE_NAME}] JSON extract found {len(releases)} releases")
        return releases

    # Fall back to HTML parsing
    releases = _parse_html()
    print(f"[{SOURCE_NAME}] HTML parse found {len(releases)} releases")
    return releases


def _try_json_extract():
    """Look for JSON data embedded in the page (script tags, __NEXT_DATA__, etc.)."""
    try:
        resp = requests.get(SOURCE_URL, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        html = resp.text

        # Look for JSON in script tags
        for pattern in [
            r'window\.__INITIAL_STATE__\s*=\s*({.*?});',
            r'window\.__NEXT_DATA__\s*=\s*({.*?});',
            r'<script[^>]*type="application/json"[^>]*>(.*?)</script>',
            r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>',
        ]:
            match = re.search(pattern, html, re.DOTALL)
            if match:
                try:
                    data = json.loads(match.group(1))
                    return _extract_from_json(data)
                except (json.JSONDecodeError, KeyError):
                    continue

        # Squarespace collection JSON endpoint
        json_url = SOURCE_URL.rstrip('/') + '?format=json'
        try:
            resp = requests.get(json_url, headers=HEADERS, timeout=10)
            if resp.ok and resp.headers.get('content-type', '').startswith('application/json'):
                data = resp.json()
                return _extract_from_json(data)
        except Exception:
            pass

    except Exception as e:
        print(f"[{SOURCE_NAME}] JSON extract failed: {e}")

    return []


def _extract_from_json(data):
    """Try to extract releases from various JSON structures."""
    releases = []

    # Recursively search for arrays of product-like objects
    def search(obj, depth=0):
        if depth > 10:
            return
        if isinstance(obj, list):
            for item in obj:
                if isinstance(item, dict) and _looks_like_release(item):
                    releases.append(_parse_json_item(item))
                search(item, depth + 1)
        elif isinstance(obj, dict):
            if _looks_like_release(obj):
                releases.append(_parse_json_item(obj))
            for val in obj.values():
                search(val, depth + 1)

    search(data)
    return releases


def _looks_like_release(obj):
    """Check if a dict looks like a bourbon release."""
    keys = set(str(k).lower() for k in obj.keys())
    name_keys = {'name', 'title', 'product_name', 'productname'}
    return bool(keys & name_keys) and any(
        k in keys for k in ('proof', 'abv', 'age', 'msrp', 'price', 'type', 'category')
    )


def _parse_json_item(item):
    """Convert a JSON item to our raw release format."""
    name = item.get('name') or item.get('title') or item.get('product_name') or ''
    return {
        'product_name': name.strip(),
        'proof': item.get('proof') or item.get('abv'),
        'age': item.get('age') or item.get('age_years') or item.get('ageStatement'),
        'msrp': item.get('msrp') or item.get('price') or item.get('retailPrice'),
        'type': item.get('type') or item.get('category') or item.get('spirit_type'),
        'release_month': item.get('release_month') or item.get('releaseDate') or item.get('month'),
        'notes': item.get('notes') or item.get('description') or item.get('tasting_notes'),
        'finish': item.get('finish') or item.get('barrel_finish'),
        'mashbill': item.get('mashbill') or item.get('mash_bill'),
        'image_url': item.get('image') or item.get('imageUrl') or item.get('thumbnail'),
        'is_new': bool(item.get('is_new') or item.get('isNew')),
        'is_limited': bool(item.get('is_limited') or item.get('limited')),
        'source_url': SOURCE_URL,
        '_source': SOURCE_NAME,
        '_source_url': SOURCE_URL,
    }


def _parse_html():
    """Parse HTML page for release entries."""
    try:
        resp = requests.get(SOURCE_URL, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"[{SOURCE_NAME}] Fetch failed: {e}")
        return []

    soup = BeautifulSoup(resp.text, 'html.parser')
    releases = []

    # Strategy 1: Card-based layouts
    selectors = [
        '[class*="release"]', '[class*="product"]', '[class*="item"]',
        '.summary-item', '.collection-item', '.blog-item',
    ]
    for sel in selectors:
        cards = soup.select(sel)
        for card in cards:
            name_el = card.select_one('h2, h3, h4, [class*="title"]')
            if not name_el:
                continue
            name = name_el.get_text(strip=True)
            if len(name) < 5:
                continue
            releases.append(_parse_card(card, name))

    if releases:
        return _tag_releases(releases)

    # Strategy 2: Month-grouped sections
    current_month = None
    for el in soup.find_all(['h2', 'h3', 'h4', 'p', 'ul', 'ol', 'li', 'div']):
        text = el.get_text(strip=True)

        # Check for month heading
        for mn in MONTH_NAMES:
            if mn in text.lower() and len(text) < 40:
                month_name = mn.capitalize()
                current_month = f"{month_name} 2026"
                break

        # Parse list items
        if el.name == 'li' and len(text) > 10:
            parsed = _parse_text_entry(text, current_month)
            if parsed:
                releases.append(parsed)

    if releases:
        return _tag_releases(releases)

    # Strategy 3: Full text extraction
    body = soup.select_one('main, .content, article, .page-content, body')
    if body:
        text = body.get_text('\n')
        releases = _parse_unstructured(text)

    return _tag_releases(releases)


def _parse_card(card, name):
    """Extract release data from an HTML card element."""
    text = card.get_text(' ')
    proof_m = re.search(r'([\d.]+)\s*(?:proof|%\s*abv)', text, re.I)
    price_m = re.search(r'\$\s*([\d,.]+)', text)
    age_m = re.search(r'(\d+)\s*[-–]?\s*(?:years?|yr)', text, re.I)

    link = card.select_one('a')
    href = link.get('href', '') if link else ''
    if href and not href.startswith('http'):
        href = f"https://www.breakingbourbon.com{href}"

    return {
        'product_name': name,
        'proof': proof_m.group(0) if proof_m else None,
        'age': age_m.group(0) if age_m else None,
        'msrp': price_m.group(0) if price_m else None,
        'release_month': None,
        'type': None,
        'notes': None,
        'is_new': bool(card.select('[class*="new"], [class*="badge"]')),
        'image_url': _extract_image(card),
        'source_url': href or SOURCE_URL,
    }


def _parse_text_entry(text, month=None):
    """Parse a single text-based entry."""
    if not text or len(text) < 5:
        return None

    proof_m = re.search(r'([\d.]+)\s*(?:proof)', text, re.I)
    price_m = re.search(r'\$\s*([\d,.]+)', text)
    age_m = re.search(r'(\d+)\s*[-–]?\s*(?:years?|yr)', text, re.I)

    name = text
    name = re.sub(r'\(.*?\)', '', name)
    name = re.sub(r'\$[\d,.]+', '', name)
    name = re.sub(r'[\d.]+\s*proof', '', name, flags=re.I)
    name = re.sub(r'\s*[-–—]\s*$', '', name)
    name = re.sub(r'\s+', ' ', name).strip()

    if len(name) > 100:
        parts = name.split(' – ')
        name = parts[0].strip()

    if len(name) < 3 or re.match(r'^(note|update|source|click)', name, re.I):
        return None

    return {
        'product_name': name,
        'proof': proof_m.group(0) if proof_m else None,
        'age': age_m.group(0) if age_m else None,
        'msrp': price_m.group(0) if price_m else None,
        'release_month': month,
        'type': None,
        'notes': None,
        'is_new': False,
        'is_limited': bool(re.search(r'limited|single barrel|cask strength', text, re.I)),
        'image_url': None,
        'source_url': SOURCE_URL,
    }


def _parse_unstructured(text):
    """Parse unstructured text for release entries."""
    releases = []
    lines = [l.strip() for l in text.split('\n') if l.strip() and len(l.strip()) > 10]

    current_month = None
    for line in lines:
        for mn in MONTH_NAMES:
            if line.lower().startswith(mn) and len(line) < 30:
                current_month = f"{mn.capitalize()} 2026"
                break

        if re.search(r'proof|abv|year|aged|\$', line, re.I) and len(line) < 300:
            parsed = _parse_text_entry(line, current_month)
            if parsed:
                releases.append(parsed)

    return releases


def _extract_image(el):
    img = el.select_one('img')
    if img:
        return img.get('src') or img.get('data-src')
    return None


def _tag_releases(releases):
    return [
        {**r, '_source': SOURCE_NAME, '_source_url': SOURCE_URL}
        for r in releases
    ]
