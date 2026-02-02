"""
Soaking Oak Bourbon Release Calendar Scraper

Source: https://blog.soakingoak.com/bourbon-release-calendar/

Blog-style WordPress site with monthly bullet-point entries.
"""
import re
import requests
from bs4 import BeautifulSoup

SOURCE_NAME = 'soaking-oak'
SOURCE_URL = 'https://blog.soakingoak.com/bourbon-release-calendar/'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

MONTH_NAMES = [
    'january', 'february', 'march', 'april', 'may', 'june',
    'july', 'august', 'september', 'october', 'november', 'december'
]


def scrape():
    """Scrape Soaking Oak release calendar."""
    print(f"[{SOURCE_NAME}] Starting scrape...")

    try:
        resp = requests.get(SOURCE_URL, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"[{SOURCE_NAME}] Fetch failed: {e}")
        return []

    soup = BeautifulSoup(resp.text, 'lxml')
    releases = []

    content = soup.select_one('article, .entry-content, .post-content, .blog-content, main')
    if not content:
        print(f"[{SOURCE_NAME}] Could not find content area")
        return []

    current_month = None

    for el in content.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'ul', 'ol', 'li', 'strong', 'b', 'div']):
        tag = el.name
        text = el.get_text(strip=True)

        # Detect month headings
        if tag in ('h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'strong', 'b'):
            for mn in MONTH_NAMES:
                if mn in text.lower():
                    current_month = f"{mn.capitalize()} 2026"
                    break

        # Parse list items
        if tag == 'li' and len(text) > 5:
            parsed = _parse_entry(text, current_month)
            if parsed:
                releases.append(parsed)

    # Fallback: parse all text content line by line
    if not releases:
        full_text = content.get_text('\n')
        lines = [l.strip() for l in full_text.split('\n') if l.strip() and len(l.strip()) > 10]

        fb_month = None
        for line in lines:
            for mn in MONTH_NAMES:
                if line.lower().startswith(mn):
                    fb_month = f"{mn.capitalize()} 2026"
                    break

            if re.search(r'proof|abv|aged|year|\$|bourbon|rye|whiskey|whisky', line, re.I) and len(line) < 300:
                parsed = _parse_entry(line, fb_month)
                if parsed:
                    releases.append(parsed)

    print(f"[{SOURCE_NAME}] Found {len(releases)} releases")
    return [{**r, '_source': SOURCE_NAME, '_source_url': SOURCE_URL} for r in releases]


def _parse_entry(text, month=None):
    """Parse a single text entry into a release dict."""
    if not text or len(text) < 5:
        return None

    proof_m = re.search(r'([\d.]+)\s*(?:proof)', text, re.I)
    abv_m = re.search(r'([\d.]+)\s*%\s*(?:abv)?', text, re.I)
    proof = proof_m.group(0) if proof_m else (abv_m.group(0) if abv_m else None)

    price_m = re.search(r'\$\s*([\d,.]+)', text)
    msrp = price_m.group(0) if price_m else None

    age_m = re.search(r'(\d+)\s*[-–]?\s*(?:years?|yr|yrs|year-old|yo)', text, re.I)
    age = age_m.group(0) if age_m else None

    type_val = 'bourbon'
    if re.search(r'rye', text, re.I):
        type_val = 'rye'
    elif re.search(r'tennessee', text, re.I):
        type_val = 'tennessee'
    elif re.search(r'wheat', text, re.I):
        type_val = 'wheat'

    name = re.sub(r'\(.*?\)', ' ', text)
    name = re.sub(r'\$[\d,.]+', '', name)
    name = re.sub(r'[\d.]+\s*proof', '', name, flags=re.I)
    name = re.sub(r'[\d.]+%\s*abv', '', name, flags=re.I)
    name = re.sub(r'\s*[-–—]\s*$', '', name)
    name = re.sub(r'\s+', ' ', name).strip()

    if len(name) > 100:
        parts = name.split(' – ')
        name = parts[0].strip()

    if len(name) < 3 or re.match(r'^(note|update|source|click)', name, re.I):
        return None

    return {
        'product_name': name,
        'proof': proof,
        'age': age,
        'msrp': msrp,
        'type': type_val,
        'release_month': month,
        'finish': None,
        'notes': None,
        'is_new': False,
        'is_limited': bool(re.search(r'limited|single barrel|cask strength|special|rare', text, re.I)),
        'image_url': None,
        'source_url': SOURCE_URL,
    }
