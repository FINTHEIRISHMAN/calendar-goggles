"""
Bourbon Bossman Release Calendar Scraper

Source: https://bourbonbossman.com/2026-bourbon-release-calendar/

WordPress-based site with server-rendered HTML.
Organized by month with bullet-point entries.
"""
import re
import requests
from bs4 import BeautifulSoup

SOURCE_NAME = 'bourbon-bossman'
SOURCE_URL = 'https://bourbonbossman.com/2026-bourbon-release-calendar/'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

MONTH_NAMES = [
    'january', 'february', 'march', 'april', 'may', 'june',
    'july', 'august', 'september', 'october', 'november', 'december'
]


def scrape():
    """Scrape Bourbon Bossman release calendar."""
    print(f"[{SOURCE_NAME}] Starting scrape...")

    try:
        resp = requests.get(SOURCE_URL, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"[{SOURCE_NAME}] Fetch failed: {e}")
        return []

    soup = BeautifulSoup(resp.text, 'html.parser')
    releases = []

    # Find the main WordPress content area
    content = soup.select_one('.entry-content, .post-content, .article-content, article, .content-area')
    if not content:
        print(f"[{SOURCE_NAME}] Could not find content area")
        return []

    current_month = None

    # Walk through all children looking for month headings and release entries
    for el in content.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'ul', 'ol', 'li', 'strong', 'b', 'div']):
        tag = el.name
        text = el.get_text(strip=True)

        # Detect month headings
        if tag in ('h1', 'h2', 'h3', 'h4', 'h5', 'h6'):
            for mn in MONTH_NAMES:
                if mn in text.lower():
                    current_month = f"{mn.capitalize()} 2026"
                    break

        # Bold text in paragraphs can also be month headers
        if tag == 'p':
            bold = el.find(['strong', 'b'])
            if bold:
                bold_text = bold.get_text(strip=True)
                for mn in MONTH_NAMES:
                    if mn in bold_text.lower():
                        current_month = f"{mn.capitalize()} 2026"
                        break

        # Parse list items
        if tag == 'li' and len(text) > 5:
            parsed = _parse_entry(text, current_month)
            if parsed:
                releases.append(parsed)

        # Parse paragraphs that contain release info (not headings)
        if tag == 'p' and current_month and not el.find(['strong', 'b']):
            lines = [l.strip() for l in text.split('\n') if l.strip() and len(l.strip()) > 5]
            for line in lines:
                if re.search(r'proof|abv|year|aged|\$|bourbon|rye|whiskey', line, re.I):
                    parsed = _parse_entry(line, current_month)
                    if parsed:
                        releases.append(parsed)

    # Fallback: parse all list items if structured approach failed
    if not releases:
        fallback_month = None
        for el in content.find_all(True):
            text = el.get_text(strip=True)

            for mn in MONTH_NAMES:
                if text.lower().startswith(mn) and len(text) < 30:
                    fallback_month = f"{mn.capitalize()} 2026"

            if el.name == 'li':
                parsed = _parse_entry(text, fallback_month)
                if parsed:
                    releases.append(parsed)

    print(f"[{SOURCE_NAME}] Found {len(releases)} releases")
    return [{**r, '_source': SOURCE_NAME, '_source_url': SOURCE_URL} for r in releases]


def _parse_entry(text, month=None):
    """Parse a single text entry into a release dict."""
    if not text or len(text) < 5:
        return None

    # Extract proof
    proof_m = re.search(r'([\d.]+)\s*(?:proof)', text, re.I)
    abv_m = re.search(r'([\d.]+)\s*%\s*(?:abv)?', text, re.I)
    proof = proof_m.group(0) if proof_m else (abv_m.group(0) if abv_m else None)

    # Extract price
    price_m = re.search(r'\$\s*([\d,.]+)', text)
    msrp = price_m.group(0) if price_m else None

    # Extract age
    age_m = re.search(r'(?:aged?\s*)?(\d+)[-–]?(?:\d+)?\s*(?:years?|yr)', text, re.I)
    age = age_m.group(0) if age_m else None

    # Extract bottle size
    size_m = re.search(r'(\d+)\s*ml', text, re.I)
    bottle_size = int(size_m.group(1)) if size_m else 750

    # Classify type
    type_val = None
    if re.search(r'straight rye|rye whiskey', text, re.I):
        type_val = 'rye'
    elif re.search(r'tennessee', text, re.I):
        type_val = 'tennessee'
    elif re.search(r'wheat', text, re.I):
        type_val = 'wheat'
    elif re.search(r'single malt', text, re.I):
        type_val = 'single_malt'
    elif re.search(r'bourbon', text, re.I):
        type_val = 'bourbon'

    # Extract finish
    finish_m = re.search(r'(?:finished?\s+in|aged\s+in|cask)\s+([^,)]+)', text, re.I)
    finish = finish_m.group(1).strip() if finish_m else None

    # Clean product name
    name = re.split(r'\s*[-–—]\s*(?:Kentucky|Straight|Tennessee|\$|MSRP|SRP|\d+\s*proof)', text, flags=re.I)[0]
    name = re.sub(r'\(.*?\)', '', name)
    name = re.sub(r'\$[\d,.]+', '', name)
    name = re.sub(r'\s+', ' ', name).strip()

    if len(name) < 3:
        name = text[:80].strip()

    # Skip non-product lines
    if re.match(r'^(note|update|source|click|link|image|photo|tbd|tba)', name, re.I):
        return None

    return {
        'product_name': name,
        'proof': proof,
        'age': age,
        'msrp': msrp,
        'type': type_val,
        'release_month': month,
        'bottle_size_ml': bottle_size,
        'finish': finish,
        'notes': None,
        'is_new': False,
        'is_limited': bool(re.search(r'limited|single barrel|cask strength|special|rare', text, re.I)),
        'image_url': None,
        'source_url': SOURCE_URL,
    }
