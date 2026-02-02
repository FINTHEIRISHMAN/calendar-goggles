"""
Article-based Scrapers

Handles less-structured editorial/blog sources:
 - Blackwell's Wines: Rare Whiskey Releases of 2026
 - Alcohol Professor: Limited Whiskey Releases
 - Frootbat: Most Anticipated Bourbon Releases of 2026

These use prose paragraphs rather than structured calendars,
so we use regex-heavy text extraction.
"""
import re
import requests
from bs4 import BeautifulSoup

SOURCE_NAME = 'articles'

ARTICLE_SOURCES = [
    {
        'name': 'blackwells',
        'url': 'https://www.blackwellswines.com/blogs/news/rare-whiskey-releases-of-2026-what-collectors-should-watch-for',
        'label': "Blackwell's Wines",
    },
    {
        'name': 'alcohol-professor',
        'url': 'https://www.alcoholprofessor.com/blog-posts/limited-whiskey-releases-winter-2025-2026',
        'label': 'Alcohol Professor',
    },
    {
        'name': 'frootbat',
        'url': 'https://www.frootbat.com/blog/2575/most-anticipated-bourbon-releases-of-2026',
        'label': 'Frootbat',
    },
]

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

MONTH_NAMES = [
    'january', 'february', 'march', 'april', 'may', 'june',
    'july', 'august', 'september', 'october', 'november', 'december'
]


def scrape():
    """Scrape all article sources."""
    print(f"[{SOURCE_NAME}] Starting scrape of editorial sources...")
    all_releases = []

    for source in ARTICLE_SOURCES:
        try:
            releases = _scrape_article(source)
            print(f"[{SOURCE_NAME}/{source['name']}] Found {len(releases)} releases")
            all_releases.extend(releases)
        except Exception as e:
            print(f"[{SOURCE_NAME}/{source['name']}] Failed: {e}")

    print(f"[{SOURCE_NAME}] Total: {len(all_releases)} releases from {len(ARTICLE_SOURCES)} sources")
    return all_releases


def _scrape_article(source):
    """Scrape a single article source."""
    resp = requests.get(source['url'], headers=HEADERS, timeout=15)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, 'html.parser')
    releases = []

    content = soup.select_one('article, .article-content, .blog-post, .entry-content, .post-content, .rte, main')
    if not content:
        content = soup.body

    # Strategy 1: Product headings (h2, h3, h4) followed by description
    headings = content.select('h2, h3, h4')
    for heading in headings:
        heading_text = heading.get_text(strip=True)

        # Skip non-product headings
        if re.match(r'^(about|share|related|comment|conclusion|introduction|overview|what to|how to|tips|guide)', heading_text, re.I):
            continue

        next_p = heading.find_next_sibling(['p', 'ul', 'div'])
        desc = next_p.get_text(strip=True) if next_p else ''
        combined = f"{heading_text} {desc}"

        if re.search(r'bourbon|rye|whiskey|whisky|proof|abv|aged|year|distillery|barrel|cask', combined, re.I):
            parsed = _parse_article_entry(heading_text, desc, source)
            if parsed:
                releases.append(parsed)

    # Strategy 2: List items with product info
    if not releases:
        for li in content.select('li'):
            text = li.get_text(strip=True)
            if len(text) > 10 and re.search(r'bourbon|rye|whiskey|whisky|proof|abv|aged|year', text, re.I):
                parsed = _parse_article_entry(text.split(' – ')[0], text, source)
                if parsed:
                    releases.append(parsed)

    # Strategy 3: Bold product names in paragraphs
    if not releases:
        for p in content.select('p'):
            for bold in p.select('strong, b'):
                bold_text = bold.get_text(strip=True)
                parent_text = p.get_text(strip=True)
                if len(bold_text) > 5 and re.search(r'bourbon|rye|whiskey|proof|abv', parent_text, re.I):
                    parsed = _parse_article_entry(bold_text, parent_text, source)
                    if parsed:
                        releases.append(parsed)

    # Strategy 4: Full-text extraction
    if not releases:
        full_text = content.get_text('\n')
        releases = _extract_from_prose(full_text, source)

    return [{**r, '_source': f"articles/{source['name']}", '_source_url': source['url']} for r in releases]


def _parse_article_entry(heading, description, source):
    """Parse a product entry from article context."""
    combined = f"{heading} {description}"

    proof_m = re.search(r'([\d.]+)\s*(?:proof)', combined, re.I)
    abv_m = re.search(r'([\d.]+)\s*%\s*(?:abv)?', combined, re.I)
    proof = proof_m.group(0) if proof_m else (abv_m.group(0) if abv_m else None)

    price_m = re.search(r'\$\s*([\d,.]+)', combined)
    msrp = price_m.group(0) if price_m else None

    age_m = re.search(r'(\d+)\s*[-–]?\s*(?:years?|yr|yrs|year-old)', combined, re.I)
    age = age_m.group(0) if age_m else None

    finish_m = re.search(r'(?:finished?\s+in|aged\s+in)\s+([^,.]+)', combined, re.I)
    finish = finish_m.group(1).strip() if finish_m else None

    mashbill_m = re.search(r'mash\s*bill[:\s]+([^.]+)', combined, re.I)
    mashbill = mashbill_m.group(1).strip() if mashbill_m else None

    # Clean product name
    name = re.sub(r'\(.*?\)', '', heading)
    name = re.sub(r'\$[\d,.]+', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    if len(name) < 3:
        return None

    # Extract type
    type_val = 'bourbon'
    if re.search(r'rye', combined, re.I):
        type_val = 'rye'
    elif re.search(r'tennessee', combined, re.I):
        type_val = 'tennessee'
    elif re.search(r'wheat', combined, re.I):
        type_val = 'wheat'
    elif re.search(r'single malt', combined, re.I):
        type_val = 'single_malt'
    elif re.search(r'scotch', combined, re.I):
        type_val = 'scotch'

    # Extract release month
    month = None
    month_m = re.search(r'(january|february|march|april|may|june|july|august|september|october|november|december)\s*(?:2026)?', combined, re.I)
    if month_m:
        mn = month_m.group(1).capitalize()
        month = f"{mn} 2026"

    # Truncate description for notes
    notes = description[:200].strip() if description and len(description) > 10 else None

    return {
        'product_name': name,
        'proof': proof,
        'age': age,
        'msrp': msrp,
        'type': type_val,
        'release_month': month,
        'finish': finish,
        'mashbill': mashbill,
        'notes': notes,
        'is_new': False,
        'is_limited': bool(re.search(r'limited|rare|single barrel|cask strength|special|collector', combined, re.I)),
        'image_url': None,
        'source_url': source['url'],
    }


def _extract_from_prose(text, source):
    """Extract releases from unstructured prose."""
    releases = []
    lines = [l.strip() for l in text.split('\n') if l.strip() and len(l.strip()) > 15]

    for line in lines:
        has_markers = bool(re.search(r'\d+\s*proof|\d+\s*%\s*abv|\d+\s*year', line, re.I))
        has_names = bool(re.search(r'bourbon|rye|whiskey|whisky|barrel|reserve|edition|batch|collection', line, re.I))

        if has_markers and has_names and len(line) < 500:
            name_part = re.split(r'\d+\s*(?:proof|%)', line, flags=re.I)[0].strip()
            if 5 < len(name_part) < 120:
                parsed = _parse_article_entry(name_part, line, source)
                if parsed:
                    releases.append(parsed)

    return releases
