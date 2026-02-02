#!/usr/bin/env python3
"""
Master Scrape Orchestrator

Runs all scrapers (or a specific one), normalizes results,
deduplicates, and stores in the database.

Usage:
    python3 scrape.py                           # Run all scrapers
    python3 scrape.py --source breaking-bourbon # Run one scraper
    python3 scrape.py --dry-run                 # Preview without saving
    python3 scrape.py --verbose                 # Show each release
"""
import sys
import os
import argparse
import time

# Add project root to path
sys.path.insert(0, os.path.dirname(__file__))

from lib.normalize import normalize_release, deduplicate_releases
from lib.db import get_db, upsert_release, add_source, log_scrape
from scrapers import breaking_bourbon, bourbon_bossman, soaking_oak, articles

SCRAPERS = {
    'breaking-bourbon': breaking_bourbon,
    'bourbon-bossman': bourbon_bossman,
    'soaking-oak': soaking_oak,
    'articles': articles,
}


def main():
    parser = argparse.ArgumentParser(description='Bourbon Release Calendar Scraper')
    parser.add_argument('--source', choices=list(SCRAPERS.keys()), help='Scrape a specific source')
    parser.add_argument('--dry-run', action='store_true', help='Preview without saving to DB')
    parser.add_argument('--verbose', '-v', action='store_true', help='Show each release')
    args = parser.parse_args()

    print('═══════════════════════════════════════════════════')
    print('  🥃 Bourbon Release Calendar — Scrape Pipeline')
    print('═══════════════════════════════════════════════════')
    print(f'  Mode: {"DRY RUN (no DB writes)" if args.dry_run else "LIVE"}')
    print(f'  Sources: {args.source or "ALL"}')
    print()

    # Determine which scrapers to run
    scrapers_to_run = {args.source: SCRAPERS[args.source]} if args.source else SCRAPERS

    all_raw_releases = []
    results = {}
    conn = get_db() if not args.dry_run else None

    # Run each scraper
    for name, scraper in scrapers_to_run.items():
        print(f'\n┌─ Scraping: {name}')
        print('│')

        try:
            start = time.time()
            raw_releases = scraper.scrape()
            elapsed = f"{time.time() - start:.1f}"

            print(f'│  Found {len(raw_releases)} raw entries ({elapsed}s)')
            results[name] = {'raw': len(raw_releases), 'status': 'success', 'elapsed': elapsed}

            # Normalize each release
            normalized = []
            for r in raw_releases:
                norm = normalize_release(r)
                if norm:
                    normalized.append({
                        'normalized': norm,
                        'raw': r,
                        'source_name': r.get('_source', name),
                        'source_url': r.get('_source_url', ''),
                    })
                    if args.verbose:
                        print(f'│  ✓ {norm["product_name"]} ({norm["proof"] or "?"} proof)')

            print(f'│  Normalized: {len(normalized)} valid releases')
            all_raw_releases.extend(normalized)

            if conn:
                log_scrape(conn, name, 'success', len(normalized))
                conn.commit()

        except Exception as e:
            print(f'│  ✗ ERROR: {e}')
            results[name] = {'raw': 0, 'status': 'error', 'error': str(e)}
            if conn:
                log_scrape(conn, name, 'error', 0, str(e))
                conn.commit()

        print('│')
        print('└─────────────────────────────────')

    # Deduplicate
    print('\n═══════════════════════════════════════════════════')
    print('  Deduplication')
    print('═══════════════════════════════════════════════════')

    normalized_list = [r['normalized'] for r in all_raw_releases]
    deduplicated = deduplicate_releases(normalized_list)

    print(f'  Raw total:    {len(all_raw_releases)}')
    print(f'  Deduplicated: {len(deduplicated)}')
    print(f'  Removed:      {len(all_raw_releases) - len(deduplicated)} duplicates')

    # Store in database
    if not args.dry_run and conn:
        print('\n═══════════════════════════════════════════════════')
        print('  Saving to Database')
        print('═══════════════════════════════════════════════════')

        for release in deduplicated:
            upsert_release(conn, release)

            # Find all raw sources for this release
            for src in all_raw_releases:
                if src['normalized']['id'] == release['id']:
                    add_source(conn, release['id'], src['source_name'], src['source_url'], src['raw'])

        conn.commit()
        print(f'  ✓ Saved {len(deduplicated)} releases to database')
    else:
        print('\n[DRY RUN] Skipping database writes.')
        print('\nPreview of deduplicated releases:')
        for r in deduplicated[:20]:
            price_str = f"${r['msrp']}" if r['msrp'] else 'TBD'
            print(f"  • {r['product_name']} | {r['proof'] or '?'} proof | {price_str} | {r['release_month'] or 'TBD'}")
        if len(deduplicated) > 20:
            print(f'  ... and {len(deduplicated) - 20} more')

    # Summary
    print('\n═══════════════════════════════════════════════════')
    print('  Summary')
    print('═══════════════════════════════════════════════════')
    for name, result in results.items():
        icon = '✓' if result['status'] == 'success' else '✗'
        print(f"  {icon} {name}: {result['raw']} entries ({result.get('elapsed', '0')}s) [{result['status']}]")
    print(f'\n  Total unique releases: {len(deduplicated)}')
    print('═══════════════════════════════════════════════════\n')

    if conn:
        conn.close()


if __name__ == '__main__':
    main()
