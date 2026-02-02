#!/usr/bin/env python3
"""
Seed Script

Populates the database with sample bourbon release data
for testing the frontend without running actual scrapers.
"""
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from lib.normalize import normalize_release
from lib.db import get_db, upsert_release, add_source

SAMPLE_RELEASES = [
    {
        "product_name": "Maker's Mark Cellar Aged 2026 Release",
        "distillery": "Maker's Mark",
        "type": "bourbon",
        "proof": "112.9 Proof",
        "age": "11 years",
        "msrp": "$200",
        "release_month": "January",
        "notes": "Kentucky Straight Bourbon Whisky, aged 11-14 years in limestone cellar",
        "is_limited": True,
        "source_url": "https://www.breakingbourbon.com/release-calendar",
        "_source": "breaking-bourbon",
    },
    {
        "product_name": "Knob Creek Blender's Edition No. 01",
        "distillery": "Jim Beam",
        "type": "bourbon",
        "proof": "109 Proof",
        "age": "10 years",
        "msrp": "$59.99",
        "release_month": "January",
        "notes": "Kentucky Straight Bourbon Whiskey",
        "source_url": "https://www.breakingbourbon.com/release-calendar",
        "_source": "breaking-bourbon",
    },
    {
        "product_name": "Old Forester Tribute Series 2026 Release",
        "distillery": "Brown-Forman",
        "type": "bourbon",
        "proof": "115 Proof",
        "age": "12 years",
        "release_month": "July",
        "notes": "375ml bottle, charred 3 times as long as the standard Old Forester barrel profile",
        "is_limited": True,
        "source_url": "https://www.breakingbourbon.com/release-calendar",
        "_source": "breaking-bourbon",
    },
    {
        "product_name": "James B. Beam Distillers' Share 230th Anniversary Bourbon",
        "distillery": "Jim Beam",
        "type": "bourbon",
        "proof": "115 Proof",
        "age": "7 years",
        "release_month": "January",
        "notes": "Aged 7-20 years, commemorating 230th anniversary",
        "is_limited": True,
        "source_url": "https://bourbonbossman.com/2026-bourbon-release-calendar/",
        "_source": "bourbon-bossman",
    },
    {
        "product_name": "King of Kentucky Small Batch",
        "distillery": "Brown-Forman",
        "type": "bourbon",
        "proof": "105 Proof",
        "age": "12 years",
        "release_month": "January",
        "notes": "Kentucky Straight Bourbon Whiskey",
        "is_limited": True,
        "source_url": "https://bourbonbossman.com/2026-bourbon-release-calendar/",
        "_source": "bourbon-bossman",
    },
    {
        "product_name": "Heaven Hill Heritage Collection 2026",
        "distillery": "Heaven Hill",
        "type": "bourbon",
        "proof": "137 Proof",
        "age": "22 years",
        "release_month": None,
        "notes": "Highest proof Heaven Hill bourbon ever released at 68.5% ABV",
        "is_limited": True,
        "source_url": "https://www.frootbat.com/blog/2575/most-anticipated-bourbon-releases-of-2026",
        "_source": "articles/frootbat",
    },
    {
        "product_name": "Maker's Mark Star Hill Farm 2026",
        "distillery": "Maker's Mark",
        "type": "bourbon",
        "proof": "114.7 Proof",
        "mashbill": "51% soft red winter wheat, 27% malted soft red winter wheat, 22% malted barley",
        "notes": "Unique wheat-forward mashbill",
        "is_limited": True,
        "source_url": "https://www.frootbat.com/blog/2575/most-anticipated-bourbon-releases-of-2026",
        "_source": "articles/frootbat",
    },
    {
        "product_name": "Jack Daniel's 14 Year Old Tennessee Whiskey Batch 2",
        "distillery": "Jack Daniel's",
        "type": "tennessee",
        "age": "14 years",
        "release_month": "February",
        "notes": "Second batch of the inaugural 14-year expression",
        "is_limited": True,
        "source_url": "https://www.frootbat.com/blog/2575/most-anticipated-bourbon-releases-of-2026",
        "_source": "articles/frootbat",
    },
    {
        "product_name": "Blood Oath Pact No. 12",
        "distillery": "Lux Row",
        "type": "bourbon",
        "proof": "98.6 Proof",
        "release_month": "April",
        "finish": "Italian wine casks (Montepulciano and Sangiovese)",
        "notes": "Blend finished in Italian wine casks",
        "is_limited": True,
        "source_url": "https://www.frootbat.com/blog/2575/most-anticipated-bourbon-releases-of-2026",
        "_source": "articles/frootbat",
    },
    {
        "product_name": "Little Book Chapter 10",
        "distillery": "Jim Beam",
        "type": "bourbon",
        "proof": "121.8 Proof",
        "release_month": "June",
        "finish": "Sherry and toasted bourbon casks",
        "notes": "Bourbon finished in sherry and toasted bourbon casks",
        "is_limited": True,
        "source_url": "https://www.frootbat.com/blog/2575/most-anticipated-bourbon-releases-of-2026",
        "_source": "articles/frootbat",
    },
    {
        "product_name": "Baker's 7 Year Old Single Barrel High Rye Bourbon",
        "distillery": "Jim Beam",
        "type": "bourbon",
        "age": "7 years",
        "notes": "Single barrel, high-rye mashbill limited release",
        "is_limited": True,
        "source_url": "https://www.blackwellswines.com/blogs/news/rare-whiskey-releases-of-2026-what-collectors-should-watch-for",
        "_source": "articles/blackwells",
    },
    {
        "product_name": "George Dickel 18 Year Old Bourbon Whisky",
        "distillery": "George Dickel",
        "type": "bourbon",
        "age": "18 years",
        "notes": "Rare aged expression from George Dickel",
        "is_limited": True,
        "source_url": "https://www.blackwellswines.com/blogs/news/rare-whiskey-releases-of-2026-what-collectors-should-watch-for",
        "_source": "articles/blackwells",
    },
    {
        "product_name": "Maker's Mark 101 Proof Limited Release",
        "distillery": "Maker's Mark",
        "type": "bourbon",
        "proof": "101 Proof",
        "notes": "Limited higher-proof expression",
        "is_limited": True,
        "source_url": "https://www.blackwellswines.com/blogs/news/rare-whiskey-releases-of-2026-what-collectors-should-watch-for",
        "_source": "articles/blackwells",
    },
    {
        "product_name": "Rebel Cask Strength Single Barrel Bourbon",
        "distillery": "Lux Row",
        "type": "bourbon",
        "proof": "126 Proof",
        "notes": "63% ABV cask strength single barrel",
        "is_limited": True,
        "source_url": "https://www.blackwellswines.com/blogs/news/rare-whiskey-releases-of-2026-what-collectors-should-watch-for",
        "_source": "articles/blackwells",
    },
    {
        "product_name": "Angel's Envy Bottled-in-Bond Cask Strength Bourbon",
        "distillery": "Angel's Envy",
        "type": "bourbon",
        "notes": "First cask-strength, un-finished release under the Bottled-in-Bond act",
        "is_limited": True,
        "source_url": "https://www.blackwellswines.com/blogs/news/rare-whiskey-releases-of-2026-what-collectors-should-watch-for",
        "_source": "articles/blackwells",
    },
    {
        "product_name": "Angel's Envy Distiller's Collection 10 Cask Strength Straight Rye",
        "distillery": "Angel's Envy",
        "type": "rye",
        "proof": "112 Proof",
        "age": "10 years",
        "release_month": "January",
        "finish": "Caribbean Rum Casks",
        "notes": "Finished in Caribbean Rum Casks",
        "is_limited": True,
        "source_url": "https://www.breakingbourbon.com/release-calendar",
        "_source": "breaking-bourbon",
    },
    {
        "product_name": "Redwood Empire Thunderbolt Bourbon Whiskey",
        "distillery": "Redwood Empire",
        "type": "bourbon",
        "proof": "94 Proof",
        "release_month": "January",
        "notes": "New release from Redwood Empire",
        "source_url": "https://www.breakingbourbon.com/release-calendar",
        "_source": "breaking-bourbon",
    },
    {
        "product_name": "Colonel E.H. Taylor Bottled in Bond Bourbon 15 Year",
        "distillery": "Buffalo Trace",
        "type": "bourbon",
        "proof": "100 Proof",
        "age": "15 years",
        "release_month": "September",
        "notes": "Part of the Buffalo Trace Antique Collection (BTAC) 2026",
        "is_limited": True,
        "source_url": "https://www.breakingbourbon.com/release-calendar",
        "_source": "breaking-bourbon",
    },
    {
        "product_name": "Eagle Rare 17 Year Old Bourbon",
        "distillery": "Buffalo Trace",
        "type": "bourbon",
        "proof": "101 Proof",
        "age": "17 years",
        "release_month": "September",
        "notes": "BTAC 2026 release",
        "is_limited": True,
        "source_url": "https://www.breakingbourbon.com/release-calendar",
        "_source": "breaking-bourbon",
    },
    {
        "product_name": "George T. Stagg Bourbon 2026",
        "distillery": "Buffalo Trace",
        "type": "bourbon",
        "age": "15 years",
        "release_month": "September",
        "notes": "BTAC 2026 release, aged 15 years 4 months",
        "is_limited": True,
        "source_url": "https://www.breakingbourbon.com/release-calendar",
        "_source": "breaking-bourbon",
    },
    {
        "product_name": "Sazerac 18 Year Old Rye Whiskey 2026",
        "distillery": "Buffalo Trace",
        "type": "rye",
        "age": "18 years",
        "release_month": "September",
        "notes": "BTAC 2026 release",
        "is_limited": True,
        "source_url": "https://www.breakingbourbon.com/release-calendar",
        "_source": "breaking-bourbon",
    },
    {
        "product_name": "Thomas H. Handy Rye 2026",
        "distillery": "Buffalo Trace",
        "type": "rye",
        "age": "6 years",
        "release_month": "September",
        "notes": "BTAC 2026 release",
        "is_limited": True,
        "source_url": "https://www.breakingbourbon.com/release-calendar",
        "_source": "breaking-bourbon",
    },
    {
        "product_name": "William Larue Weller Bourbon 2026",
        "distillery": "Buffalo Trace",
        "type": "bourbon",
        "age": "14 years",
        "release_month": "September",
        "notes": "BTAC 2026 release, wheated bourbon",
        "is_limited": True,
        "source_url": "https://www.breakingbourbon.com/release-calendar",
        "_source": "breaking-bourbon",
    },
    {
        "product_name": "Barrell Bourbon New Year 2026",
        "distillery": "Barrell Craft Spirits",
        "type": "blend",
        "age": "5 years",
        "release_month": "January",
        "mashbill": "78% corn, 18% rye, 4% malted barley",
        "notes": "Sourced from KY, IN, MD, WY, TN, NY, OH — 5 to 16 years old",
        "source_url": "https://seelbachs.com/blogs/news/barrell-bourbon-armagnac-2026-new-year",
        "_source": "articles/seelbachs",
    },
]


def seed():
    print('Seeding database with sample data...\n')

    conn = get_db()
    count = 0

    for raw in SAMPLE_RELEASES:
        normalized = normalize_release(raw)
        if not normalized:
            print(f'  ⚠ Skipped: {raw["product_name"]}')
            continue

        upsert_release(conn, normalized)
        source_name = raw.get('_source', 'unknown')
        source_url = raw.get('source_url', '')
        add_source(conn, normalized['id'], source_name, source_url, raw)
        count += 1

        print(f'  ✓ {normalized["product_name"]} ({normalized["proof"] or "?"} proof)')

    conn.commit()
    conn.close()

    print(f'\n✓ Seeded {count} releases into the database.')
    print('  Run `python3 server.py` to view the calendar.\n')


if __name__ == '__main__':
    seed()
