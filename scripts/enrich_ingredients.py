#!/usr/bin/env python3
"""
Enrich ewg_products.json with ingredient lists scraped from EWG Skin Deep.

Each product in the CSV has an EWG product ID. EWG's product pages
(https://www.ewg.org/skindeep/products/{id}/) contain a fully rendered
ingredient table with per-ingredient hazard scores (1-10).

Usage:
    python scripts/enrich_ingredients.py

Writes results to db/ewg_products.json in-place.
Saves progress every 25 products — safe to interrupt and re-run.
"""

import asyncio
import json
import os
import re
import sys
import time

import httpx

DB_PATH = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "db", "ewg_products.json")
)
EWG_PRODUCT_URL = "https://www.ewg.org/skindeep/products/{ewg_id}/"
CONCURRENCY = 10
REQUEST_TIMEOUT = 10
RATE_DELAY = 0.1   # seconds stagger between task starts

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html",
    "Accept-Language": "en-US,en;q=0.9",
}

# Pattern: score img alt right before the ingredient name div
INGREDIENT_PATTERN = re.compile(
    r'alt="Ingredient score:\s*(\d+)".*?'
    r'<div class="td-ingredient-interior">\s*(.*?)\s*</div>',
    re.DOTALL,
)


def load_db() -> dict:
    with open(DB_PATH) as f:
        return json.load(f)


def save_db(db: dict) -> None:
    with open(DB_PATH, "w") as f:
        json.dump(db, f, indent=2)


def group_by_ewg_id(db: dict) -> dict:
    """Return {ewg_id: {record, barcodes[]}}."""
    groups: dict = {}
    for barcode, record in db.items():
        eid = record["ewg_id"]
        if eid not in groups:
            groups[eid] = {"record": record, "barcodes": []}
        groups[eid]["barcodes"].append(barcode)
    return groups


def parse_ingredients(html: str) -> list[dict]:
    """
    Extract ingredient list from an EWG product page.
    Returns [{name, score}] sorted as they appear on the page.
    """
    rows = INGREDIENT_PATTERN.findall(html)
    result = []
    seen = set()
    for score_str, raw_name in rows:
        name = re.sub(r"\s+", " ", raw_name).strip()
        # Strip any stray HTML tags (e.g. <a>...</a>) that appear inside
        name = re.sub(r"<[^>]+>", "", name).strip()
        if not name or name in seen:
            continue
        seen.add(name)
        try:
            score = int(score_str)
        except ValueError:
            score = 1
        result.append({"name": name.title(), "score": score})
    return result


async def fetch_product(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    ewg_id: str,
    delay: float,
) -> tuple[str, list[dict]]:
    """Fetch one EWG product page and return (ewg_id, ingredients)."""
    await asyncio.sleep(delay)
    url = EWG_PRODUCT_URL.format(ewg_id=ewg_id)
    async with sem:
        try:
            resp = await client.get(url, timeout=REQUEST_TIMEOUT, headers=HEADERS)
            if resp.status_code != 200:
                return ewg_id, []
            return ewg_id, parse_ingredients(resp.text)
        except Exception:
            return ewg_id, []


async def main() -> None:
    db = load_db()
    groups = group_by_ewg_id(db)

    already_done = sum(
        1 for g in groups.values()
        if g["record"].get("ingredients") is not None
    )
    to_do = [
        (eid, g) for eid, g in groups.items()
        if g["record"].get("ingredients") is None
    ]

    print(f"Products in DB:      {len(groups)}")
    print(f"Already enriched:    {already_done}")
    print(f"To fetch:            {len(to_do)}")
    print(f"Concurrency:         {CONCURRENCY}")
    print()

    if not to_do:
        print("Nothing to do — all products already enriched.")
        return

    sem = asyncio.Semaphore(CONCURRENCY)
    found = 0
    processed = 0
    start = time.monotonic()

    async with httpx.AsyncClient(follow_redirects=True) as client:
        tasks = [
            fetch_product(client, sem, eid, i * RATE_DELAY)
            for i, (eid, _) in enumerate(to_do)
        ]

        for coro in asyncio.as_completed(tasks):
            ewg_id, ingredients = await coro
            processed += 1

            # Write back to all barcode entries for this EWG ID
            for record in db.values():
                if record["ewg_id"] == ewg_id:
                    record["ingredients"] = [i["name"] for i in ingredients]
                    record["ingredient_scores"] = {
                        i["name"]: i["score"] for i in ingredients
                    }

            name = groups[ewg_id]["record"]["name"][:50]
            if ingredients:
                found += 1
                print(f"  [{processed:>3}/{len(to_do)}] ✓  {name}  ({len(ingredients)} ingredients)")
            else:
                print(f"  [{processed:>3}/{len(to_do)}]    {name}  — no data")

            # Incremental save every 25 products
            if processed % 25 == 0:
                save_db(db)
                elapsed = time.monotonic() - start
                eta = (elapsed / processed) * (len(to_do) - processed)
                print(f"  --- saved  {processed}/{len(to_do)} done  |  {found} with ingredients  |  ETA {eta:.0f}s ---")

    save_db(db)
    elapsed = time.monotonic() - start
    print()
    print(f"Completed in {elapsed:.1f}s")
    print(f"Ingredients found for: {found} / {len(to_do)} products ({found/len(to_do)*100:.0f}%)")
    print(f"Saved to: {DB_PATH}")


if __name__ == "__main__":
    asyncio.run(main())
