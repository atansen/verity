#!/usr/bin/env python3
"""
Enrich ewg_products.json with ingredient lists and product-level concerns
scraped from EWG Skin Deep product pages.

Captures per product:
  - ingredients[]            list of ingredient names
  - ingredient_scores{}      name -> EWG score (1-10)
  - ewg_concerns[]           product-level concern categories with severity
                             e.g. [{"category": "Cancer", "level": "MODERATE"}, ...]

Usage:
    python scripts/enrich_ingredients.py           # enrich missing products only
    python scripts/enrich_ingredients.py --all     # re-scrape everything

Writes results to db/ewg_products.json in-place.
Safe to interrupt and re-run — saves progress every 25 products.
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
RATE_DELAY = 0.1

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html",
    "Accept-Language": "en-US,en;q=0.9",
}

INGREDIENT_PATTERN = re.compile(
    r'alt="Ingredient score:\s*(\d+)".*?'
    r'<div class="td-ingredient-interior">\s*(.*?)\s*</div>',
    re.DOTALL,
)

CONCERN_PATTERN = re.compile(
    r'<div class="concern-(low|moderate|high)"></div>\s*(?:LOW|MODERATE|HIGH)</div>\s*'
    r'<div class="concern-text">(.*?)</div>',
    re.DOTALL | re.IGNORECASE,
)

# Map EWG concern text -> our standard category names
CONCERN_CATEGORY_MAP = {
    "cancer": "Cancer",
    "allergies & immunotoxicity": "Allergies & immunotoxicity",
    "allergies and immunotoxicity": "Allergies & immunotoxicity",
    "developmental and reproductive toxicity": "Developmental & reproductive toxicity",
    "developmental & reproductive toxicity": "Developmental & reproductive toxicity",
    "endocrine disruption": "Hormonal disruption",
    "use restrictions": "Restricted in other countries",
    "organ system toxicity": "Organ system toxicity",
    "neurotoxicity": "Neurotoxicity",
    "contamination concerns": "Contamination concerns",
    "irritation (skin, eyes, or lungs)": "Allergies & immunotoxicity",
    "enhanced skin absorption": "Organ system toxicity",
    "biochemical or cellular level changes": "Organ system toxicity",
    "occupational hazards": "Organ system toxicity",
    "ecotoxicology": "Contamination concerns",
}


def load_db() -> dict:
    with open(DB_PATH) as f:
        return json.load(f)


def save_db(db: dict) -> None:
    with open(DB_PATH, "w") as f:
        json.dump(db, f, indent=2)


def group_by_ewg_id(db: dict) -> dict:
    groups: dict = {}
    for barcode, record in db.items():
        eid = record["ewg_id"]
        if eid not in groups:
            groups[eid] = {"record": record, "barcodes": []}
        groups[eid]["barcodes"].append(barcode)
    return groups


def parse_page(html: str) -> tuple[list[dict], list[dict]]:
    """
    Parse an EWG product page and return:
      ingredients: [{name, score}]
      concerns:    [{category, level}]  — MODERATE and HIGH only
    """
    # Ingredients + per-ingredient scores
    ingredients = []
    seen = set()
    for score_str, raw_name in INGREDIENT_PATTERN.findall(html):
        name = re.sub(r"<[^>]+>", "", raw_name)
        name = re.sub(r"\s+", " ", name).strip().title()
        if not name or name in seen:
            continue
        seen.add(name)
        try:
            score = int(score_str)
        except ValueError:
            score = 1
        ingredients.append({"name": name, "score": score})

    # Product-level concerns (skip LOW — not meaningful for flagging)
    concerns = []
    seen_cats = set()
    for level, raw_cat in CONCERN_PATTERN.findall(html):
        if level.lower() == "low":
            continue
        cat_key = re.sub(r"\s+", " ", raw_cat).strip().lower()
        cat_key = re.sub(r"<[^>]+>", "", cat_key)
        mapped = CONCERN_CATEGORY_MAP.get(cat_key)
        if not mapped:
            # Try partial match
            for key, val in CONCERN_CATEGORY_MAP.items():
                if key in cat_key:
                    mapped = val
                    break
        if not mapped:
            mapped = re.sub(r"\s+", " ", raw_cat).strip()
        if mapped not in seen_cats:
            seen_cats.add(mapped)
            concerns.append({"category": mapped, "level": level.upper()})

    return ingredients, concerns


async def fetch_product(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    ewg_id: str,
    delay: float,
) -> tuple[str, list[dict], list[dict]]:
    await asyncio.sleep(delay)
    url = EWG_PRODUCT_URL.format(ewg_id=ewg_id)
    async with sem:
        try:
            resp = await client.get(url, timeout=REQUEST_TIMEOUT, headers=HEADERS)
            if resp.status_code != 200:
                return ewg_id, [], []
            ingredients, concerns = parse_page(resp.text)
            return ewg_id, ingredients, concerns
        except Exception:
            return ewg_id, [], []


async def main() -> None:
    re_scrape_all = "--all" in sys.argv

    db = load_db()
    groups = group_by_ewg_id(db)

    def needs_enrichment(g: dict) -> bool:
        r = g["record"]
        if re_scrape_all:
            return True
        # Re-scrape if ingredients or concerns are missing/empty
        return not r.get("ingredients") or r.get("ewg_concerns") is None

    already_done = sum(1 for g in groups.values() if not needs_enrichment(g))
    to_do = [(eid, g) for eid, g in groups.items() if needs_enrichment(g)]

    print(f"Products in DB:      {len(groups)}")
    print(f"Already complete:    {already_done}")
    print(f"To fetch:            {len(to_do)}")
    print()

    if not to_do:
        print("Nothing to do.")
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
            ewg_id, ingredients, concerns = await coro
            processed += 1

            for record in db.values():
                if record["ewg_id"] == ewg_id:
                    record["ingredients"] = [i["name"] for i in ingredients]
                    record["ingredient_scores"] = {
                        i["name"]: i["score"] for i in ingredients
                    }
                    record["ewg_concerns"] = concerns

            name = groups[ewg_id]["record"]["name"][:50]
            if ingredients:
                found += 1
                concern_labels = [c["category"] for c in concerns]
                print(f"  [{processed:>3}/{len(to_do)}] ✓  {name}")
                print(f"          {len(ingredients)} ingredients | concerns: {concern_labels}")
            else:
                print(f"  [{processed:>3}/{len(to_do)}]    {name} — no data")

            if processed % 25 == 0:
                save_db(db)
                elapsed = time.monotonic() - start
                eta = (elapsed / processed) * (len(to_do) - processed)
                print(f"  --- saved  {processed}/{len(to_do)}  |  ETA {eta:.0f}s ---")

    save_db(db)
    elapsed = time.monotonic() - start
    print(f"\nDone in {elapsed:.1f}s")
    print(f"Ingredients found for {found}/{len(to_do)} products")


if __name__ == "__main__":
    asyncio.run(main())
