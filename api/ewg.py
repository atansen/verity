"""EWG Skin Deep adapter.

Priority order:
  1. Live API (when api_key is set in config.json)
  2. Local EWG product database (db/ewg_products.json — built from the EWG CSV export)

The local DB maps barcodes to {name, brand, category, score, ewg_id}.
Scores in the local DB use EWG's 1-10 scale (same as the live API).

EWG Skin Deep API docs: https://www.ewg.org/skindeep/developers/
"""

import json
import logging
import os
import re
from typing import Optional

from .base import BaseAPIClient, RawProductData

logger = logging.getLogger(__name__)

SOURCE_NAME = "EWG Skin Deep"

_LOCAL_DB_PATH = os.path.join(os.path.dirname(__file__), "..", "db", "ewg_products.json")

# Loaded once at import time
_LOCAL_DB: dict = {}


def _load_local_db() -> None:
    global _LOCAL_DB
    path = os.path.normpath(_LOCAL_DB_PATH)
    if not os.path.exists(path):
        logger.warning("EWG local DB not found at %s", path)
        return
    with open(path, "r", encoding="utf-8") as f:
        _LOCAL_DB = json.load(f)
    logger.debug("Loaded %d EWG product entries from local DB", len(_LOCAL_DB))


_load_local_db()


class EWGClient(BaseAPIClient):
    """EWG Skin Deep adapter — API-first, local DB fallback."""

    def __init__(self, config: dict):
        super().__init__(config)
        self.api_key: str = config.get("api_key", "").strip()
        self.base_url: str = config.get("base_url", "https://data.ewg.org/skindeep/v2").rstrip("/")

    async def lookup(self, barcode: str) -> RawProductData:
        # Try live API first if key is configured
        if self.api_key:
            result = await self._api_lookup(barcode)
            if result.found:
                return result
            logger.debug("EWG API miss for %s — trying local DB", barcode)

        # Fall back to local CSV-sourced database
        return self._local_lookup(barcode)

    # ------------------------------------------------------------------
    # Live API
    # ------------------------------------------------------------------

    async def _api_lookup(self, barcode: str) -> RawProductData:
        url = f"{self.base_url}/search/products"
        params = {"search_criteria": barcode, "api_key": self.api_key}
        data = await self._get(url, params=params)
        if not data:
            return RawProductData(source=SOURCE_NAME, found=False)
        return self._parse_api(data)

    def _parse_api(self, data: dict) -> RawProductData:
        try:
            results = data.get("results") or data.get("products") or []
            if not results:
                if "name" in data or "product_name" in data:
                    results = [data]
                else:
                    return RawProductData(source=SOURCE_NAME, found=False)

            product = results[0]
            name = product.get("name") or product.get("product_name", "Unknown Product")
            brand = product.get("brand") or product.get("brand_name", "")
            raw_score = product.get("score") or product.get("hazard_score")

            product_score: Optional[float] = None
            if raw_score is not None:
                try:
                    # EWG 1-10 → 0-10 (shift by 1 so 1=0, 10=9... cap at 10)
                    product_score = max(0.0, min(10.0, float(raw_score) - 1))
                except (TypeError, ValueError):
                    pass

            ingredient_scores: dict[str, float] = {}
            ingredients: list[str] = []
            for ing in product.get("ingredients") or []:
                ing_name = ing.get("name") or ing.get("ingredient_name", "")
                ing_score = ing.get("score") or ing.get("hazard_score")
                if ing_name:
                    ingredients.append(ing_name)
                    if ing_score is not None:
                        try:
                            ingredient_scores[ing_name] = max(0.0, min(10.0, float(ing_score) - 1))
                        except (TypeError, ValueError):
                            pass

            return RawProductData(
                source=SOURCE_NAME,
                found=True,
                product_name=name,
                brand=brand,
                ingredients=ingredients,
                product_score=product_score,
                ingredient_scores=ingredient_scores,
                score_is_direct=(product_score is not None),
            )
        except Exception as exc:
            logger.warning("Error parsing EWG API response: %s", exc)
            return RawProductData(source=SOURCE_NAME, found=False)

    # ------------------------------------------------------------------
    # Local CSV-sourced database
    # ------------------------------------------------------------------

    def _local_lookup(self, barcode: str) -> RawProductData:
        """Look up a barcode in the local EWG product database."""
        record = _LOCAL_DB.get(barcode)

        # Try with leading zeros stripped (some scanners/barcodes vary)
        if record is None:
            record = _LOCAL_DB.get(barcode.lstrip("0"))

        # Try zero-padded to 13 digits (EAN-13 format)
        if record is None and len(barcode) < 13:
            padded = barcode.zfill(13)
            record = _LOCAL_DB.get(padded)

        if record is None:
            return RawProductData(source=SOURCE_NAME, found=False)

        # EWG CSV scores are on 1-10 scale; normalise to 0-10 (shift by 1)
        raw_score = record.get("score", 1)
        product_score = max(0.0, min(10.0, float(raw_score) - 1))

        # ingredient_scores from EWG are on 1-10 scale; normalise to 0-10
        raw_ing_scores = record.get("ingredient_scores") or {}
        ingredient_scores = {
            name: max(0.0, min(10.0, float(s) - 1))
            for name, s in raw_ing_scores.items()
        }

        return RawProductData(
            source=SOURCE_NAME,
            found=True,
            product_name=record.get("name", "Unknown Product"),
            brand=record.get("brand", ""),
            ingredients=record.get("ingredients") or [],
            product_score=product_score,
            ingredient_scores=ingredient_scores,
            score_is_direct=True,
        )
