"""Compilation scoring engine.

Takes raw results from one or more database adapters + the built-in ingredient DB,
and produces a final ScanResult with a weighted compilation score.

To add a new database:
  1. Create api/<db_name>.py with a class extending BaseAPIClient
  2. Register it in main.py alongside EWG and OBF
  3. Add a weight entry in config.json under databases.<db_name>.weight
"""

import asyncio
import json
import logging
import os
from typing import Optional

from api.base import RawProductData
from api.ewg import EWGClient, _LOCAL_DB_PATH
from api.open_beauty_facts import OpenBeautyFactsClient
from db.ingredient_db import scan_ingredients
from models import AlternativeProduct, DatabaseResult, FlaggedIngredient, ScanResult, ScoreBand

logger = logging.getLogger(__name__)


def _load_ewg_products() -> tuple[dict, dict]:
    """
    Load ewg_products.json and return two indexes:
      barcode_index: {barcode -> record}   (the raw file)
      product_index: {ewg_id -> record}    (deduplicated, one entry per product)
    """
    path = os.path.normpath(_LOCAL_DB_PATH)
    if not os.path.exists(path):
        return {}, {}
    with open(path) as f:
        barcode_index = json.load(f)
    product_index: dict = {}
    for record in barcode_index.values():
        eid = record["ewg_id"]
        if eid not in product_index:
            product_index[eid] = record
    return barcode_index, product_index


class Scanner:
    """Orchestrates parallel API lookups and compiles the final score."""

    def __init__(self, config: dict):
        self.config = config
        self.good_max: int = config["scoring"]["good_max"]
        self.moderate_max: int = config["scoring"]["moderate_max"]
        self.alt_threshold: int = config["scoring"]["alternatives_threshold"]
        self.max_flagged: int = config["display"]["max_flagged_ingredients_shown"]
        self.force_offline: bool = config["offline"]["force_offline"]

        db_cfg = config["databases"]
        self.ewg_enabled: bool = db_cfg["ewg"]["enabled"] and not self.force_offline
        self.obf_enabled: bool = db_cfg["open_beauty_facts"]["enabled"] and not self.force_offline

        self.ewg_weight: float = db_cfg["ewg"]["weight"]
        self.obf_weight: float = db_cfg["open_beauty_facts"]["weight"]

        self.ewg_client = EWGClient(db_cfg["ewg"]) if self.ewg_enabled else None
        self.obf_client = OpenBeautyFactsClient(db_cfg["open_beauty_facts"]) if self.obf_enabled else None

        # Alternatives config
        self._same_brand_cap: int = config["alternatives"]["same_brand_cap"]
        self._other_brand_cap: int = config["alternatives"]["other_brand_cap"]
        self._same_brand_session_cap: int = config["alternatives"]["same_brand_session_cap"]
        self._same_brand_counts: dict[str, int] = {}

        # EWG product index for alternatives lookup (loaded once at startup)
        self._barcode_index, self._product_index = _load_ewg_products()

    async def scan(self, barcode: str) -> ScanResult:
        """Run a full scan for the given barcode and return a ScanResult."""
        # --- Parallel API lookups ---
        tasks = []
        task_labels = []
        if self.ewg_client:
            tasks.append(self.ewg_client.lookup(barcode))
            task_labels.append("ewg")
        if self.obf_client:
            tasks.append(self.obf_client.lookup(barcode))
            task_labels.append("obf")

        raw_results: dict[str, Optional[RawProductData]] = {}

        if tasks:
            gathered = await asyncio.gather(*tasks, return_exceptions=True)
            for label, result in zip(task_labels, gathered):
                if isinstance(result, Exception):
                    logger.warning("API lookup failed for %s: %s", label, result)
                    raw_results[label] = None
                else:
                    raw_results[label] = result
        else:
            # Fully offline mode
            raw_results = {}

        ewg_data: Optional[RawProductData] = raw_results.get("ewg")
        obf_data: Optional[RawProductData] = raw_results.get("obf")

        # --- Determine product identity ---
        product_name = "Unknown Product"
        brand = ""
        ingredients: list[str] = []

        for source in [ewg_data, obf_data]:
            if source and source.found:
                if source.product_name and source.product_name != "Unknown Product":
                    product_name = source.product_name
                if source.brand:
                    brand = source.brand
                if source.ingredients:
                    ingredients = source.ingredients
                break

        # If nothing found in any API
        if product_name == "Unknown Product" and not any(
            r and r.found for r in [ewg_data, obf_data]
        ):
            # Check built-in DB only if we have a name somehow — otherwise not found
            offline = not tasks or self.force_offline
            return ScanResult(
                barcode=barcode,
                product_name="Product Not Found",
                brand="",
                compilation_score=0.0,
                band=ScoreBand.GOOD,
                db_results=[],
                concerns=[],
                flagged_ingredients=[],
                alternatives=[],
                not_found=True,
                offline_mode=offline,
            )

        # --- Scan ingredient list against built-in DB ---
        local_matches = scan_ingredients(ingredients)
        flagged = _build_flagged_ingredients(local_matches, ewg_data)

        # --- Per-database scores ---
        db_results: list[DatabaseResult] = []

        ewg_score: Optional[float] = None
        if ewg_data and ewg_data.found:
            ewg_score = _compute_ewg_score(ewg_data, local_matches)
            db_results.append(DatabaseResult(
                db_name="EWG Skin Deep",
                score=ewg_score,
                band=ScoreBand.from_score(ewg_score, self.good_max, self.moderate_max),
                source_type="direct" if ewg_data.score_is_direct else "derived",
            ))

        obf_score: Optional[float] = None
        if obf_data and obf_data.found:
            obf_score = _compute_obf_score(local_matches)
            db_results.append(DatabaseResult(
                db_name="Open Beauty Facts",
                score=obf_score,
                band=ScoreBand.from_score(obf_score, self.good_max, self.moderate_max),
                source_type="derived",
            ))

        # If only local DB available (offline or API miss but we have ingredient list)
        local_score: Optional[float] = None
        if not db_results and local_matches:
            local_score = _compute_local_score(local_matches)
            db_results.append(DatabaseResult(
                db_name="Built-in Ingredient DB",
                score=local_score,
                band=ScoreBand.from_score(local_score, self.good_max, self.moderate_max),
                source_type="derived",
            ))

        # --- Compilation score ---
        compilation_score = _compile_score(
            ewg_score, self.ewg_weight,
            obf_score, self.obf_weight,
            local_score,
        )
        band = ScoreBand.from_score(compilation_score, self.good_max, self.moderate_max)

        # --- Concerns (deduplicated, severity sorted) ---
        all_concerns: list[str] = []
        for _, record in local_matches:
            for c in record.get("concerns", []):
                if c not in all_concerns:
                    all_concerns.append(c)
        all_concerns = _sort_concerns(all_concerns)

        # --- Alternatives ---
        show_alternatives = compilation_score > self.alt_threshold
        alternatives: list[AlternativeProduct] = []
        if show_alternatives:
            alternatives = self._find_alternatives(barcode, brand)

        offline_mode = self.force_offline or (not self.ewg_client and not self.obf_client)

        return ScanResult(
            barcode=barcode,
            product_name=product_name,
            brand=brand,
            compilation_score=round(compilation_score, 1),
            band=band,
            db_results=db_results,
            concerns=all_concerns,
            flagged_ingredients=flagged[: self.max_flagged],
            alternatives=alternatives,
            offline_mode=offline_mode,
            not_found=False,
            show_alternatives=show_alternatives,
        )

    def _find_alternatives(self, barcode: str, brand: str) -> list[AlternativeProduct]:
        """
        Search the EWG product index for safer alternatives.

        Priority:
          1. Same brand, same category, EWG score <= 4 (≈ 3 on 0–10 scale)
          2. Other brands, same category, EWG score <= 4, sorted safest first

        Category is looked up from the EWG product index using the scanned barcode.
        If the barcode is not in the EWG index the category is unknown and an empty
        list is returned — the UI shows "No safer alternatives found yet."
        """
        # Resolve scanned product in the index
        scanned = self._barcode_index.get(barcode)
        if scanned is None:
            scanned = self._barcode_index.get(barcode.lstrip("0"))
        if scanned is None:
            return []

        category = scanned.get("category", "").lower().strip()
        scanned_ewg_id = scanned["ewg_id"]
        brand_norm = brand.lower().strip()

        # Check session cap for this brand
        session_count = self._same_brand_counts.get(brand_norm, 0)
        same_brand_budget = max(0, self._same_brand_session_cap - session_count)

        same_brand: list[AlternativeProduct] = []
        other_brand: list[AlternativeProduct] = []

        for record in self._product_index.values():
            # Skip the product being scanned
            if record["ewg_id"] == scanned_ewg_id:
                continue
            # Must be in the same category
            if record.get("category", "").lower().strip() != category:
                continue
            # Must be a safer score: EWG raw 1–10, threshold ≤ 4 → normalized ≤ 3 on 0–10
            if record["score"] > 4:
                continue

            display_score = max(0.0, float(record["score"]) - 1)
            is_same_brand = record.get("brand", "").lower().strip() == brand_norm

            alt = AlternativeProduct(
                name=record["name"],
                brand=record.get("brand", ""),
                score=display_score,
                same_brand=is_same_brand,
            )
            if is_same_brand:
                same_brand.append(alt)
            else:
                other_brand.append(alt)

        # Sort each group safest first
        same_brand.sort(key=lambda a: a.score)
        other_brand.sort(key=lambda a: a.score)

        # Apply caps
        if same_brand_budget > 0:
            same_brand = same_brand[:min(self._same_brand_cap, same_brand_budget)]
            self._same_brand_counts[brand_norm] = session_count + len(same_brand)
        else:
            same_brand = []

        other_brand = other_brand[:self._other_brand_cap]

        return same_brand + other_brand


# ---------------------------------------------------------------------------
# Scoring helpers
# ---------------------------------------------------------------------------

def _compute_ewg_score(ewg_data: RawProductData, local_matches: list) -> float:
    """Use EWG's direct product score if available, else derive from ingredient scores."""
    if ewg_data.product_score is not None:
        return min(10.0, max(0.0, ewg_data.product_score))

    # Derive from EWG per-ingredient scores
    if ewg_data.ingredient_scores:
        scores = list(ewg_data.ingredient_scores.values())
        return min(10.0, max(scores))

    # Fall back to local DB
    return _compute_local_score(local_matches)


def _compute_obf_score(local_matches: list) -> float:
    """Derive OBF score from ingredient list matched against local DB."""
    return _compute_local_score(local_matches)


def _compute_local_score(local_matches: list) -> float:
    """Derive a score from local DB matches. Uses the highest single-ingredient score
    blended with average to weight worst-offenders more heavily."""
    if not local_matches:
        return 0.0
    scores = [record["score"] for _, record in local_matches]
    # Weighted: 60% max score, 40% average — penalises products with one very bad ingredient
    return min(10.0, 0.6 * max(scores) + 0.4 * (sum(scores) / len(scores)))


def _compile_score(
    ewg_score: Optional[float], ewg_weight: float,
    obf_score: Optional[float], obf_weight: float,
    local_score: Optional[float],
) -> float:
    """Weighted compilation score. If only one source, 100% weight applied to it."""
    weighted_sum = 0.0
    weight_sum = 0.0

    if ewg_score is not None:
        weighted_sum += ewg_score * ewg_weight
        weight_sum += ewg_weight

    if obf_score is not None:
        weighted_sum += obf_score * obf_weight
        weight_sum += obf_weight

    if weight_sum > 0:
        return weighted_sum / weight_sum

    if local_score is not None:
        return local_score

    return 0.0


def _build_flagged_ingredients(
    local_matches: list, ewg_data: Optional[RawProductData]
) -> list[FlaggedIngredient]:
    """Build the flagged ingredient list from local DB matches and EWG scores."""
    flagged = []
    for original_name, record in local_matches:
        # Use EWG individual score if available
        score = record["score"]
        if ewg_data and ewg_data.ingredient_scores:
            ewg_ing_score = ewg_data.ingredient_scores.get(original_name)
            if ewg_ing_score is not None:
                score = ewg_ing_score

        flagged.append(FlaggedIngredient(
            name=record["name"],
            score=score,
            concerns=record.get("concerns", []),
            description=record.get("description", ""),
            source="ewg" if (ewg_data and ewg_data.ingredient_scores.get(original_name)) else "local_db",
        ))

    # Sort by score descending (worst first)
    flagged.sort(key=lambda x: x.score, reverse=True)
    return flagged


_CONCERN_SEVERITY = [
    "Cancer",
    "Developmental & reproductive toxicity",
    "Neurotoxicity",
    "Hormonal disruption",
    "Organ system toxicity",
    "Restricted in other countries",
    "Contamination concerns",
    "Allergies & immunotoxicity",
    "Hidden / undisclosed ingredients",
]


def _sort_concerns(concerns: list[str]) -> list[str]:
    """Sort concern categories by severity (most severe first)."""
    def key(c: str) -> int:
        try:
            return _CONCERN_SEVERITY.index(c)
        except ValueError:
            return len(_CONCERN_SEVERITY)
    return sorted(set(concerns), key=key)
