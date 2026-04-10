"""Data models for the Skincare Scanner application."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class ScoreBand(Enum):
    GOOD = "GOOD"
    MODERATE = "MODERATE"
    BAD = "BAD"

    @classmethod
    def from_score(cls, score: float, good_max: int = 3, moderate_max: int = 6) -> "ScoreBand":
        if score <= good_max:
            return cls.GOOD
        elif score <= moderate_max:
            return cls.MODERATE
        else:
            return cls.BAD

    @property
    def emoji(self) -> str:
        return {"GOOD": "✅", "MODERATE": "⚠️", "BAD": "❌"}[self.value]

    @property
    def label(self) -> str:
        return {"GOOD": "Low hazard", "MODERATE": "Moderate hazard", "BAD": "High hazard"}[self.value]

    @property
    def color(self) -> str:
        return {"GOOD": "green", "MODERATE": "yellow", "BAD": "red"}[self.value]


@dataclass
class FlaggedIngredient:
    name: str
    score: float
    concerns: list[str]
    description: str
    source: str  # "ewg", "local_db", "obf"


@dataclass
class DatabaseResult:
    db_name: str
    score: float
    band: ScoreBand
    source_type: str  # "direct" (from API score) or "derived" (computed from ingredients)
    available: bool = True


@dataclass
class AlternativeProduct:
    name: str
    brand: str
    score: float
    same_brand: bool


@dataclass
class ScanResult:
    barcode: str
    product_name: str
    brand: str
    compilation_score: float
    band: ScoreBand
    db_results: list[DatabaseResult]
    concerns: list[str]                  # deduplicated concern categories
    flagged_ingredients: list[FlaggedIngredient]
    alternatives: list[AlternativeProduct]
    offline_mode: bool = False
    not_found: bool = False
    error_message: Optional[str] = None
    show_alternatives: bool = False
