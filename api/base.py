"""Abstract base class for all API database adapters."""

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Optional

import httpx

logger = logging.getLogger(__name__)


class RawProductData:
    """Raw response from a single database lookup."""

    def __init__(
        self,
        source: str,
        found: bool,
        product_name: str = "",
        brand: str = "",
        ingredients: list[str] = None,
        product_score: Optional[float] = None,
        ingredient_scores: dict[str, float] = None,
        score_is_direct: bool = True,
    ):
        self.source = source
        self.found = found
        self.product_name = product_name
        self.brand = brand
        self.ingredients = ingredients or []
        self.product_score = product_score           # 0-10 scale
        self.ingredient_scores = ingredient_scores or {}  # name -> 0-10
        self.score_is_direct = score_is_direct       # True = came from API; False = derived


class BaseAPIClient(ABC):
    """Abstract base for database API adapters."""

    def __init__(self, config: dict):
        self.config = config
        self.timeout = config.get("timeout_seconds", 5)

    @abstractmethod
    async def lookup(self, barcode: str) -> RawProductData:
        """Perform a barcode lookup and return raw product data."""
        ...

    async def _get(self, url: str, params: dict = None, headers: dict = None) -> Optional[dict]:
        """Shared HTTP GET with timeout and error handling."""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(url, params=params, headers=headers)
                if response.status_code == 404:
                    return None
                response.raise_for_status()
                return response.json()
        except httpx.TimeoutException:
            logger.warning("Timeout fetching %s", url)
            return None
        except httpx.HTTPStatusError as exc:
            logger.warning("HTTP %s from %s", exc.response.status_code, url)
            return None
        except Exception as exc:
            logger.warning("Error fetching %s: %s", url, exc)
            return None
