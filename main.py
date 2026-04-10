#!/usr/bin/env python3
"""
Skincare & Makeup Product Scanner
Raspberry Pi terminal application — entry point.

Usage:
    python main.py

The USB barcode scanner behaves as a keyboard (HID class).
Barcodes appear as typed characters terminated by Enter, which triggers
a lookup automatically without any additional keypress required.
"""

import asyncio
import json
import logging
import os
import re
import sys
import time

# Ensure the project root is on the path (needed when launched from systemd)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scorer import Scanner
from ui import display

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")

# Valid barcode lengths: EAN-8 (8), UPC-E (6 or 8), UPC-A (12), EAN-13 (13)
BARCODE_PATTERN = re.compile(r"^\d{6,14}$")


def load_config() -> dict:
    if not os.path.exists(CONFIG_PATH):
        print(f"Warning: config.json not found at {CONFIG_PATH}. Using defaults.")
        return _default_config()
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)


def _default_config() -> dict:
    return {
        "databases": {
            "ewg": {"enabled": True, "weight": 0.70, "api_key": "",
                    "base_url": "https://data.ewg.org/skindeep/v2", "timeout_seconds": 5},
            "open_beauty_facts": {"enabled": True, "weight": 0.30,
                                   "base_url": "https://world.openbeautyfacts.org", "timeout_seconds": 5},
        },
        "scoring": {"good_max": 3, "moderate_max": 6, "alternatives_threshold": 3},
        "alternatives": {"same_brand_cap": 5, "other_brand_cap": 5, "same_brand_session_cap": 10},
        "display": {"clear_screen_between_scans": True, "show_barcode_in_header": True,
                    "max_flagged_ingredients_shown": 20},
        "offline": {"force_offline": False},
    }


async def run_scan(scanner: Scanner, barcode: str, clear: bool) -> None:
    """Run the full scan pipeline and display results."""
    display.show_scanning(barcode)
    try:
        result = await scanner.scan(barcode)
    except Exception as exc:
        logger.exception("Unhandled error during scan")
        display.show_error(str(exc))
        return

    if result.not_found:
        display.show_not_found(barcode, clear=clear)
    else:
        display.show_result(result, clear=clear)


def read_barcode() -> str:
    """Read a barcode from stdin. Works with USB HID scanners (keyboard emulation)."""
    try:
        line = input()
        return line.strip()
    except EOFError:
        return ""


def is_valid_barcode(value: str) -> bool:
    return bool(BARCODE_PATTERN.match(value))


def main() -> None:
    config = load_config()
    clear = config.get("display", {}).get("clear_screen_between_scans", True)

    try:
        scanner = Scanner(config)
    except Exception as exc:
        print(f"Fatal: failed to initialize scanner: {exc}", file=sys.stderr)
        sys.exit(1)

    display.show_banner()

    last_barcode = ""
    last_scan_time = 0.0
    debounce_seconds = 2.0  # Prevent double-fire from scanner hardware

    while True:
        display.show_prompt()
        try:
            raw_input = read_barcode()
        except KeyboardInterrupt:
            print("\nExiting.")
            sys.exit(0)

        if not raw_input:
            continue

        barcode = raw_input.strip()

        if not is_valid_barcode(barcode):
            display.show_error(
                f"'{barcode}' does not look like a valid barcode (expected 6-14 digits). "
                "Please try again."
            )
            continue

        # Debounce: ignore duplicate scan within window
        now = time.monotonic()
        if barcode == last_barcode and (now - last_scan_time) < debounce_seconds:
            continue

        last_barcode = barcode
        last_scan_time = now

        asyncio.run(run_scan(scanner, barcode, clear=clear))


if __name__ == "__main__":
    main()
