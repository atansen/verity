"""
Verity™ Web Server
Flask API that exposes the scanner logic as HTTP endpoints
for the web frontend to call.

Run on Raspberry Pi:
    pip3 install flask flask-cors
    python3 web_server.py

Endpoints:
    GET  /health              → { "status": "ok", "name": "Verity" }
    GET  /scan/<barcode>      → full scan result
    GET  /product/<barcode>   → raw product info only (no scoring)
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import json
import os
import sys
import traceback

# ── Make sure sibling modules are importable ──────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCANNER_DIR = os.path.join(BASE_DIR, "..")  # product_scanner/
sys.path.insert(0, SCANNER_DIR)

from api.open_beauty_facts import lookup_product
from scorer import score_product
from alternatives import find_alternatives

# ── Load config ───────────────────────────────────────────────────────────────
CONFIG_PATH = os.path.join(SCANNER_DIR, "config.json")
try:
    with open(CONFIG_PATH) as f:
        CONFIG = json.load(f)
except FileNotFoundError:
    CONFIG = {}

# ── App setup ─────────────────────────────────────────────────────────────────
app = Flask(__name__)
CORS(app)   # Allow requests from any origin (web frontend on same LAN)


# ── Helper: shape a consistent error response ─────────────────────────────────
def err(message, code=400):
    return jsonify({"ok": False, "error": message}), code


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/health")
def health():
    """Heartbeat — lets the web UI show Online / Offline status."""
    return jsonify({
        "ok": True,
        "status": "ok",
        "name": "Verity™",
        "version": "1.0.0"
    })


@app.route("/product/<barcode>")
def product_info(barcode):
    """
    Raw product lookup (no scoring).
    Returns the product dict from Open Beauty Facts.
    """
    barcode = barcode.strip()
    if not barcode:
        return err("Barcode is required")

    try:
        product = lookup_product(barcode)
    except Exception as e:
        traceback.print_exc()
        return err(f"Lookup failed: {str(e)}", 500)

    if not product:
        return jsonify({
            "ok": False,
            "error": "Product not found",
            "barcode": barcode
        }), 404

    return jsonify({"ok": True, "product": product})


@app.route("/scan/<barcode>")
def scan(barcode):
    """
    Full scan: lookup → score → alternatives.

    Response shape:
    {
      "ok": true,
      "barcode": "...",
      "product": {
        "name": "...",
        "brand": "...",
        "category": "...",
        "ingredients": [...]
      },
      "score": {
        "compiled": 4.2,
        "rating": "moderate",        // "good" | "moderate" | "bad"
        "label": "⚠️ Moderate",
        "databases": {
          "ewg":              { "score": 5.0, "source": "fallback" },
          "open_beauty_facts": { "score": 3.0, "source": "api" }
        }
      },
      "concerns": [
        { "name": "Hormonal disruption", "ingredients": ["oxybenzone"] },
        ...
      ],
      "flagged_ingredients": [
        { "name": "oxybenzone", "score": 8, "concerns": ["Hormonal disruption", "Cancer"] },
        ...
      ],
      "alternatives": {
        "same_brand": [ { "name": "...", "brand": "...", "score": 2.1 }, ... ],
        "other_brand": [ ... ]
      }
    }
    """
    barcode = barcode.strip()
    if not barcode:
        return err("Barcode is required")

    # ── 1. Product lookup ──────────────────────────────────────────────────────
    try:
        product = lookup_product(barcode)
    except Exception as e:
        traceback.print_exc()
        return err(f"Product lookup failed: {str(e)}", 500)

    if not product:
        return jsonify({
            "ok": False,
            "error": "Product not found. Try scanning again or check the barcode.",
            "barcode": barcode
        }), 404

    # ── 2. Scoring ─────────────────────────────────────────────────────────────
    try:
        score_result = score_product(product)
    except Exception as e:
        traceback.print_exc()
        return err(f"Scoring failed: {str(e)}", 500)

    compiled = score_result.get("compiled", 0)
    thresholds = CONFIG.get("score_thresholds", {"good": [0, 3], "moderate": [4, 6], "bad": [7, 10]})

    if compiled <= thresholds["good"][1]:
        rating = "good"
        label = "✅ Good"
    elif compiled <= thresholds["moderate"][1]:
        rating = "moderate"
        label = "⚠️ Moderate"
    else:
        rating = "bad"
        label = "❌ Bad"

    # ── 3. Alternatives (only if score > 3) ────────────────────────────────────
    alts = {"same_brand": [], "other_brand": []}
    threshold = CONFIG.get("alternatives", {}).get("only_show_if_score_above", 3)
    if compiled > threshold:
        try:
            alts = find_alternatives(product, score_result)
        except Exception as e:
            traceback.print_exc()
            # Non-fatal — return empty alts rather than crashing
            alts = {"same_brand": [], "other_brand": [], "error": str(e)}

    # ── 4. Shape concerns list ─────────────────────────────────────────────────
    # score_result["concerns"] is a dict: { "Cancer": ["ingredient_a"], ... }
    raw_concerns = score_result.get("concerns", {})
    concerns_list = [
        {"name": concern, "ingredients": ingredients}
        for concern, ingredients in raw_concerns.items()
        if ingredients
    ]

    # ── 5. Flagged ingredients list ────────────────────────────────────────────
    flagged = score_result.get("flagged_ingredients", [])
    max_shown = CONFIG.get("display", {}).get("max_ingredients_shown", 10)
    flagged = flagged[:max_shown]

    # ── 6. Build response ──────────────────────────────────────────────────────
    return jsonify({
        "ok": True,
        "barcode": barcode,
        "product": {
            "name": product.get("name", "Unknown Product"),
            "brand": product.get("brand", "Unknown Brand"),
            "category": product.get("category", ""),
            "ingredients": product.get("ingredients", [])
        },
        "score": {
            "compiled": round(compiled, 1),
            "rating": rating,
            "label": label,
            "databases": score_result.get("databases", {})
        },
        "concerns": concerns_list,
        "flagged_ingredients": flagged,
        "alternatives": alts
    })


@app.route("/config")
def get_config():
    """Return the public parts of config (no API keys)."""
    safe = {
        "score_thresholds": CONFIG.get("score_thresholds", {}),
        "display": CONFIG.get("display", {}),
        "database_weights": CONFIG.get("database_weights", {})
    }
    return jsonify({"ok": True, "config": safe})


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Verity™ Web Server")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind (default 0.0.0.0)")
    parser.add_argument("--port", default=5000, type=int, help="Port to listen on (default 5000)")
    parser.add_argument("--debug", action="store_true", help="Enable Flask debug mode")
    args = parser.parse_args()

    print()
    print("  ✨ Verity™ Web Server")
    print(f"  🌸 Listening on http://{args.host}:{args.port}")
    print(f"  💗 Open http://[your-pi-ip]:{args.port} in a browser")
    print()

    app.run(host=args.host, port=args.port, debug=args.debug)
