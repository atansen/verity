"""Color and style constants for the Skincare Scanner TUI."""

from rich.style import Style

# Score band colors
GOOD_COLOR = "green"
MODERATE_COLOR = "yellow"
BAD_COLOR = "red"
INFO_COLOR = "cyan"
DIM_COLOR = "dim"
HEADER_COLOR = "bold white"

# Panel border styles
BORDER_STYLE = "bright_black"

# Score band styles
SCORE_STYLES = {
    "GOOD": Style(color="green", bold=True),
    "MODERATE": Style(color="yellow", bold=True),
    "BAD": Style(color="red", bold=True),
}

# Ingredient score color thresholds
def ingredient_color(score: float) -> str:
    if score <= 3:
        return GOOD_COLOR
    elif score <= 6:
        return MODERATE_COLOR
    else:
        return BAD_COLOR
