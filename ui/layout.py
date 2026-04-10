"""Rich layout builders. Returns renderables from ScanResult — never prints directly."""

from rich.columns import Columns
from rich.console import Group
from rich.panel import Panel
from rich.style import Style
from rich.table import Table
from rich.text import Text

from models import ScanResult, ScoreBand
from ui.themes import (
    BAD_COLOR, BORDER_STYLE, DIM_COLOR, GOOD_COLOR, HEADER_COLOR,
    INFO_COLOR, MODERATE_COLOR, SCORE_STYLES, ingredient_color,
)


def build_result(result: ScanResult) -> list:
    """Build the full list of Rich renderables for a scan result."""
    panels = [
        _product_header(result),
        _compilation_score(result),
        _database_breakdown(result),
        _red_flags(result),
        _flagged_ingredients(result),
    ]
    if result.show_alternatives:
        panels.append(_alternatives(result))

    if result.offline_mode:
        panels.append(Panel(
            Text("⚠  OFFLINE MODE — results are based on the built-in ingredient database only.", style="yellow"),
            border_style="yellow",
        ))

    return panels


def build_not_found(barcode: str) -> list:
    """Renderables for a barcode not found in any database."""
    text = Text()
    text.append(f"Barcode {barcode!r} was not found in any database.\n\n", style="bold yellow")
    text.append("• Check that the barcode was scanned correctly.\n", style=DIM_COLOR)
    text.append(
        "• You can help by adding this product at: ",
        style=DIM_COLOR,
    )
    text.append("https://world.openbeautyfacts.org/product/add", style=INFO_COLOR)
    return [Panel(text, title="[bold yellow]Product Not Found[/]", border_style="yellow")]


def build_error(message: str) -> list:
    """Renderables for an unexpected error."""
    return [Panel(
        Text(f"An error occurred: {message}", style="red"),
        title="[bold red]Error[/]",
        border_style="red",
    )]


# ---------------------------------------------------------------------------
# Panel builders
# ---------------------------------------------------------------------------

def _product_header(result: ScanResult) -> Panel:
    text = Text()
    text.append(f"{result.band.emoji}  ", style="bold")
    text.append(result.product_name, style=HEADER_COLOR)
    if result.brand:
        text.append(f"\nBrand: ", style=DIM_COLOR)
        text.append(result.brand, style="white")
    text.append(f"\nBarcode: ", style=DIM_COLOR)
    text.append(result.barcode, style="white")
    return Panel(text, border_style=result.band.color, padding=(0, 1))


def _compilation_score(result: ScanResult) -> Panel:
    score_style = SCORE_STYLES[result.band.value]
    text = Text(justify="center")
    text.append(
        f"{result.compilation_score:.1f} / 10",
        style=Style(color=result.band.color, bold=True),
    )
    text.append(f"  {result.band.emoji} {result.band.value} — {result.band.label}", style=score_style)
    return Panel(
        text,
        title="[bold]COMPILED HAZARD SCORE[/]",
        border_style=result.band.color,
        padding=(1, 2),
    )


def _database_breakdown(result: ScanResult) -> Panel:
    if not result.db_results:
        return Panel(
            Text("No database results available.", style=DIM_COLOR),
            title="[bold]DATABASE BREAKDOWN[/]",
            border_style=BORDER_STYLE,
        )

    table = Table(show_header=True, header_style="bold", box=None, padding=(0, 1))
    table.add_column("Database", style="white", min_width=22)
    table.add_column("Score", justify="right", min_width=6)
    table.add_column("Grade", min_width=10)
    table.add_column("Source", style=DIM_COLOR, min_width=8)

    for db in result.db_results:
        score_text = Text(f"{db.score:.1f}", style=ingredient_color(db.score))
        grade_text = Text(f"{db.band.emoji} {db.band.value}", style=db.band.color)
        table.add_row(db.db_name, score_text, grade_text, db.source_type)

    return Panel(table, title="[bold]DATABASE BREAKDOWN[/]", border_style=BORDER_STYLE)


def _red_flags(result: ScanResult) -> Panel:
    if not result.concerns:
        text = Text("✅  No major concerns found.", style=GOOD_COLOR)
        return Panel(text, title="[bold]RED FLAGS[/]", border_style=GOOD_COLOR)

    text = Text()
    for concern in result.concerns:
        text.append(f"  ❗ {concern}\n", style=BAD_COLOR)

    return Panel(
        text,
        title=f"[bold red]RED FLAGS ({len(result.concerns)} detected)[/]",
        border_style=BAD_COLOR,
    )


def _flagged_ingredients(result: ScanResult) -> Panel:
    if not result.flagged_ingredients:
        return Panel(
            Text("No flagged ingredients detected.", style=GOOD_COLOR),
            title="[bold]FLAGGED INGREDIENTS[/]",
            border_style=GOOD_COLOR,
        )

    table = Table(show_header=True, header_style="bold", box=None, padding=(0, 1))
    table.add_column("Ingredient", style="white", min_width=28)
    table.add_column("Score", justify="right", min_width=6)
    table.add_column("Concerns", min_width=30)

    for ing in result.flagged_ingredients:
        score_text = Text(str(ing.score), style=ingredient_color(ing.score))
        concerns_text = Text(", ".join(ing.concerns) if ing.concerns else "—", style=DIM_COLOR)
        table.add_row(ing.name, score_text, concerns_text)

    title_color = BAD_COLOR if any(i.score > 6 for i in result.flagged_ingredients) else MODERATE_COLOR
    return Panel(
        table,
        title=f"[bold {title_color}]FLAGGED INGREDIENTS ({len(result.flagged_ingredients)})[/]",
        border_style=title_color,
    )


def _alternatives(result: ScanResult) -> Panel:
    text = Text()

    same = [a for a in result.alternatives if a.same_brand]
    other = [a for a in result.alternatives if not a.same_brand]

    if not result.alternatives:
        text.append("No safer alternatives found yet in the database.\n", style=DIM_COLOR)
        text.append(
            "Tip: Look for products from brands known for clean formulations\n"
            "(e.g. CeraVe, Vanicream, La Roche-Posay, INCI Beauty-certified brands).",
            style=DIM_COLOR,
        )
    else:
        if same:
            text.append(f"SAME BRAND ({result.brand or 'Unknown'}):\n", style=f"bold {INFO_COLOR}")
            for alt in same:
                band = ScoreBand.from_score(alt.score)
                text.append(f"  • {alt.name}  —  {band.emoji} {alt.score:.1f}/10\n", style="white")

        if other:
            text.append("OTHER BRANDS:\n", style=f"bold {INFO_COLOR}")
            for alt in other:
                band = ScoreBand.from_score(alt.score)
                text.append(f"  • {alt.brand} — {alt.name}  —  {band.emoji} {alt.score:.1f}/10\n", style="white")

    return Panel(
        text,
        title=f"[bold {INFO_COLOR}]SAFER ALTERNATIVES[/]",
        border_style=INFO_COLOR,
    )
