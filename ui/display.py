"""Console interface. All Rich Console usage goes through this module."""

from rich.console import Console
from rich.rule import Rule
from rich.text import Text

from models import ScanResult
from ui import layout

console = Console()


def show_banner() -> None:
    console.print()
    console.rule("[bold cyan]🌿  Skincare & Makeup Product Scanner[/]")
    console.print(
        Text(
            "Scan a barcode or type it manually, then press Enter.",
            justify="center",
            style="dim",
        )
    )
    console.print()


def show_scanning(barcode: str) -> None:
    console.print()
    console.print(f"  [cyan]Scanning barcode:[/] [bold]{barcode}[/]  …")
    console.print()


def show_result(result: ScanResult, clear: bool = True) -> None:
    if clear:
        console.clear()
    show_banner()
    panels = layout.build_result(result)
    for panel in panels:
        console.print(panel)
    console.print()
    console.print(
        Text("Press Enter or scan the next product to continue.", style="dim", justify="center")
    )
    console.print()


def show_not_found(barcode: str, clear: bool = True) -> None:
    if clear:
        console.clear()
    show_banner()
    panels = layout.build_not_found(barcode)
    for panel in panels:
        console.print(panel)
    console.print()
    console.print(
        Text("Press Enter or scan the next product to continue.", style="dim", justify="center")
    )
    console.print()


def show_error(message: str) -> None:
    panels = layout.build_error(message)
    for panel in panels:
        console.print(panel)


def show_prompt() -> None:
    console.print(Text("  Barcode: ", style="bold cyan"), end="")
