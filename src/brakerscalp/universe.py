from __future__ import annotations

import json
from pathlib import Path

from brakerscalp.domain.models import UniverseSymbol


def load_universe(path: Path) -> list[UniverseSymbol]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return [UniverseSymbol.model_validate(item) for item in payload["symbols"]]


def save_universe(path: Path, symbols: list[UniverseSymbol]) -> None:
    ordered = sorted(symbols, key=lambda item: item.symbol.upper())
    payload = {
        "symbols": [
            {
                "symbol": item.symbol,
                "primary_venue": item.primary_venue.value,
            }
            for item in ordered
        ]
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
