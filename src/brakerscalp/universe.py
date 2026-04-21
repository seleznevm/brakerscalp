from __future__ import annotations

from pathlib import Path

from brakerscalp.domain.models import UniverseSymbol


def load_universe(path: Path) -> list[UniverseSymbol]:
    import json

    payload = json.loads(path.read_text(encoding="utf-8"))
    return [UniverseSymbol.model_validate(item) for item in payload["symbols"]]

