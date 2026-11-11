"""Load the versioned JSON Schema contracts in ``contracts/<version>/`` and validate payloads.

The schemas are plain JSON files, so they are not tied to Python: a producer in any language
could enforce the same contract. Each schema is meta-validated when loaded, so a broken
contract fails at startup instead of silently accepting everything.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from collector.events import EVENT_TYPES

CONTRACTS_DIR = Path(
    os.environ.get("RIFT_CONTRACTS_DIR", Path(__file__).resolve().parent.parent / "contracts")
)
SCHEMA_VERSION = "v1"
MAX_VIOLATIONS = 10  # enough to debug, small enough for a DLQ message
MAX_MESSAGE_CHARS = 300  # jsonschema messages can embed whole arrays


@dataclass(frozen=True)
class Violation:
    path: str  # JSON path, e.g. "$.info.participants[3].teamPosition"
    message: str
    rule: str  # the JSON Schema keyword that failed: required, enum, minItems, ...


def _json_path(parts: Iterable[str | int]) -> str:
    path = "$"
    for part in parts:
        path += f"[{part}]" if isinstance(part, int) else f".{part}"
    return path


class ContractRegistry:
    def __init__(self, version: str = SCHEMA_VERSION, root: Path = CONTRACTS_DIR) -> None:
        self.version = version
        self._validators: dict[str, Draft202012Validator] = {}
        for event_type in EVENT_TYPES:
            schema = json.loads((root / version / f"{event_type}.schema.json").read_text())
            Draft202012Validator.check_schema(schema)  # the contract itself must be valid
            self._validators[event_type] = Draft202012Validator(schema)

    def validate(self, event_type: str, payload: Any) -> list[Violation]:  # noqa: ANN401 - JSON
        validator = self._validators.get(event_type)
        if validator is None:
            return [Violation("$", f"no {self.version} contract for {event_type!r}", "contract")]
        errors = sorted(validator.iter_errors(payload), key=lambda e: [str(p) for p in e.path])
        return [
            Violation(_json_path(e.absolute_path), e.message[:MAX_MESSAGE_CHARS], str(e.validator))
            for e in errors[:MAX_VIOLATIONS]
        ]
