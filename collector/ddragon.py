"""Data Dragon (Riot's static CDN): champion metadata for ``dim_champion``.

No API key and no rate limit. Champion names, titles and tags come from the latest game
version. A match's ``championId`` is the champion's numeric ``key`` here (Data Dragon's string
``id``, e.g. ``MonkeyKing``, differs from the display name, e.g. ``Wukong``).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx

DDRAGON = "https://ddragon.leagueoflegends.com"


def latest_version(http: httpx.Client) -> str:
    resp = http.get(f"{DDRAGON}/api/versions.json")
    resp.raise_for_status()
    return resp.json()[0]


def fetch_champions(http: httpx.Client, version: str) -> list[dict[str, Any]]:
    resp = http.get(f"{DDRAGON}/cdn/{version}/data/en_US/champion.json")
    resp.raise_for_status()
    return [
        {
            "version": version,
            "champion_key": int(champ["key"]),
            "champion_id": champ["id"],
            "name": champ["name"],
            "title": champ["title"],
            "tags": champ["tags"],
        }
        for champ in resp.json()["data"].values()
    ]


def write_champions(landing_dir: Path, version: str, rows: list[dict[str, Any]]) -> Path:
    """One file per version: rerunning for the same version rewrites the same file."""
    out = landing_dir / "ddragon_champions" / f"version={version}" / "champions.jsonl"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
    return out
