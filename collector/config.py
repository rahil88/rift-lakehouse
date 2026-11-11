"""Settings from environment variables (``.env`` is loaded by ``uv run --env-file .env``)."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


class ConfigError(RuntimeError):
    pass


@dataclass(frozen=True)
class Settings:
    riot_api_key: str
    riot_platform: str
    riot_region: str
    kafka_bootstrap_servers: str
    data_dir: Path

    @classmethod
    def from_env(cls, env: Mapping[str, str] = os.environ) -> Settings:
        key = env.get("RIOT_API_KEY", "").strip()
        if not key or "xxxx" in key:
            raise ConfigError("RIOT_API_KEY is not set. Copy .env.example to .env and fill it in.")
        return cls(
            riot_api_key=key,
            riot_platform=env.get("RIOT_PLATFORM", "na1").strip().lower(),
            riot_region=env.get("RIOT_REGION", "americas").strip().lower(),
            kafka_bootstrap_servers=env.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
            data_dir=Path(env.get("RIFT_DATA_DIR", "data")),
        )

    def __repr__(self) -> str:  # never print the key
        return (
            f"Settings(platform={self.riot_platform!r}, region={self.riot_region!r}, "
            f"kafka={self.kafka_bootstrap_servers!r}, data_dir={str(self.data_dir)!r})"
        )
