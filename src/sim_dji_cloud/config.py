import os
import re
from pathlib import Path
from typing import Any
import yaml

_ENV_PATTERN = re.compile(r"\$\{env:([A-Z_][A-Z0-9_]*)\}")
REQUIRED_SECTIONS = ["mqtt", "storage", "video", "flight_detection"]
REQUIRED_SECTIONS_PILOT = ["mqtt", "storage", "pilot_flight_detection"]


class ConfigError(Exception):
    pass


def substitute_env(value: Any) -> Any:
    if isinstance(value, str):
        def replace(m: re.Match) -> str:
            var = m.group(1)
            v = os.environ.get(var)
            if v is None:
                raise ConfigError(f"env var not set: {var}")
            return v
        return _ENV_PATTERN.sub(replace, value)
    if isinstance(value, dict):
        return {k: substitute_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [substitute_env(v) for v in value]
    return value


def _load_and_validate(path: Path, required_sections: list[str]) -> dict[str, Any]:
    text = Path(path).read_text()
    raw = yaml.safe_load(text)
    if not isinstance(raw, dict):
        raise ConfigError("top-level YAML must be a mapping")

    for sect in required_sections:
        if sect not in raw:
            raise ConfigError(f"missing required section: {sect}")

    return substitute_env(raw)


def load_config(path: Path) -> dict[str, Any]:
    return _load_and_validate(path, REQUIRED_SECTIONS)


def load_pilot_config(path: Path) -> dict[str, Any]:
    return _load_and_validate(path, REQUIRED_SECTIONS_PILOT)
