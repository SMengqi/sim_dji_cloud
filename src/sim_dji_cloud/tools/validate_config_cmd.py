import sys
from pathlib import Path
from sim_dji_cloud.config import load_config, ConfigError


def validate_config_file(path: Path) -> int:
    try:
        cfg = load_config(path)
    except ConfigError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"ERROR: failed to parse YAML: {e}", file=sys.stderr)
        return 1

    mqtt = cfg["mqtt"]
    for key in ("ca_file", "cert_file", "key_file"):
        val = mqtt.get(key)
        if val and not Path(val).is_file():
            print(f"ERROR: mqtt.{key} not readable: {val}", file=sys.stderr)
            return 1

    patterns = mqtt.get("subscribe_patterns", [])
    if not isinstance(patterns, list) or not all(isinstance(p, str) for p in patterns):
        print("ERROR: mqtt.subscribe_patterns must be list[str]", file=sys.stderr)
        return 1

    print("OK")
    return 0
