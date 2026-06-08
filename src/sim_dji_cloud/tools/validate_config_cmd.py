import sys
from pathlib import Path
from sim_dji_cloud.config import load_config, ConfigError

# 必填 sub-keys —— 这些缺失 / 空值会让 record 启动深处抛 KeyError 或 connect 失败；
# 在 validate-config 阶段提前报错，给用户清晰的错误位置。
# Regression (review MAJOR): config.py:37 缺 sub-key 校验。
REQUIRED_SUB_KEYS = {
    "mqtt": ["host", "port", "client_id", "dock_sn"],
    "storage": ["root"],
}


def validate_config_file(path: Path) -> int:
    try:
        cfg = load_config(path)
    except ConfigError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"ERROR: failed to parse YAML: {e}", file=sys.stderr)
        return 1

    for section, keys in REQUIRED_SUB_KEYS.items():
        sect = cfg.get(section, {})
        if not isinstance(sect, dict):
            print(f"ERROR: {section} must be a mapping", file=sys.stderr)
            return 1
        for key in keys:
            val = sect.get(key)
            if val is None or val == "":
                print(f"ERROR: required key missing or empty: {section}.{key}",
                      file=sys.stderr)
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
