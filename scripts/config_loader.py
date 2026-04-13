from __future__ import annotations

import json
from pathlib import Path


def _parse_scalar(value: str) -> object:
    text = value.strip()
    if text.lower() in {"true", "false"}:
        return text.lower() == "true"
    if text.lower() in {"null", "none"}:
        return None
    try:
        if "." in text:
            return float(text)
        return int(text)
    except ValueError:
        return text


def _parse_simple_yaml(text: str) -> dict[str, object]:
    root: dict[str, object] = {}
    stack: list[tuple[int, dict[str, object]]] = [(0, root)]
    for raw_line in text.splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" "))
        line = raw_line.strip()
        while len(stack) > 1 and indent < stack[-1][0]:
            stack.pop()
        current = stack[-1][1]
        key, _, remainder = line.partition(":")
        key = key.strip()
        value = remainder.strip()
        if not key:
            continue
        if not value:
            nested: dict[str, object] = {}
            current[key] = nested
            stack.append((indent + 2, nested))
            continue
        current[key] = _parse_scalar(value)
    return root


def load_config_file(path: str | Path) -> dict[str, object]:
    file_path = Path(path)
    text = file_path.read_text()
    if file_path.suffix.lower() == ".json":
        payload = json.loads(text)
        if not isinstance(payload, dict):
            raise RuntimeError("config file must contain an object")
        return payload
    return _parse_simple_yaml(text)


def nested_config_value(payload: dict[str, object], *keys: str) -> object:
    current: object = payload
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return None
        current = current[key]
    return current
