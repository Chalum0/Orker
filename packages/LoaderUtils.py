from importlib import import_module, invalidate_caches
from pathlib import Path
import hashlib
import json
import re
import sys


def import_attr(module_path: str, attr: str, *, kind: str):
    if module_path in sys.modules:
        del sys.modules[module_path]
    invalidate_caches()
    module = import_module(module_path)
    try:
        return getattr(module, attr)
    except AttributeError:
        raise RuntimeError(f"{kind.capitalize()} '{attr}' not found in {module_path}") from None


def regex_check_str(string: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", string))


def file_hash(path, algo="sha256") -> str:
    try:
        h = hashlib.new(algo)
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()
    except FileNotFoundError:
        return "0"


def read_json(src, fallback=None):
    src = Path(src)
    if not src.exists():
        raise FileNotFoundError(src)
    try:
        return json.loads(src.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return fallback if fallback is not None else {}


def make_file(path: str) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.touch(exist_ok=True)
    return p


def delete_file(path: str) -> bool:
    p = Path(path)
    if not p.exists() or not p.is_file():
        return False
    p.unlink()
    return True