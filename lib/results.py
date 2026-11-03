"""Allocate dated experiment folders without overwriting earlier runs."""

from datetime import date
from pathlib import Path
import re


def allocate_result_directory(root, *, day=None):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    stamp = (day or date.today()).strftime("%Y%m%d")
    pattern = re.compile(rf"{stamp}-(\d+)")
    numbers = [int(match[1]) for path in root.iterdir()
               if (match := pattern.fullmatch(path.name)) is not None]
    number = max(numbers, default=0) + 1
    while True:
        directory = root / f"{stamp}-{number}"
        try:
            directory.mkdir()
        except FileExistsError:
            number += 1
        else:
            return directory


def latest_comparison(root):
    """Find the newest dated comparison; retain access to the legacy layout."""
    root = Path(root)
    pattern = re.compile(r"(\d{8})-(\d+)")
    candidates = []
    if root.exists():
        for directory in root.iterdir():
            match = pattern.fullmatch(directory.name)
            if match and (directory / "comparison.json").is_file():
                candidates.append((match[1], int(match[2]), directory / "comparison.json"))
    if candidates:
        return max(candidates)[2]
    legacy = root / "comparison" / "comparison.json"
    if legacy.is_file():
        return legacy
    raise FileNotFoundError(f"no completed comparison under {root}")
