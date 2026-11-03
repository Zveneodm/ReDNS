"""Compatibility entry: ReDNS uses the prefetch switch in config.json."""

from pathlib import Path
import sys

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sim.run_ReDNS import main, run_simulation


if __name__ == "__main__":
    raise SystemExit(main())
