from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stdout
from datetime import date
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from lib.comparison import run_comparison, source_fingerprint
from lib.config import experiment_model, validate_config
from lib.results import allocate_result_directory, latest_comparison
from sim.run_ReDNS import Simulation as ReDNSSimulation
from sim.run_baseline import Simulation as BaselineSimulation
from tests.helpers import config
from tests.test_timing import fake_result


class StructureTests(unittest.TestCase):
    def test_prefetch_switch_is_boolean_and_selects_existing_variants(self):
        self.assertFalse(validate_config({})["prefetch"]["enabled"])
        for value in (None, 0, 1, "true", []):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "prefetch.enabled"):
                validate_config({"prefetch": {"enabled": value}})
        for enabled in (False, True):
            cfg = config(prefetch={"enabled": enabled})
            for base, cls in (("baseline", BaselineSimulation), ("ReDNS", ReDNSSimulation)):
                expected = base + ("-prefetch" if enabled else "")
                self.assertEqual(experiment_model(base, cfg), expected)
                self.assertEqual(cls(cfg).model, expected)

    def test_dated_names_increment_and_latest_uses_numeric_order(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "20261101-2").mkdir()
            (root / "20261101-10").mkdir()
            (root / "20261101-not-a-number").mkdir()
            for name in ("20261101-2", "20261101-10"):
                (root / name / "comparison.json").write_text("{}")
            self.assertEqual(latest_comparison(root), root / "20261101-10/comparison.json")
            self.assertEqual(allocate_result_directory(root, day=date(2026, 11, 1)).name, "20261101-11")
            self.assertEqual(allocate_result_directory(root, day=date(2026, 11, 2)).name, "20261102-1")
            with ThreadPoolExecutor(max_workers=4) as pool:
                allocated = list(pool.map(lambda _: allocate_result_directory(root, day=date(2026, 11, 2)),
                                          range(8)))
            self.assertEqual(len(set(allocated)), 8)

    def test_latest_skips_incomplete_runs_and_accepts_legacy_results(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            legacy = root / "comparison/comparison.json"
            legacy.parent.mkdir()
            legacy.write_text("{}")
            (root / "20261101-1").mkdir()
            self.assertEqual(latest_comparison(root), legacy)
            (root / "20261101-1/comparison.json").write_text("{}")
            (root / "20261101-2").mkdir()
            self.assertEqual(latest_comparison(root), root / "20261101-1/comparison.json")

    def test_default_comparison_uses_config_switch_and_flat_date_folder(self):
        for enabled in (False, True):
            with self.subTest(enabled=enabled), tempfile.TemporaryDirectory() as temporary:
                cfg = config(time_scale=1, duration_seconds=1, output_dir=temporary,
                             prefetch={"enabled": enabled},
                             timing={"probe_duration_seconds": 1, "probe_warm_up_seconds": 0})
                with redirect_stdout(io.StringIO()):
                    manifest = run_comparison(cfg, runner=fake_result)
                expected = {"baseline-prefetch", "ReDNS-prefetch"} if enabled else {"baseline", "ReDNS"}
                self.assertEqual(set(manifest["models"]), expected)
                folder = Path(manifest["result_directory"])
                self.assertEqual(folder.parent, Path(temporary))
                self.assertRegex(folder.name, r"^\d{8}-1$")
                self.assertEqual(set(manifest["results"].values()),
                                 {f"{model}_results.json" for model in expected})
                self.assertTrue((folder / "comparison.json").is_file())
                self.assertTrue((folder / "config.json").is_file())
                self.assertFalse(list(folder.glob("run-*")))

    def test_source_fingerprint_includes_library_code(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for folder in ("src", "sim", "lib"):
                (root / folder).mkdir()
            source = root / "lib/config.py"
            source.write_text("first = True\n")
            with patch("lib.comparison.PROJECT_ROOT", root):
                before = source_fingerprint()
                source.write_text("first = False\n")
                self.assertNotEqual(source_fingerprint(), before)

    def test_real_single_entry_and_comparison_work_from_another_directory(self):
        project = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for enabled in (False, True):
                cfg = config(prefetch={"enabled": enabled, "report_seconds": 0.25, "advice_seconds": 0.5},
                             output_dir=str(root / "res"), time_scale=0.25, queries_per_second=40,
                             warm_up_seconds=0, duration_seconds=1,
                             timing={"probe_duration_seconds": 1, "probe_warm_up_seconds": 0})
                source = root / "config.json"
                source.write_text(json.dumps(cfg))
                completed = subprocess.run([sys.executable, str(project / "sim/run_ReDNS.py"),
                                            "--config", str(source)], cwd=root,
                                           text=True, capture_output=True, timeout=30)
                self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
            folders = sorted((root / "res").iterdir())
            self.assertEqual(len(folders), 2)
            self.assertTrue((folders[0] / "ReDNS_results.json").is_file())
            self.assertTrue((folders[1] / "ReDNS-prefetch_results.json").is_file())
            self.assertFalse(json.loads((folders[0] / "config.json").read_text())["prefetch"]["enabled"])
            self.assertTrue(json.loads((folders[1] / "config.json").read_text())["prefetch"]["enabled"])
            completed = subprocess.run([sys.executable, str(project / "sim/run_comparison.py"),
                                        "--config", str(source)], cwd=root,
                                       text=True, capture_output=True, timeout=30)
            self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
            folders = sorted((root / "res").iterdir())
            self.assertEqual(len(folders), 3)
            manifest = json.loads((folders[2] / "comparison.json").read_text())
            self.assertEqual(set(manifest["models"]), {"baseline-prefetch", "ReDNS-prefetch"})


if __name__ == "__main__":
    unittest.main()
