# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for the context-footprint measurement.

Covers `benchmarks/context_footprint.py`: every example workflow runs end to
end in-process with the scripted operator inputs, the measurement is
deterministic, the process state it borrows (cwd, argv, environment) is
restored, and the table embedded in `benchmarks/README.md` matches a fresh run.
"""

from __future__ import annotations

# pylint: disable=missing-function-docstring,protected-access

import importlib.util
import os
import sys
from typing import Any
import unittest

_OSS_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_MODULE_PATH = os.path.join(_OSS_ROOT, "benchmarks", "context_footprint.py")


def _load_module() -> Any:
  spec = importlib.util.spec_from_file_location(
      "context_footprint", _MODULE_PATH
  )
  if spec is None or spec.loader is None:
    raise ImportError(f"Cannot load {_MODULE_PATH}")
  module = importlib.util.module_from_spec(spec)
  sys.modules["context_footprint"] = module
  spec.loader.exec_module(module)
  return module


cf = _load_module()


class ContextFootprintTest(unittest.TestCase):
  """Runs the measurement once per class and checks its properties."""

  names: tuple[str, ...] = ()
  results: list[Any] = []
  runner_skill: int = 0
  author_skill: int = 0

  @classmethod
  def setUpClass(cls) -> None:
    super().setUpClass()
    if cf.python_subprocess_available():
      cls.names = cf.WORKFLOW_NAMES
    else:
      # Hermetic runners without a usable `sys.executable` cannot run the two
      # examples that spawn a Python worker; measure the rest.
      cls.names = tuple(
          n for n in cf.WORKFLOW_NAMES if n not in cf.WORKFLOWS_SPAWNING_PYTHON
      )
    with cf.FootprintRunner() as fp:
      cls.results = fp.run_all(cls.names)
      cls.runner_skill, cls.author_skill = fp.skill_chars()

  def _by_name(self, name: str) -> Any:
    return next(r for r in self.results if r.name == name)

  def test_every_workflow_runs_to_completion_as_scripted(self) -> None:
    # `run_all` raises if any step exits with an unexpected code, so reaching
    # here means every scripted exit code matched; check the shape as well.
    self.assertEqual([r.name for r in self.results], list(self.names))
    for r in self.results:
      with self.subTest(workflow=r.name):
        self.assertGreaterEqual(len(r.steps), 2)
        self.assertEqual(r.steps[-1].label, "cleanup")
        self.assertEqual(r.steps[-1].exit_code, 0)
        self.assertGreater(r.runtime_cmd_chars, 0)
        self.assertGreater(r.runtime_stdout_chars, 0)
        self.assertGreater(r.passport_chars, 0)
        self.assertGreater(r.source_chars, 0)
        self.assertEqual(
            r.runtime_total_chars, r.runtime_cmd_chars + r.runtime_stdout_chars
        )

  def test_measurement_is_deterministic(self) -> None:
    with cf.FootprintRunner() as fp:
      again = fp.measure("usgs_seismic_alert")
    self.assertEqual(
        again.to_dict(), self._by_name("usgs_seismic_alert").to_dict()
    )

  def test_invocation_restores_process_state(self) -> None:
    cwd, argv = os.getcwd(), list(sys.argv)
    env_before = dict(os.environ)
    with cf.FootprintRunner() as fp:
      fp.measure("hn_digest")
    self.assertEqual(os.getcwd(), cwd)
    self.assertEqual(sys.argv, argv)
    self.assertEqual(dict(os.environ), env_before)

  def test_cumulative_tokens_resend_earlier_turns(self) -> None:
    r = self._by_name("usgs_seismic_alert")
    per_turn = [s.total_tokens for s in r.steps]
    expected = sum(sum(per_turn[: i + 1]) for i in range(len(per_turn)))
    self.assertEqual(r.cumulative_multiturn_tokens, expected)

  def test_cold_start_adds_authoring_skill_only_to_authoring_workflow(
      self,
  ) -> None:
    table = cf.readme_table(self.results, self.runner_skill, self.author_skill)
    rows = {
        cf._split_cells(l)[0]: cf._split_cells(l) for l in table.splitlines()
    }
    for r in self.results:
      cells = rows[f"[`{r.name}`](../examples/{r.name}/)"]
      skill = self.runner_skill + (
          self.author_skill if r.is_authoring_workflow else 0
      )
      cold = r.runtime_total_chars + skill
      self.assertTrue(
          cells[3].startswith(f"{cold:,} ch"), (r.name, cells[3], cold)
      )

  def test_readme_table_matches_fresh_run(self) -> None:
    with open(cf._README_PATH, "r", encoding="utf-8") as f:
      text = f.read()
    problems = cf.check_readme_table(
        text, self.results, self.runner_skill, self.author_skill
    )
    self.assertEqual(
        problems,
        [],
        "\n".join(problems)
        + "\nRun `python3 benchmarks/context_footprint.py --update-readme`.",
    )

  def test_readme_check_reports_stale_rows_and_update_repairs_them(
      self,
  ) -> None:
    table = cf.readme_table(self.results, self.runner_skill, self.author_skill)
    doc = f"intro\n\n{cf.README_BEGIN}\n{table}\n{cf.README_END}\n\nrest\n"
    self.assertEqual(
        cf.check_readme_table(
            doc, self.results, self.runner_skill, self.author_skill
        ),
        [],
    )
    row = table.splitlines()[2]
    stale_row = row.replace(" ch ", " chars ", 1)
    self.assertNotEqual(row, stale_row)
    stale = doc.replace(row, stale_row)
    problems = cf.check_readme_table(
        stale, self.results, self.runner_skill, self.author_skill
    )
    self.assertEqual(len(problems), 1, problems)
    self.assertIn("chars", problems[0])
    repaired = cf.update_readme_table(
        stale, self.results, self.runner_skill, self.author_skill
    )
    self.assertEqual(repaired, doc)
    missing = doc.replace(row + "\n", "")
    problems = cf.check_readme_table(
        missing, self.results, self.runner_skill, self.author_skill
    )
    self.assertEqual(len(problems), 1, problems)
    self.assertTrue(problems[0].startswith("missing row:"), problems[0])
    with self.assertRaisesRegex(ValueError, "markers"):
      cf.check_readme_table(
          "no markers here", self.results, self.runner_skill, self.author_skill
      )

  def test_report_and_json_name_no_savings(self) -> None:
    report = cf.format_report(
        self.results, self.runner_skill, self.author_skill
    )
    self.assertIn("measurement, not a comparison", report)
    for word in ("saving", "Off-Context", "avoided", "Zero-Context"):
      self.assertNotIn(word, report)
    for r in self.results:
      self.assertNotIn("savings_ratio", r.to_dict())

  def test_normalization_strips_os_and_python_version_drift(self) -> None:
    tb_py310 = (
        "Traceback (most recent call last):\n"
        '  File "/repo/lightflow/lightflow/engine.py", line 1927, in'
        " execute_stage\n"
        "    new_payload, message = self._execute_action_with_timeout(\n"
        '  File "/repo/lightflow/examples/pypi_upgrade_guard/actions.py", line'
        " 177, in apply_and_smoke_test\n"
        "    raise RuntimeError(\n"
        "RuntimeError: boom\n"
    )
    tb_py312 = (
        "Traceback (most recent call last):\n"
        '  File "/repo/lightflow/lightflow/engine.py", line 1927, in'
        " execute_stage\n"
        "    new_payload, message = self._execute_action_with_timeout(\n"
        "                           ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^\n"
        '  File "/repo/lightflow/examples/pypi_upgrade_guard/actions.py", line'
        " 177, in apply_and_smoke_test\n"
        "    raise RuntimeError(\n"
        "RuntimeError: boom\n"
    )
    tb_py313 = (
        "Traceback (most recent call last):\n"
        '  File "/repo/lightflow/lightflow/engine.py", line 1927, in'
        " execute_stage\n"
        "    new_payload, message = self._execute_action_with_timeout(\n"
        "                           ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^\n"
        "        stage_name, python_action, payload_dict, timeout=timeout\n"
        "        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n"
        "    )\n"
        "    ^\n"
        '  File "/repo/lightflow/examples/pypi_upgrade_guard/actions.py", line'
        " 177, in apply_and_smoke_test\n"
        "    raise RuntimeError(\n"
        "    ...<2 lines>...\n"
        "    )\n"
        "RuntimeError: boom\n"
    )
    self.assertEqual(cf._normalize_traceback(tb_py312), tb_py310)
    self.assertEqual(cf._normalize_traceback(tb_py313), tb_py310)

    with cf.FootprintRunner() as fp:
      fp.work_dir = r"C:\Users\runneradmin\AppData\Local\Temp\lf_work_123"
      fp.state_dir = r"C:\Users\runneradmin\AppData\Local\Temp\lf_state_123"
      win_json = (
          r'{"p":'
          r' "C:\\Users\\runneradmin\\AppData\\Local\\Temp\\lf_work_123\\sub\\a.md"}'
      )
      self.assertEqual(
          fp._normalize_text(win_json),
          '{"p": "/tmp/lf_work/sub/a.md"}',
      )


if __name__ == "__main__":
  unittest.main()
