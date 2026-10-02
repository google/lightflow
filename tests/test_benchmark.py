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

"""Regression tests for benchmark subprocess and payload handling."""

from __future__ import annotations

import json
import unittest
from typing import Any
from unittest import mock

from benchmarks import run_benchmark
from lightflow import runner as lightflow_runner


class BenchmarkPayloadTest(unittest.TestCase):
  """Tests benchmark payload serialization and path normalization."""

  def setUp(self) -> None:
    super().setUp()
    self.runner = run_benchmark.BenchmarkRunner()

  def tearDown(self) -> None:
    self.runner.close()
    super().tearDown()

  def test_windows_path_round_trips_through_cli_payload_parsing(self) -> None:
    path = r"C:\Users\Vero\AppData\Local\Temp\report.json"
    arg = run_benchmark._payload_arg({"output_path": path})

    self.assertEqual(
        lightflow_runner._parse_arg(arg[len("--payload=") :])["output_path"],
        path,
    )

  def test_run_step_decodes_utf8_subprocess_output(self) -> None:
    log_id = "bench_utf8"
    real_run = run_benchmark.subprocess.run
    captured_output: list[str] = []

    def run_and_capture(*args: Any, **kwargs: Any) -> Any:
      result = real_run(*args, **kwargs)
      captured_output.append(result.stdout + result.stderr)
      return result

    try:
      with mock.patch.object(
          run_benchmark.subprocess, "run", side_effect=run_and_capture
      ):
        metric = self.runner._run_step(
            "utf8",
            [
                "start",
                "--lightflow=examples/hn_digest",
                f"--log_id={log_id}",
                '--payload={"offline": true, "limit": 1}',
            ],
            expected_exit=2,
        )
    finally:
      self.runner._run_step("cleanup", ["cleanup", f"--log_id={log_id}"], 0)

    self.assertEqual(len(captured_output), 1)
    self.assertIn("ℹ️", captured_output[0])
    self.assertGreater(metric.stdout_chars, 0)

  def test_normalize_text_canonicalizes_native_and_json_paths(self) -> None:
    original_work_dir = self.runner.work_dir
    self.runner.work_dir = (
        r"C:\Users\Vero\AppData\Local\Temp\lightflow_bench_work"
    )
    try:
      native_path = self.runner.work_dir + r"\output.md"
      escaped_path = json.dumps(native_path)[1:-1]

      normalized = self.runner._normalize_text(
          f"native={native_path}; escaped={escaped_path}"
      )

      self.assertEqual(
          normalized,
          "native=/tmp/lf_bench/output.md; "
          "escaped=/tmp/lf_bench/output.md",
      )

      normalized_payload = self.runner._normalize_text(
          json.dumps({"output_path": native_path})
      )
      self.assertEqual(
          json.loads(normalized_payload),
          {"output_path": "/tmp/lf_bench/output.md"},
      )
    finally:
      self.runner.work_dir = original_work_dir


if __name__ == "__main__":
  unittest.main()
