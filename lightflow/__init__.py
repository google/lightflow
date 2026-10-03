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

"""Lightflow: Lightweight Deterministic DAG Engine for AI Agents."""

from __future__ import annotations

# pylint: disable=g-import-not-at-top,g-bad-import-order
try:
  from . import engine
  from . import lib
  from . import schema
  from . import visualizer
except ImportError:
  from lightflow import engine  # pyrefly: ignore[missing-import]
  from lightflow import lib  # pyrefly: ignore[missing-import]
  from lightflow import schema  # pyrefly: ignore[missing-import]
  from lightflow import visualizer  # pyrefly: ignore[missing-import]
# pylint: enable=g-import-not-at-top,g-bad-import-order

__version__ = "0.2.0"

EngineError = engine.EngineError
StageTimeoutError = engine.StageTimeoutError
OperatorActionSuspended = engine.OperatorActionSuspended
SafeCelEvaluator = engine.SafeCelEvaluator
LightflowEngine = engine.LightflowEngine
evaluate_cel = engine.evaluate_cel

PassportManager = lib.PassportManager
LightflowRunnerCLI = lib.LightflowRunnerCLI
LightflowAlreadyPausedError = lib.LightflowAlreadyPausedError
LightflowAlreadyCompletedError = lib.LightflowAlreadyCompletedError
EXIT_SUSPENDED = lib.EXIT_SUSPENDED

Lightflow = schema.Lightflow
Stage = schema.Stage
PythonAction = schema.PythonAction
OperatorAction = schema.OperatorAction
RetryPolicy = schema.RetryPolicy
PollingPolicy = schema.PollingPolicy
ActionDefinition = schema.ActionDefinition
Passport = schema.Passport
Stamp = schema.Stamp
TriggerRule = schema.TriggerRule
StampStatus = schema.StampStatus
Resolution = schema.Resolution
load_lightflow = schema.load_lightflow
parse_lightflow_text = schema.parse_lightflow_text

generate_visualizer_html = visualizer.generate_visualizer_html

__all__ = [
    "__version__",
    "ActionDefinition",
    "EXIT_SUSPENDED",
    "EngineError",
    "Lightflow",
    "LightflowAlreadyCompletedError",
    "LightflowAlreadyPausedError",
    "LightflowEngine",
    "LightflowRunnerCLI",
    "OperatorAction",
    "OperatorActionSuspended",
    "Passport",
    "PassportManager",
    "PollingPolicy",
    "PythonAction",
    "Resolution",
    "RetryPolicy",
    "SafeCelEvaluator",
    "Stage",
    "StageTimeoutError",
    "Stamp",
    "StampStatus",
    "TriggerRule",
    "evaluate_cel",
    "generate_visualizer_html",
    "load_lightflow",
    "parse_lightflow_text",
]
