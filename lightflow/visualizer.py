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

"""Interactive Generative UI visualizer for Lightflow OSS workflows and passports.

Generates self-contained HTML applications compliant with Antigravity
Generative UI standards and standalone browser rendering, with zero external
CDN dependencies.
"""

from __future__ import annotations

import html
import importlib.resources
import json
import os
from typing import Any, Optional

# pylint: disable=g-import-not-at-top,g-bad-import-order
try:
  from . import schema
except ImportError:
  from lightflow import schema  # pyrefly: ignore[missing-import]
# pylint: enable=g-import-not-at-top,g-bad-import-order


def _load_template() -> str:
  """Loads the visualizer.html template."""
  template_path = os.path.join(os.path.dirname(__file__), "visualizer.html")
  if os.path.exists(template_path):
    with open(template_path, "r", encoding="utf-8") as f:
      return f.read()
  for pkg in dict.fromkeys((__package__ or "lightflow", "lightflow")):
    try:
      return importlib.resources.read_text(pkg, "visualizer.html")
    except (
        FileNotFoundError,
        ModuleNotFoundError,
        TypeError,
        ValueError,
        AttributeError,
        OSError,
    ):
      pass
  raise FileNotFoundError("Could not locate visualizer.html template.")


def lightflow_to_dict(
    lightflow: schema.Lightflow | dict[str, Any],
) -> dict[str, Any]:
  """Converts a Lightflow object or proto to a JSON-serializable dictionary."""
  if isinstance(lightflow, dict):
    return lightflow
  if hasattr(lightflow, "to_dict"):
    return lightflow.to_dict()
  raise TypeError(f"Unsupported Lightflow type: {type(lightflow).__name__}")


workflow_to_dict = lightflow_to_dict


def passport_to_dict(
    passport: Optional[schema.Passport | dict[str, Any]],
) -> Optional[dict[str, Any]]:
  """Converts a Passport object or proto to a JSON-serializable dictionary."""
  if not passport:
    return None
  if isinstance(passport, dict):
    return passport
  if hasattr(passport, "to_dict"):
    return passport.to_dict()
  raise TypeError(f"Unsupported Passport type: {type(passport).__name__}")


def generate_visualizer_html(
    lightflow: Optional[schema.Lightflow | dict[str, Any]] = None,
    passport: Optional[schema.Passport | dict[str, Any]] = None,
    title: Optional[str] = None,
    standalone: bool = True,
    workflow: Optional[schema.Lightflow | dict[str, Any]] = None,
) -> str:
  """Renders a self-contained HTML visualizer for a Lightflow."""
  target_lf = lightflow or workflow
  if not target_lf:
    raise ValueError("A Lightflow manifest must be provided.")
  wf_data = lightflow_to_dict(target_lf)
  pp_data = passport_to_dict(passport)

  wf_json = json.dumps(wf_data).replace("<", "\\u003c")
  pp_json = json.dumps(pp_data).replace("<", "\\u003c")
  wf_name = (
      wf_data.get("name", "Lightflow")
      if isinstance(wf_data, dict)
      else getattr(target_lf, "name", "Lightflow")
  )
  display_title = html.escape(title or wf_name or "Lightflow")

  bg_class = (
      "bg-[var(--app-background,#0f172a)]" if standalone else "bg-transparent"
  )

  template = _load_template()
  rendered = template.replace("__TITLE__", display_title)
  rendered = rendered.replace("__BG_CLASS__", bg_class)
  rendered = rendered.replace("__WORKFLOW_DATA__", wf_json)
  rendered = rendered.replace("__PASSPORT_DATA__", pp_json)
  return rendered
