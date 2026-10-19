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

"""Notification dispatch and audit-log formatting helpers for Lightflow."""

from __future__ import annotations

import os
import shutil
import subprocess
import threading
import time
from typing import Callable

_SUMMARY_MAX_CHARS = 140


def summarise(text: str) -> str:
  """Reduces multi-line operator text to a single audit-log line.

  Args:
    text: The full text.

  Returns:
    The first non-empty line, truncated, with an ellipsis if anything was
    dropped. Empty for empty input.
  """
  stripped = text.strip()
  if not stripped:
    return ""

  first, _, rest = stripped.partition("\n")
  first = first.strip()
  overlong = len(first) > _SUMMARY_MAX_CHARS
  if overlong:
    first = first[:_SUMMARY_MAX_CHARS].rstrip()
  return first + ("..." if overlong or rest.strip() else "")


_NOTIFICATION_THREADS: list[threading.Thread] = []
_NOTIFICATION_HANDLERS: list[Callable[[str, str], None]] = []


def register_notification_handler(
    handler: Callable[[str, str], None],
) -> None:
  """Registers a callback `(title, content)` invoked on stage notifications."""
  if handler not in _NOTIFICATION_HANDLERS:
    _NOTIFICATION_HANDLERS.append(handler)


def clear_notification_handlers() -> None:
  """Removes all registered in-process notification callbacks."""
  _NOTIFICATION_HANDLERS.clear()


# Notification titles that are worth interrupting an agent for. Everything
# else ("Stage Entered", "Stage Completed", "Stage Skipped") is already on
# stdout and, pushed one by one through `agentapi` (~0.6s each, serialized),
# made a two-stage run take 3s instead of 0.15s and delivered five
# high-priority messages for two stages.
_MILESTONE_TITLE_PREFIXES = (
    "Operator Action Suspended",
    "Stage Waiting",
    "Stage Failed",
    "Stage Rejected",
    "Rollback",
    "Lightflow ",
)


def _notification_mode() -> str:
  """Returns `off` (default), `milestones`, or `all` from LIGHTFLOW_NOTIFY."""
  mode = os.environ.get("LIGHTFLOW_NOTIFY", "off").strip().lower()
  return mode if mode in ("milestones", "all", "off") else "off"


def send_notification(title: str, content: str) -> None:
  """Sends an asynchronous push notification to Antigravity or custom hooks.

  Custom handlers registered via `register_notification_handler` receive
  every event. The `agentapi` push to the hosting agent is controlled by
  `LIGHTFLOW_NOTIFY`: `off` (default, so synchronous CLI runs do not
  double-notify a caller that already reads stdout/JSON) disables the push;
  `milestones` forwards only gate suspensions, waiting yields, failures,
  rejections, rollbacks, and the final run outcome; `all` forwards every
  stage transition.

  Args:
    title: Short event title, e.g. "Stage Failed: publish".
    content: One-line human-readable body.
  """
  for handler in list(_NOTIFICATION_HANDLERS):
    try:
      handler(title, content)
    except Exception:  # pylint: disable=broad-exception-caught
      # Notification hooks are best-effort and must not abort stage execution.
      pass

  mode = _notification_mode()
  if mode == "off":
    return
  if mode == "milestones" and not title.startswith(_MILESTONE_TITLE_PREFIXES):
    return

  conv_id = os.environ.get("ANTIGRAVITY_CONVERSATION_ID")
  if not conv_id:
    return

  if not shutil.which("agentapi"):
    return

  cmd = [
      "agentapi",
      "send-message",
      f"--title={title}",
      conv_id,
      content,
  ]

  _NOTIFICATION_THREADS[:] = [t for t in _NOTIFICATION_THREADS if t.is_alive()]
  prev_thread = _NOTIFICATION_THREADS[-1] if _NOTIFICATION_THREADS else None

  def run_cmd():
    if prev_thread is not None:
      try:
        prev_thread.join(timeout=5.0)
      except Exception:  # pylint: disable=broad-exception-caught
        pass
    try:
      subprocess.run(
          cmd,
          stdout=subprocess.DEVNULL,
          stderr=subprocess.DEVNULL,
          check=False,
      )
    except Exception:  # pylint: disable=broad-exception-caught
      pass

  # Run in a fire-and-forget daemon thread to prevent blocking and avoid
  # zombie processes
  t = threading.Thread(target=run_cmd, daemon=True)
  _NOTIFICATION_THREADS.append(t)
  t.start()


def wait_for_notifications(timeout: float = 5.0) -> None:
  """Waits for all pending notification threads to finish."""
  start_time = time.monotonic()
  for t in list(_NOTIFICATION_THREADS):
    elapsed = time.monotonic() - start_time
    remaining = timeout - elapsed
    if remaining <= 0:
      break
    t.join(timeout=remaining)
  _NOTIFICATION_THREADS.clear()
