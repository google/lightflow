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

"""Launcher script for the Lightflow OSS workflow engine."""

from __future__ import annotations

import ast
import dataclasses
import functools
import inspect
import json
import sys
from typing import Any

# pylint: disable=g-import-not-at-top,g-bad-import-order
try:
  from . import engine
  from . import lib
except ImportError:
  from lightflow import engine  # pyrefly: ignore[missing-import]
  from lightflow import lib  # pyrefly: ignore[missing-import]
# pylint: enable=g-import-not-at-top,g-bad-import-order

_VERSION = "0.1.0"


def _parse_arg(val: Any) -> Any:
  """Deserializes string arguments into JSON dicts/lists or native booleans."""
  if not isinstance(val, str):
    return val

  if val.lower() == "true":
    return True
  if val.lower() == "false":
    return False

  val_stripped = val.strip()
  if (val_stripped.startswith("{") and val_stripped.endswith("}")) or (
      val_stripped.startswith("[") and val_stripped.endswith("]")
  ):
    try:
      return json.loads(val_stripped)
    except json.JSONDecodeError:
      try:
        return ast.literal_eval(val_stripped)
      except (ValueError, SyntaxError):
        pass
  return val


def _json_default(obj: Any) -> Any:
  if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
    return dataclasses.asdict(obj)
  if isinstance(obj, (set, frozenset)):
    return sorted(list(obj))
  return str(obj)


def _format_result(res: Any, as_json: bool = False) -> str:
  if (
      as_json
      or isinstance(res, (dict, list, set, frozenset))
      or dataclasses.is_dataclass(res)
  ):
    try:
      return json.dumps(res, indent=2, default=_json_default)
    except (TypeError, ValueError):
      return str(res)
  return str(res)


class CliWrapper:
  """Wraps a CLI object to parse JSON/boolean flags and print return values."""

  def __init__(self, client: Any):
    self._client = client
    if hasattr(client, "__doc__") and client.__doc__:
      self.__doc__ = client.__doc__

  def __dir__(self) -> list[str]:
    attrs = []
    for name in dir(self._client):
      if name.startswith("_"):
        continue
      try:
        val = getattr(self._client, name)
        if callable(val) or (
            hasattr(val, "__dict__")
            and not isinstance(
                val, (int, str, float, bool, list, dict, set, tuple)
            )
        ):
          attrs.append(name)
      except Exception:  # pylint: disable=broad-exception-caught
        pass
    return attrs

  def __getattr__(self, name: str) -> Any:
    attr = getattr(self._client, name)
    if callable(attr):
      sig = None
      try:
        sig = inspect.signature(attr)
      except (ValueError, TypeError):
        pass

      @functools.wraps(attr)
      def wrapper(*args: Any, **kwargs: Any) -> Any:
        as_json = False
        if sig is not None:
          if "json" not in sig.parameters and "json" in kwargs:
            as_json = bool(kwargs.pop("json"))
          if "format" not in sig.parameters and kwargs.get("format") == "json":
            kwargs.pop("format")
            as_json = True
        else:
          if "json" in kwargs:
            as_json = bool(kwargs.pop("json"))
          if kwargs.get("format") == "json":
            kwargs.pop("format")
            as_json = True

        parsed_args = [_parse_arg(arg) for arg in args]
        parsed_kwargs = {k: _parse_arg(v) for k, v in kwargs.items()}
        res = attr(*parsed_args, **parsed_kwargs)
        if res is not None:
          print(_format_result(res, as_json=as_json))
        return None

      if sig is not None:
        try:
          setattr(wrapper, "__signature__", sig)
        except (ValueError, TypeError):
          pass
      return wrapper

    if hasattr(attr, "__dict__") and not isinstance(
        attr, (int, str, float, bool, list, dict, set, tuple)
    ):
      return CliWrapper(attr)

    return attr


def _is_balanced_json_like(text: str) -> bool:
  """Returns True once opening '{'/'[' delimiters are balanced outside quotes."""
  s = text.strip()
  if not s or s[0] not in ("{", "["):
    return True
  depth = 0
  in_str = False
  quote_char = ""
  escape = False
  for ch in s:
    if escape:
      escape = False
      continue
    if ch == "\\":
      escape = True
      continue
    if in_str:
      if ch == quote_char:
        in_str = False
      continue
    if ch in ('"', "'"):
      in_str = True
      quote_char = ch
    elif ch in ("{", "["):
      depth += 1
    elif ch in ("}", "]"):
      depth -= 1
  return depth <= 0 and s[-1] in ("}", "]")


def _dispatch_argv(wrapper: CliWrapper, argv: list[str]) -> None:
  """Dispatches command-line arguments to a CLI command."""
  commands = sorted(dir(wrapper))
  if argv and argv[0] in ("-V", "--version", "version"):
    print(f"lightflow {_VERSION}")
    sys.exit(0)
  if not argv or argv[0] in ("-h", "--help", "help"):
    print(
        f"Usage: lightflow <{'|'.join(commands)}> [--key=value ...]"
        "  (also: --help, --version)"
    )
    client_doc = getattr(getattr(wrapper, "_client", None), "__doc__", "")
    if client_doc:
      print(f"\n{client_doc.strip()}")
    sys.exit(0)

  cmd_name = argv[0].replace("-", "_")
  if cmd_name not in commands:
    raise ValueError(
        f"Unknown command '{argv[0]}'. Available commands: {commands}"
    )

  target = getattr(wrapper, cmd_name)
  pos_args: list[Any] = []
  kw_args: dict[str, Any] = {}
  i = 1
  while i < len(argv):
    tok = argv[i]
    if tok in ("-h", "--help"):
      doc = getattr(target, "__doc__", "") or ""
      sig = getattr(target, "__signature__", "")
      print(f"lightflow {cmd_name}{sig}\n\n{doc}".strip())
      sys.exit(0)
    if tok.startswith("--"):
      body = tok[2:]
      if "=" in body:
        k, v = body.split("=", 1)
        norm_k = k.replace("-", "_")
        if (
            norm_k == "payload"
            and v.lstrip().startswith(("{", "["))
            and not _is_balanced_json_like(v)
        ):
          while (
              i + 1 < len(argv)
              and not argv[i + 1].startswith("--")
              and not _is_balanced_json_like(v)
          ):
            i += 1
            v = f"{v} {argv[i]}"
        kw_args[norm_k] = v
      elif body.startswith("no-") or body.startswith("no_"):
        kw_args[body[3:].replace("-", "_")] = False
      elif i + 1 < len(argv) and not argv[i + 1].startswith("--"):
        norm_k = body.replace("-", "_")
        v = argv[i + 1]
        i += 1
        if (
            norm_k == "payload"
            and v.lstrip().startswith(("{", "["))
            and not _is_balanced_json_like(v)
        ):
          while (
              i + 1 < len(argv)
              and not argv[i + 1].startswith("--")
              and not _is_balanced_json_like(v)
          ):
            i += 1
            v = f"{v} {argv[i]}"
        kw_args[norm_k] = v
      else:
        kw_args[body.replace("-", "_")] = True
    else:
      pos_args.append(tok)
    i += 1

  if "payload_file" in kw_args:
    pf = kw_args.pop("payload_file")
    if not isinstance(pf, str) or not pf.strip():
      raise ValueError("--payload_file requires a file path.")
    if "payload" in kw_args:
      raise ValueError("Cannot specify both --payload and --payload_file.")
    kw_args["payload"] = f"@{pf}"

  if pos_args and ("lightflow" in kw_args or "workflow" in kw_args):
    raise ValueError(
        f"Unexpected positional argument(s): {pos_args}. If passing JSON via"
        " --payload on Windows PowerShell, quote stripping may have split your"
        " JSON across arguments; pass a file via --payload=@payload.json"
        " instead."
    )

  target(*pos_args, **kw_args)


def run_cli() -> None:
  """Runs the Lightflow CLI."""
  lib._ensure_utf8_stdio()  # pylint: disable=protected-access
  try:
    _dispatch_argv(CliWrapper(lib.LightflowRunnerCLI()), sys.argv[1:])
  except (
      engine.OperatorActionSuspended,
      lib.LightflowAlreadyPausedError,
  ) as e:
    print(f"\n{e}")
    sys.exit(lib.EXIT_SUSPENDED)
  except lib.LightflowAlreadyCompletedError as e:
    print(f"\n{e}")
    sys.exit(0)
  except lib.LightflowRunFailedError as e:
    print(f"\n{e}")
    sys.exit(lib.EXIT_FAILED)
  except lib.LightflowStateNotFoundError as e:
    print(f"\n{e}")
    sys.exit(lib.EXIT_NO_STATE)
  except lib.LightflowRunningError as e:
    print(f"\n{e}")
    sys.exit(lib.EXIT_RUNNING)
  except Exception as e:  # pylint: disable=broad-except
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(lib.EXIT_FAILED)
  finally:
    engine.wait_for_notifications(timeout=3.0)


if __name__ == "__main__":
  run_cli()
