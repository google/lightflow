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

"""Textproto, YAML fallback, and JSON Schema parsers for Lightflow OSS."""

from __future__ import annotations

import json
import re
from typing import Any, Iterator, Optional


def _unwrap_proto_struct_fields(raw: Any) -> Any:
  """Converts protobuf-style Struct `{fields: {key: ..., value: ...}}` to dict."""
  if not isinstance(raw, dict):
    return raw
  if "fields" in raw and len(raw) == 1:
    fields_val = raw["fields"]
    if isinstance(fields_val, list):
      out = {}
      for entry in fields_val:
        if isinstance(entry, dict) and "key" in entry and "value" in entry:
          out[str(entry["key"])] = _unwrap_proto_value(entry["value"])
      return out
    elif isinstance(fields_val, dict):
      if "key" in fields_val and "value" in fields_val:
        return {
            str(fields_val["key"]): _unwrap_proto_value(fields_val["value"])
        }
      return {str(k): _unwrap_proto_value(v) for k, v in fields_val.items()}
  return {k: _unwrap_proto_struct_fields(v) for k, v in raw.items()}


def _unwrap_proto_value(val: Any) -> Any:
  """Extracts scalar/list/struct values from Protobuf Value representations."""
  if not isinstance(val, dict):
    return val
  if "number_value" in val:
    num = val["number_value"]
    if isinstance(num, float) and num.is_integer():
      return int(num)
    return num
  if "string_value" in val:
    return str(val["string_value"])
  if "bool_value" in val:
    return bool(val["bool_value"])
  if "null_value" in val:
    return None
  if "struct_value" in val:
    return _unwrap_proto_struct_fields(val["struct_value"])
  if "list_value" in val:
    lv = val["list_value"]
    if isinstance(lv, dict) and "values" in lv:
      vals = lv["values"]
      if not isinstance(vals, list):
        vals = [vals]
      return [_unwrap_proto_value(x) for x in vals]
  return _unwrap_proto_struct_fields(val)


_TOKEN_RE = re.compile(
    r"""
    \s*(?:
      (\#[^\n]*)                        | # 1: Comment
      ("(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*') | # 2: Quoted string
      ([{}:\[\],])                      | # 3: Punctuation
      ([^\s{}:\[\],#"'`]+)                # 4: Identifier / Number
    )
    """,
    re.VERBOSE,
)


def _tokenize_textproto(text: str) -> Iterator[tuple[str, str]]:
  """Yields (kind, token) pairs from a textproto string."""
  pos = 0
  length = len(text)
  while pos < length:
    m = _TOKEN_RE.match(text, pos)
    if not m:
      if text[pos:].strip():
        raise ValueError(f"Unexpected textproto character at pos {pos}")
      break
    pos = m.end()
    comment, quoted, punct, ident = m.groups()
    if comment:
      continue
    if quoted is not None:
      yield ("STRING", _unescape_proto_string(quoted[1:-1]))
    elif punct is not None:
      yield ("PUNCT", punct)
    elif ident is not None:
      yield ("IDENT", ident)


_UNESCAPE_MAP = {
    '"': '"',
    "'": "'",
    "n": "\n",
    "t": "\t",
    "r": "\r",
    "\\": "\\",
}


def _unescape_proto_string(s: str) -> str:
  """Unescapes C/textproto escape sequences in a single pass."""
  return re.sub(
      r'\\(["\'ntr\\])',
      lambda m: _UNESCAPE_MAP.get(m.group(1), m.group(0)),
      s,
  )


_REPEATED_KEYS = frozenset(
    {"actions", "stages", "run_after", "fields", "values"}
)


def parse_textproto_to_dict(text: str) -> dict[str, Any]:
  """Parses a Lightflow `.textproto` manifest into a nested Python dictionary."""
  tokens = list(_tokenize_textproto(text))
  idx = 0
  n = len(tokens)

  def parse_scalar(kind: str, val: str) -> Any:
    nonlocal idx
    if kind == "STRING":
      parts = [val]
      while idx < n and tokens[idx][0] == "STRING":
        parts.append(tokens[idx][1])
        idx += 1
      return "".join(parts)
    if val in ("true", "True"):
      return True
    if val in ("false", "False"):
      return False
    if val in ("null", "None"):
      return None
    try:
      if "." in val or "e" in val.lower():
        return float(val)
      return int(val)
    except ValueError:
      return val

  def parse_block(stop_punct: Optional[str] = None) -> dict[str, Any]:
    nonlocal idx
    result: dict[str, Any] = {}
    while idx < n:
      kind, tok = tokens[idx]
      if stop_punct and kind == "PUNCT" and tok == stop_punct:
        idx += 1
        break
      if kind == "PUNCT" and tok in (",", ";"):
        idx += 1
        continue
      if kind != "IDENT":
        raise ValueError(f"Expected field identifier in textproto, got {tok!r}")
      key = tok
      idx += 1
      if idx >= n:
        break

      has_colon = False
      if tokens[idx] == ("PUNCT", ":"):
        has_colon = True
        idx += 1

      if idx < n and tokens[idx] == ("PUNCT", "{"):
        idx += 1
        val = parse_block("}")
      elif idx < n and tokens[idx] == ("PUNCT", "["):
        idx += 1
        items = []
        while idx < n and tokens[idx] != ("PUNCT", "]"):
          if tokens[idx] == ("PUNCT", ","):
            idx += 1
            continue
          if tokens[idx] == ("PUNCT", "{"):
            idx += 1
            items.append(parse_block("}"))
          else:
            k_item, v_item = tokens[idx]
            idx += 1
            items.append(parse_scalar(k_item, v_item))
        if idx < n and tokens[idx] == ("PUNCT", "]"):
          idx += 1
        val = items
      elif has_colon and idx < n:
        k_val, v_val = tokens[idx]
        idx += 1
        val = parse_scalar(k_val, v_val)
      else:
        raise ValueError(f"Unexpected token after field {key!r} in textproto")

      if key in _REPEATED_KEYS:
        existing = result.setdefault(key, [])
        if isinstance(val, list):
          existing.extend(val)
        else:
          existing.append(val)
      elif key in result:
        if not isinstance(result[key], list):
          result[key] = [result[key]]
        result[key].append(val)
      else:
        result[key] = val

    return result

  return parse_block(None)


def _strip_yaml_comment_outside_quotes(line: str) -> str:
  """Strips `#` comments only when outside single or double quotes."""
  in_single = False
  in_double = False
  escaped = False
  for i, ch in enumerate(line):
    if escaped:
      escaped = False
      continue
    if ch == "\\":
      escaped = True
      continue
    if ch == "'" and not in_double:
      in_single = not in_single
    elif ch == '"' and not in_single:
      in_double = not in_double
    elif ch == "#" and not in_single and not in_double:
      if i == 0 or line[i - 1].isspace():
        return line[:i].rstrip()
  return line


def _parse_yaml_scalar(val_str: str) -> Any:
  """Parses a YAML scalar value using pure Python stdlib."""
  s = val_str.strip()
  if not s:
    return ""
  if (s.startswith('"') and s.endswith('"')) or (
      s.startswith("'") and s.endswith("'")
  ):
    return s[1:-1]
  if s[0] in "[{" and s[-1] != {"[": "]", "{": "}"}[s[0]]:
    raise ValueError(f"unterminated flow collection: {s!r}")
  if s.startswith("[") and s.endswith("]"):
    inner = s[1:-1].strip()
    if not inner:
      return []
    return [_parse_yaml_scalar(x) for x in inner.split(",")]
  if s.startswith("{") and s.endswith("}"):
    try:
      return json.loads(s)
    except ValueError as e:
      raise ValueError(
          f"flow mapping {s!r} is not valid JSON; the built-in parser only"
          " supports JSON-style flow mappings"
      ) from e
  if s[0] in "&*!":
    raise ValueError(
        f"{s!r}: YAML anchors, aliases and tags are not supported by the"
        " built-in parser"
    )
  if s in ("true", "True"):
    return True
  if s in ("false", "False"):
    return False
  if s in ("null", "None", "~"):
    return None
  try:
    if "." in s:
      return float(s)
    return int(s)
  except ValueError:
    return s


def _parse_simple_yaml(text: str) -> dict[str, Any]:
  """Best-effort YAML subset parser used only when PyYAML is not installed.

  PyYAML (`yaml.safe_load`) is the supported parser. This fallback covers the
  subset used by Lightflow manifests: nested mappings, block lists, quoted and
  plain scalars, `#` comments, `|`/`>` block scalars and JSON-style flow
  collections. Known gaps: blank lines inside block scalars are dropped and
  relative indentation inside them is not preserved. Anything else
  (anchors/aliases, tags, YAML-style flow mappings, multi-document streams,
  multi-line plain scalars) raises rather than being guessed at.

  Args:
    text: The YAML document.

  Returns:
    The top-level mapping as a dict.

  Raises:
    ValueError: If the document uses syntax outside the supported subset or
      is not a mapping at the top level.
  """
  lines: list[tuple[int, str]] = []
  doc_starts = 0
  doc_ended = False
  block_scalar_indent: Optional[int] = None
  for lineno, raw in enumerate(text.splitlines(), start=1):
    if block_scalar_indent is not None:
      raw_stripped = raw.strip()
      if not raw_stripped:
        continue
      raw_indent = len(raw) - len(raw.lstrip(" "))
      if raw_indent > block_scalar_indent:
        lines.append((raw_indent, raw_stripped))
        continue
      block_scalar_indent = None

    stripped_comment = _strip_yaml_comment_outside_quotes(raw)
    content = stripped_comment.strip()
    if not content:
      continue
    if doc_ended:
      raise ValueError(
          f"line {lineno}: multi-document YAML streams are not supported"
          " by the built-in parser"
      )
    if content == "---":
      doc_starts += 1
      if doc_starts > 1 or lines:
        raise ValueError(
            f"line {lineno}: multi-document YAML streams are not supported"
            " by the built-in parser"
        )
      continue
    if content == "...":
      doc_ended = True
      continue
    indent = len(stripped_comment) - len(stripped_comment.lstrip(" "))
    if ":" in content:
      _, _, rhs = content.partition(":")
      if rhs.strip() in (">-", ">", "|-", "|"):
        block_scalar_indent = indent
    lines.append((indent, content))

  idx = 0
  n = len(lines)

  def parse_block_scalar(parent_indent: int, indicator: str) -> str:
    nonlocal idx
    block_lines: list[str] = []
    while idx < n and lines[idx][0] > parent_indent:
      block_lines.append(lines[idx][1])
      idx += 1
    joiner = " " if indicator.startswith(">") else "\n"
    return joiner.join(block_lines).strip()

  def parse_value(parent_indent: int, raw_val: str) -> Any:
    vs = raw_val.strip()
    if vs in (">-", ">", "|-", "|"):
      return parse_block_scalar(parent_indent, vs)
    if vs:
      return _parse_yaml_scalar(vs)
    return parse_node(parent_indent + 1, allow_same_indent_list=True)

  def parse_node(min_indent: int, allow_same_indent_list: bool = False) -> Any:
    nonlocal idx
    if idx >= n:
      return {}
    first_indent, first_content = lines[idx]
    if first_indent < min_indent and not (
        allow_same_indent_list
        and first_indent == min_indent - 1
        and first_content.startswith("- ")
    ):
      return {}

    if first_content.startswith("- "):
      arr = []
      while (
          idx < n
          and lines[idx][0] == first_indent
          and lines[idx][1].startswith("- ")
      ):
        item_str = lines[idx][1][2:].strip()
        idx += 1
        if not item_str:
          arr.append(parse_node(first_indent + 1))
        elif ":" in item_str and not (
            item_str.startswith('"') or item_str.startswith("'")
        ):
          k, _, v = item_str.partition(":")
          item_dict: dict[str, Any] = {}
          item_dict[k.strip()] = parse_value(first_indent, v)
          while (
              idx < n
              and lines[idx][0] > first_indent
              and not lines[idx][1].startswith("- ")
          ):
            sub_indent, sub_line = lines[idx]
            sk, sep, sv = sub_line.partition(":")
            if not sep:
              raise ValueError(
                  f"expected 'key: value', got {sub_line!r} (multi-line"
                  " plain scalars are not supported by the built-in parser)"
              )
            idx += 1
            item_dict[sk.strip()] = parse_value(sub_indent, sv)
          arr.append(item_dict)
        else:
          arr.append(_parse_yaml_scalar(item_str))
      return arr

    obj: dict[str, Any] = {}
    while (
        idx < n
        and lines[idx][0] == first_indent
        and not lines[idx][1].startswith("- ")
    ):
      _, content = lines[idx]
      k, sep, v = content.partition(":")
      if not sep:
        raise ValueError(
            f"expected 'key: value', got {content!r} (multi-line plain"
            " scalars are not supported by the built-in parser)"
        )
      idx += 1
      key = k.strip()
      obj[key] = parse_value(first_indent, v)
    return obj

  res = parse_node(0)
  if idx < n:
    raise ValueError(
        f"could not parse line {lines[idx][1]!r} (unexpected indentation)"
    )
  if not isinstance(res, dict):
    raise ValueError("Lightflow YAML manifest must be a mapping at top level.")
  return res


_STDLIB_JSON_SCHEMA_TYPES = frozenset({
    "object",
    "array",
    "string",
    "number",
    "integer",
    "boolean",
    "null",
})

_STDLIB_JSON_SCHEMA_KEYWORDS = frozenset({
    "type",
    "enum",
    "const",
    "required",
    "properties",
    "additionalProperties",
    "items",
    "minimum",
    "maximum",
    "minLength",
    "maxLength",
    "minItems",
    "maxItems",
    "title",
    "description",
    "default",
    "examples",
    "$schema",
    "$id",
    "$comment",
})


def _check_json_schema_stdlib(
    schema_dict: dict[str, Any], path: str = ""
) -> None:
  """Validates a JSON Schema dict for the stdlib fallback validator."""
  loc = f" at '{path}'" if path else ""
  if not isinstance(schema_dict, dict):
    raise ValueError(f"JSON Schema{loc} must be a dictionary.")
  unsupported = set(schema_dict.keys()) - _STDLIB_JSON_SCHEMA_KEYWORDS
  if unsupported:
    raise ValueError(
        f"Unsupported JSON Schema keyword(s) in stdlib fallback{loc}:"
        f" {sorted(unsupported)}. Install 'jsonschema' for full Draft 7"
        " validation."
    )
  if "type" in schema_dict:
    t = schema_dict["type"]
    types = [t] if isinstance(t, str) else t if isinstance(t, list) else None
    if not types or any(
        not isinstance(x, str) or x not in _STDLIB_JSON_SCHEMA_TYPES
        for x in types
    ):
      raise ValueError(f"Invalid JSON Schema type{loc}: {t!r}")
  if "enum" in schema_dict and (
      not isinstance(schema_dict["enum"], list) or not schema_dict["enum"]
  ):
    raise ValueError(f"'enum'{loc} must be a non-empty list.")
  if "required" in schema_dict:
    req = schema_dict["required"]
    if not isinstance(req, list) or any(not isinstance(k, str) for k in req):
      raise ValueError(f"'required'{loc} must be a list of strings.")
  for num_kw in (
      "minimum",
      "maximum",
      "minLength",
      "maxLength",
      "minItems",
      "maxItems",
  ):
    if num_kw in schema_dict:
      bound = schema_dict[num_kw]
      if not isinstance(bound, (int, float)) or isinstance(bound, bool):
        raise ValueError(f"'{num_kw}'{loc} must be a number.")
  if "properties" in schema_dict:
    props = schema_dict["properties"]
    if not isinstance(props, dict):
      raise ValueError(f"'properties'{loc} must be a dictionary.")
    for prop_key, prop_schema in props.items():
      sub_path = f"{path}.{prop_key}" if path else str(prop_key)
      _check_json_schema_stdlib(prop_schema, sub_path)
  if "additionalProperties" in schema_dict:
    addl = schema_dict["additionalProperties"]
    if isinstance(addl, dict):
      sub_path = (
          f"{path}.additionalProperties" if path else "additionalProperties"
      )
      _check_json_schema_stdlib(addl, sub_path)
    elif not isinstance(addl, bool):
      raise ValueError(
          f"'additionalProperties'{loc} must be a boolean or dictionary."
      )
  if "items" in schema_dict:
    items_schema = schema_dict["items"]
    if not isinstance(items_schema, dict):
      raise ValueError(
          f"'items'{loc} must be a schema dictionary in stdlib fallback."
          " Install 'jsonschema' for tuple validation."
      )
    sub_path = f"{path}.items" if path else "items"
    _check_json_schema_stdlib(items_schema, sub_path)


def check_json_schema(schema_dict: dict[str, Any]) -> None:
  """Validates that a JSON schema dictionary is well-formed."""
  try:
    import jsonschema as _js  # pylint: disable=g-import-not-at-top

    _js.Draft7Validator.check_schema(schema_dict)
    return
  except ImportError:
    pass
  _check_json_schema_stdlib(schema_dict)


def _matches_json_type(val: Any, expected_type: str) -> bool:
  """Returns True if `val` matches the JSON Schema primitive/container type."""
  if expected_type == "object":
    return isinstance(val, dict)
  if expected_type == "array":
    return isinstance(val, list)
  if expected_type == "string":
    return isinstance(val, str)
  if expected_type == "boolean":
    return isinstance(val, bool)
  if expected_type == "integer":
    return isinstance(val, int) and not isinstance(val, bool)
  if expected_type == "number":
    return isinstance(val, (int, float)) and not isinstance(val, bool)
  if expected_type == "null":
    return val is None
  return True


def _validate_json_schema_stdlib(
    instance: Any, schema_dict: dict[str, Any], path: str = ""
) -> None:
  """Stdlib fallback validator for JSON Schema when `jsonschema` is absent."""
  if not path:
    _check_json_schema_stdlib(schema_dict)
  loc = f" at '{path}'" if path else ""
  expected_type = schema_dict.get("type")
  if isinstance(expected_type, str):
    if not _matches_json_type(instance, expected_type):
      raise ValueError(
          f"Expected {expected_type}{loc}, got {type(instance).__name__}"
      )
  elif isinstance(expected_type, list):
    if not any(_matches_json_type(instance, t) for t in expected_type):
      raise ValueError(
          f"Expected one of {expected_type!r}{loc}, got"
          f" {type(instance).__name__}"
      )
  if "const" in schema_dict and instance != schema_dict["const"]:
    raise ValueError(
        f"Expected const {schema_dict['const']!r}{loc}, got {instance!r}"
    )
  if "enum" in schema_dict and isinstance(schema_dict["enum"], list):
    if instance not in schema_dict["enum"]:
      raise ValueError(
          f"{instance!r} is not one of {schema_dict['enum']!r}{loc}"
      )
  if isinstance(instance, (int, float)) and not isinstance(instance, bool):
    if "minimum" in schema_dict and instance < schema_dict["minimum"]:
      raise ValueError(
          f"{instance!r} is less than minimum {schema_dict['minimum']!r}{loc}"
      )
    if "maximum" in schema_dict and instance > schema_dict["maximum"]:
      raise ValueError(
          f"{instance!r} is greater than maximum"
          f" {schema_dict['maximum']!r}{loc}"
      )
  if isinstance(instance, str):
    if "minLength" in schema_dict and len(instance) < schema_dict["minLength"]:
      raise ValueError(
          f"String length {len(instance)} is less than minLength"
          f" {schema_dict['minLength']}{loc}"
      )
    if "maxLength" in schema_dict and len(instance) > schema_dict["maxLength"]:
      raise ValueError(
          f"String length {len(instance)} is greater than maxLength"
          f" {schema_dict['maxLength']}{loc}"
      )
  if isinstance(instance, dict):
    for req_key in schema_dict.get("required", []):
      if req_key not in instance:
        raise ValueError(f"'{req_key}' is a required property{loc}")
    props = schema_dict.get("properties")
    prop_keys = set(props.keys()) if isinstance(props, dict) else set()
    if isinstance(props, dict):
      for prop_key, prop_schema in props.items():
        if prop_key in instance and isinstance(prop_schema, dict):
          sub_path = f"{path}.{prop_key}" if path else prop_key
          _validate_json_schema_stdlib(
              instance[prop_key], prop_schema, sub_path
          )
    addl = schema_dict.get("additionalProperties", True)
    if isinstance(addl, bool) and not addl:
      extra = sorted(set(instance.keys()) - prop_keys)
      if extra:
        raise ValueError(
            f"Additional properties are not allowed ({extra!r} were"
            f" unexpected){loc}"
        )
    elif isinstance(addl, dict):
      for k in set(instance.keys()) - prop_keys:
        sub_path = f"{path}.{k}" if path else k
        _validate_json_schema_stdlib(instance[k], addl, sub_path)
  elif isinstance(instance, list):
    if "minItems" in schema_dict and len(instance) < schema_dict["minItems"]:
      raise ValueError(
          f"Array length {len(instance)} is less than minItems"
          f" {schema_dict['minItems']}{loc}"
      )
    if "maxItems" in schema_dict and len(instance) > schema_dict["maxItems"]:
      raise ValueError(
          f"Array length {len(instance)} is greater than maxItems"
          f" {schema_dict['maxItems']}{loc}"
      )
    items_schema = schema_dict.get("items")
    if isinstance(items_schema, dict):
      for idx, item in enumerate(instance):
        sub_path = f"{path}[{idx}]" if path else f"[{idx}]"
        _validate_json_schema_stdlib(item, items_schema, sub_path)


def validate_json_schema(instance: Any, schema_dict: dict[str, Any]) -> None:
  """Validates an instance against a JSON schema dictionary."""
  try:
    import jsonschema as _js  # pylint: disable=g-import-not-at-top

    _js.validate(instance=instance, schema=schema_dict)
    return
  except ImportError:
    pass
  _validate_json_schema_stdlib(instance, schema_dict)
