"""Load, render and lightly validate test metadata YAML files.

Templating follows Airflow conventions so the same file works in both places:
  {{ params.test_run_id }}  -> DAG params (runtime) or the params dict (local)
  {{ var.value.gcs_bucket }} -> Airflow Variables (runtime) or the variables dict (local)

Unresolvable placeholders are left intact so a file can be loaded at DAG
parse time (params unknown) and rendered again at task runtime.
"""
from __future__ import annotations

import re
from typing import Any, Dict, Optional

import yaml

_TEMPLATE_RE = re.compile(r"\{\{\s*(params|var\.value)\.([A-Za-z0-9_]+)\s*\}\}")


def render_templates(obj: Any, params: Dict[str, Any], variables: Dict[str, Any]) -> Any:
    if isinstance(obj, str):
        def _repl(m: re.Match) -> str:
            kind, key = m.group(1), m.group(2)
            lookup = params if kind == "params" else variables
            return str(lookup[key]) if key in lookup else m.group(0)

        return _TEMPLATE_RE.sub(_repl, obj)
    if isinstance(obj, dict):
        return {k: render_templates(v, params, variables) for k, v in obj.items()}
    if isinstance(obj, list):
        return [render_templates(v, params, variables) for v in obj]
    return obj


def validate_metadata(meta: Dict[str, Any]) -> None:
    missing = [k for k in ("version", "pipeline", "test_data", "test_cases") if k not in meta]
    if missing:
        raise ValueError(f"metadata missing required keys: {missing}")
    pipe = meta["pipeline"]
    for k in ("name", "gcs", "dataflow", "postgres"):
        if k not in pipe:
            raise ValueError(f"metadata.pipeline missing required key: {k!r}")
    for tc in meta["test_cases"]:
        for k in ("id", "name", "type"):
            if k not in tc:
                raise ValueError(f"test case missing {k!r}: {tc}")


def load_metadata(
    path: str,
    params: Optional[Dict[str, Any]] = None,
    variables: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        meta = yaml.safe_load(f)
    meta = render_templates(meta, params or {}, variables or {})
    validate_metadata(meta)
    return meta
