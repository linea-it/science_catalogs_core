"""Output helpers (file naming and writing)."""

import hashlib
import os
import re
import warnings
from typing import Any

import dask
import pandas as pd
from dask import delayed
from dask import dataframe as dd

_fname_safe_re = re.compile(r"[^A-Za-z0-9._-]+")


def _sanitize_token(token: str) -> str:
    token = str(token).strip()
    if token == "":
        return "empty"
    return _fname_safe_re.sub("_", token)


def _build_values_tag(values, col_name: str, max_list: int = 12) -> str:
    vals = [v for v in values if v is not None and not (isinstance(v, float) and pd.isna(v))]
    if not vals:
        return f"{col_name}_none"
    vals = sorted(map(_sanitize_token, vals))
    if len(vals) <= max_list:
        return f"{col_name}_" + "_".join(vals)
    head = "_".join(vals[:max_list])
    h = hashlib.sha1(",".join(vals).encode("utf-8")).hexdigest()[:8]
    return f"{col_name}_{head}_plus{len(vals)-max_list}_{h}"


@delayed
def _write_part(
    pdf: pd.DataFrame,
    base_dir: str,
    base_suffix: str,
    i: int,
    ext: str,
    col_for_name: str | None,
):
    tag = None
    if col_for_name is not None and col_for_name in pdf.columns:
        uniques = pd.unique(pdf[col_for_name])
        tag = _build_values_tag(uniques, _sanitize_token(col_for_name))

    fname = f"{base_suffix}_part{i}.{ext}" if not tag else f"{base_suffix}_part{i}_{tag}.{ext}"
    out_path = os.path.join(base_dir, fname)

    if ext == "parquet":
        pdf.to_parquet(out_path, index=False)
    elif ext == "csv":
        pdf.to_csv(out_path, index=False)
    elif ext == "h5":
        import tables_io

        pdf = pdf.reset_index(drop=True)
        tables_io.write(pdf, out_path)
    else:
        raise ValueError(f"Unsupported extension {ext}")

    return out_path


def write_partitions(ddf_out: dd.DataFrame, output_cfg: dict[str, Any], data_dir: str, suffix: str):
    ext_map = {"parquet": "parquet", "csv": "csv", "hdf5": "h5"}
    ext = ext_map[output_cfg.get("save_as", "parquet")]

    col_for_name = output_cfg.get("col_for_filename")
    if col_for_name is not None and col_for_name not in ddf_out.columns:
        warnings.warn(f"col_for_filename='{col_for_name}' not found; ignoring")
        col_for_name = None

    delayed_parts = ddf_out.to_delayed()
    tasks = [
        _write_part(part, data_dir, suffix, i, ext, col_for_name)
        for i, part in enumerate(delayed_parts)
    ]
    written_paths = dask.compute(*tasks)
    return written_paths


__all__ = ["write_partitions"]
