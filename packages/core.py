"""
Núcleo reutilizável do pipeline Science Catalogs.
Portado do notebook data_prep_tsm.ipynb, removendo a etapa de validação
mas mantendo a lógica de processamento, ordenação, reparticionamento e salvamento.
"""

from __future__ import annotations

import gc
import glob
import hashlib
import math
import os
import re
import warnings
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from contextlib import nullcontext

import dask
import numpy as np
import pandas as pd
from astropy.coordinates import SkyCoord
import astropy.units as u
from astropy.io import fits
from dask import delayed
from dask import dataframe as dd
from dask.distributed import Client, performance_report, wait

# módulos locais (PYTHONPATH aponta para packages/)
from executor import get_executor
from utils import copy_file, copy_directory, create_logdir, dump_yml, load_yml, setup_logger

try:
    from dustmaps.config import config as dust_config
except Exception:  # pragma: no cover - só falha se dustmaps não instalado
    dust_config = None

DUST_QUERY_CACHE: dict[str, Any] = {}

MAG_CONV = np.log(10) * 0.4


# ---------------------------------------------------------------------------
# Helpers de config
# ---------------------------------------------------------------------------

def _as_none(v):
    if v is None:
        return None
    if isinstance(v, float) and np.isnan(v):
        return None
    if isinstance(v, str) and v.strip().lower() in ("", "none", "null", "nan", "~"):
        return None
    return v


def _as_float_or_none(v):
    v = _as_none(v)
    if v is None:
        return None
    return float(v)


# ---------------------------------------------------------------------------
# Dustmaps
# ---------------------------------------------------------------------------

def _make_dust_query(name: str):
    name = (name or "sfd").strip().lower()
    if name == "sfd":
        from dustmaps.sfd import SFDQuery
        return SFDQuery()
    if name.startswith("bayestar"):
        from dustmaps.bayestar import BayestarQuery
        return BayestarQuery()
    if name == "planck":
        from dustmaps.planck import PlanckQuery
        return PlanckQuery()
    # fallback genérico
    import importlib

    mod = importlib.import_module(f"dustmaps.{name}")
    for attr in dir(mod):
        if attr.lower().endswith("query"):
            return getattr(mod, attr)()
    raise ValueError(f"Unsupported dustmap '{name}'")


def get_dust_query(dust_cfg: dict[str, Any]):
    name = (dust_cfg.get("use_dustmap") or "sfd").strip().lower()
    if name in DUST_QUERY_CACHE:
        return DUST_QUERY_CACHE[name]
    dq = _make_dust_query(name)
    DUST_QUERY_CACHE[name] = dq
    return dq


def configure_dustmaps_path(dust_cfg: dict[str, Any]):
    if dust_config is None:
        return
    path = dust_cfg.get("path_to_dustmaps")
    if path:
        dust_config["data_dir"] = path
        os.environ.setdefault("DUSTMAPS_PATH", path)


# ---------------------------------------------------------------------------
# Flux/mag flags e sufixo
# ---------------------------------------------------------------------------
ALLOWED_INPUT_TYPES = {"flux", "flux_dered", "mag", "mag_dered"}


def decide_suffix_and_flags(input_cfg: dict[str, Any], mag_flag: bool, dered_flag: bool) -> Tuple[str, bool, bool, bool]:
    input_col_type = input_cfg.get("input_col_type", "flux")
    if input_col_type not in ALLOWED_INPUT_TYPES:
        raise ValueError(f"Invalid input_col_type='{input_col_type}'. Allowed: {sorted(ALLOWED_INPUT_TYPES)}")

    compute_mag = bool(mag_flag)
    compute_dered = bool(dered_flag)

    will_mag = False
    will_dered_flux = False
    will_dered_mag = False

    if input_col_type == "flux":
        if compute_mag and compute_dered:
            suffix = "_mag_dered"
            will_mag = True
            will_dered_flux = True
        elif compute_mag:
            suffix = "_mag"
            will_mag = True
        elif compute_dered:
            suffix = "_flux_dered"
            will_dered_flux = True
        else:
            suffix = "_flux"

    elif input_col_type == "flux_dered":
        if compute_dered:
            raise ValueError("Cannot deredden an already dereddened flux (input_col_type='flux_dered').")
        if compute_mag:
            suffix = "_mag_dered"
            will_mag = True
        else:
            suffix = "_flux_dered"

    elif input_col_type == "mag":
        if compute_mag:
            raise ValueError("Cannot compute magnitude when input is already magnitude (input_col_type='mag').")
        if compute_dered:
            suffix = "_mag_dered"
            will_dered_mag = True
        else:
            suffix = "_mag"

    else:  # mag_dered
        if compute_mag:
            raise ValueError("Cannot compute magnitude when input is already magnitude (input_col_type='mag_dered').")
        if compute_dered:
            raise ValueError("Cannot deredden an already dereddened magnitude (input_col_type='mag_dered').")
        suffix = "_mag_dered"

    out_kind = "mag" if (will_mag or str(input_col_type).startswith("mag")) else "flux"
    dust_tag = None
    if will_dered_flux or will_dered_mag:
        dust_tag = (input_cfg.get("use_dustmap") or "dered").strip().lower()
    elif str(input_col_type).endswith("_dered"):
        dust_tag = "dered"

    model_token = input_cfg.get("input_col_model")
    model_token = model_token.strip() if isinstance(model_token, str) and model_token.strip() else None

    tokens = [str(input_cfg.get("which_release", "release")), out_kind]
    if model_token:
        tokens.append(model_token)
    if dust_tag:
        tokens.append(dust_tag)

    suffix = "_" + "_".join(tokens)
    return suffix, will_mag, will_dered_flux, will_dered_mag


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def read_fits_to_df_no_fix(filename, columns=None):
    with fits.open(filename, memmap=True) as hdul:
        data = hdul[1].data
        df = pd.DataFrame(data)
        if columns is not None and len(columns) > 0:
            df = df[columns]
    return df


# ---------------------------------------------------------------------------
# Processamento de um arquivo (executa em workers Dask)
# ---------------------------------------------------------------------------

def process_file_df(
    path: str,
    cfg_path: str,
    will_mag: bool,
    will_dered_flux: bool,
    will_dered_mag: bool,
):
    import yaml

    with open(cfg_path, "r") as _f:
        cfgw = yaml.safe_load(_f) or {}

    input_cfg = cfgw.get("input", {})
    dust_cfg = cfgw.get("dust", {})
    output_cfg = cfgw.get("output", {})
    invalid = cfgw.get("invalid_handling", {})

    which_release = input_cfg.get("which_release", "LSST_DP02")
    input_user_selected_cols = list(input_cfg.get("user_selected_cols", []) or [])
    is_id_index = bool(input_cfg.get("is_id_in_index", False))

    col_pattern = input_cfg.get("col_pattern")
    err_pattern = input_cfg.get("err_pattern")
    selected_bands = list(input_cfg.get("selected_bands", []))
    ra_col = input_cfg.get("ra_col")
    dec_col = input_cfg.get("dec_col")

    mag_offset = _as_float_or_none(output_cfg.get("mag_offset"))
    A_EBV = dict(output_cfg.get("A_EBV", {}))

    # reading: auto-detect by extension first, then fall back to release rules
    suffix = Path(path).suffix.lower()
    if suffix == ".csv":
        df = pd.read_csv(path, usecols=input_user_selected_cols or None)
    elif suffix in {".parquet", ".pq", ".parq"}:
        try:
            df = pd.read_parquet(path, columns=input_user_selected_cols or None)
        except Exception:
            # fallback for mislabeled CSVs used in quick tests
            df = pd.read_csv(path, usecols=input_user_selected_cols or None)
    elif suffix in {".fits", ".fit"}:
        df = read_fits_to_df_no_fix(path, columns=input_user_selected_cols or None)
    else:
        # fallback to release-based reader
        if which_release in ["LSST_DP02", "LSST_DP1", "DES_Y6_GOLD_PARQUET"]:
            try:
                df = pd.read_parquet(path, columns=input_user_selected_cols or None)
            except Exception:
                df = pd.read_csv(path, usecols=input_user_selected_cols or None)
        elif which_release == "DES_DR2":
            df = read_fits_to_df_no_fix(path, columns=input_user_selected_cols or None)
        else:
            # last resort: try CSV
            df = pd.read_csv(path, usecols=input_user_selected_cols or None)

    # filtro booleano
    filt = input_cfg.get("filter", {})
    if filt.get("enabled"):
        col = filt.get("column")
        val = filt.get("value")
        if col not in df.columns:
            raise ValueError(f"Boolean column '{col}' not found in {path}")
        df = df[df[col] == val]
        if filt.get("drop_column_after_filter") and col in df.columns:
            df = df.drop(columns=[col])

    # corte inicial
    init = input_cfg.get("initial_cut", {})
    if init.get("enabled"):
        cut_col = init.get("column")
        if cut_col not in df.columns:
            raise ValueError(f"Initial-cut column '{cut_col}' not found in {path}")
        col_type = str(init.get("column_type", "flux")).strip().lower()
        mag_val = _as_float_or_none(init.get("mag_value"))
        flux_val = _as_float_or_none(init.get("flux_value"))
        if (mag_val is None) == (flux_val is None):
            raise ValueError("Provide exactly one of mag_value or flux_value in initial_cut")
        if col_type == "mag":
            if mag_val is None:
                if flux_val <= 0:
                    raise ValueError("flux_value must be > 0 to convert to mag")
                if mag_offset is None:
                    raise ValueError("mag_offset is required to convert flux -> mag")
                mag_lim = -2.5 * np.log10(float(flux_val)) + float(mag_offset)
            else:
                mag_lim = mag_val
            mask = np.isfinite(df[cut_col].values) & (df[cut_col].values <= mag_lim)
        else:  # flux
            if flux_val is None:
                if mag_offset is None:
                    raise ValueError("mag_offset is required to convert mag -> flux")
                flux_lim = np.power(10.0, -(float(mag_val) - float(mag_offset)) / 2.5)
            else:
                flux_lim = flux_val
            mask = np.isfinite(df[cut_col].values) & (df[cut_col].values >= flux_lim)
        df = df[mask]

    # Dereddening: preparar E(B-V)
    needs_ebv = bool(will_dered_flux or will_dered_mag)
    if needs_ebv:
        dq = get_dust_query(dust_cfg)
        dist_pc = None
        if dust_cfg.get("use_dustmap") in {"bayestar", "marshall", "stilism"}:
            if dust_cfg.get("distance_col_pc"):
                dist_pc = df[dust_cfg["distance_col_pc"]].values.astype(float)
            elif dust_cfg.get("distance_fixed_pc") is not None:
                dist_pc = np.full(len(df), float(dust_cfg["distance_fixed_pc"]), dtype=float)
            else:
                raise ValueError("3D dustmap requer distance_col_pc ou distance_fixed_pc")
            coords = SkyCoord(ra=df[ra_col].values * u.deg, dec=df[dec_col].values * u.deg, distance=dist_pc * u.pc)
        else:
            coords = SkyCoord(ra=df[ra_col].values * u.deg, dec=df[dec_col].values * u.deg)
        df["E_BV"] = dq(coords)

    # mascaras inválidas helpers
    inv = invalid
    def get_invalid_masks(values, errors):
        invalid_val = np.zeros(len(values), dtype=bool)
        invalid_err = np.zeros(len(errors), dtype=bool)

        def _apply_cmp(arr, thr, how, use_abs):
            x = np.abs(arr) if use_abs else arr
            if how == "greater_or_equal":
                return x >= thr
            if how == "less_or_equal":
                return x <= thr
            raise ValueError("Comparison must be 'greater_or_equal' or 'less_or_equal'")

        if inv.get("set_limit_for_col"):
            invalid_val |= _apply_cmp(values, inv.get("limit_value_for_col", 999.0), inv.get("limit_comparison_for_col", "greater_or_equal"), inv.get("use_absolute_for_col_limits", True))
        if inv.get("set_limit_for_err"):
            invalid_err |= _apply_cmp(errors, inv.get("limit_value_for_err", 999.0), inv.get("limit_comparison_for_err", "greater_or_equal"), inv.get("use_absolute_for_err_limits", True))

        if inv.get("is_nan_and_inf_invalid_for_col", True):
            invalid_val |= ~np.isfinite(values)
        if inv.get("is_nan_and_inf_invalid_for_err", True):
            invalid_err |= ~np.isfinite(errors)

        if inv.get("cross_invalidate") and inv.get("how_to_replace_col_values") == "all" and inv.get("how_to_replace_err_values") == "all":
            invalid_err |= invalid_val
            invalid_val |= invalid_err
        return invalid_val, invalid_err

    def apply_replacement(arr, mask, replacement_value):
        if replacement_value is None:
            return np.where(mask, np.nan, arr)
        return np.where(mask, replacement_value, arr)

    keep_inputs = bool(input_cfg.get("keep_input_columns_when_computing_mag_or_dered", False))

    band_case = input_cfg.get("band_case") or output_cfg.get("band_case", "lower_case")
    col_final_pattern = output_cfg.get("col_final_pattern")
    err_final_pattern = output_cfg.get("err_final_pattern")

    input_cols_to_drop: list[str] = []
    did_transform = bool(will_mag or will_dered_flux or will_dered_mag)

    for band in selected_bands:
        col_in = col_pattern.replace("BAND", band)
        err_in = err_pattern.replace("BAND", band)

        band_fmt = band.lower() if band_case == "lower_case" else band.upper()
        final_col = col_final_pattern.replace("BAND", band_fmt)
        final_err_col = err_final_pattern.replace("BAND", band_fmt)

        if col_in not in df.columns or err_in not in df.columns:
            raise ValueError(f"Missing column(s) {[col_in, err_in]} in file {path}")

        values = df[col_in].astype(float, copy=False).values
        errors = df[err_in].astype(float, copy=False).values

        if will_dered_flux:
            A_lambda = df["E_BV"].values * A_EBV[band]
            factor = np.power(10.0, 0.4 * A_lambda)
            values = values * factor
            errors = errors * factor

        if will_mag:
            f_curr = values
            values = -2.5 * np.log10(f_curr) + float(mag_offset)
            errors = errors / (f_curr * MAG_CONV)

        if will_dered_mag:
            A_lambda = df["E_BV"].values * A_EBV[band]
            values = values - A_lambda

        input_cols_to_drop.extend([col_in, err_in])

        if inv.get("replace_invalid_values"):
            invalid_val, invalid_err = get_invalid_masks(values, errors)
            col_repl = _as_float_or_none(inv.get("col_value_to_replace"))
            err_repl = _as_float_or_none(inv.get("err_value_to_replace"))

            if inv.get("how_to_replace_col_values") == "all":
                values = apply_replacement(values, invalid_val, col_repl)
            elif inv.get("how_to_replace_col_values") == "only_with_invalid_err":
                values = apply_replacement(values, invalid_err, col_repl)

            if inv.get("how_to_replace_err_values") == "all":
                errors = apply_replacement(errors, invalid_err, err_repl)
            elif inv.get("how_to_replace_err_values") == "only_with_invalid_col":
                errors = apply_replacement(errors, invalid_val, err_repl)

        if inv.get("round_col"):
            values = np.round(values, int(inv.get("round_col_decimal_cases", 5)))
        if inv.get("round_err"):
            errors = np.round(errors, int(inv.get("round_err_decimal_cases", 5)))

        df[final_col] = values
        df[final_err_col] = errors

    if needs_ebv and "E_BV" in df.columns:
        df.drop(columns=["E_BV"], inplace=True)

    if did_transform and not keep_inputs:
        drop_cols = [c for c in set(input_cols_to_drop) if c in df.columns]
        if drop_cols:
            df.drop(columns=drop_cols, inplace=True)

    if is_id_index:
        df = df.reset_index()

    return df


# ---------------------------------------------------------------------------
# Reordenação e reparticionamento
# ---------------------------------------------------------------------------

def reorder_and_rechunk(ddf: dd.DataFrame, output_cfg: dict[str, Any]):
    has_target = output_cfg.get("target_rows_per_part") not in (None, False)
    has_order = output_cfg.get("order_by") not in (None, False, "", [])

    order_cols: list[str] | None = None
    if has_order:
        ob = output_cfg.get("order_by")
        if isinstance(ob, str):
            order_cols = [ob]
        elif isinstance(ob, (list, tuple)):
            order_cols = list(ob)
        else:
            raise ValueError("order_by deve ser string ou lista")
        missing = [c for c in order_cols if c not in ddf.columns]
        if missing:
            raise ValueError(f"order_by columns not found: {missing}")

    if has_target and has_order:
        if len(order_cols) == 1:
            ddf_sorted = ddf.sort_values(order_cols[0], shuffle="p2p")
        else:
            ddf_sorted = ddf
            for col in reversed(order_cols):
                ddf_sorted = ddf_sorted.sort_values(col, shuffle="p2p")
        ddf_in = ddf_sorted
    elif (not has_target) and has_order:
        def _sort_partition(pdf, cols):
            return pdf.sort_values(cols, ascending=True)
        return ddf.map_partitions(_sort_partition, order_cols, meta=ddf._meta)
    else:
        ddf_in = ddf

    if not has_target:
        return ddf_in

    part_lengths = np.asarray(ddf_in.map_partitions(len).compute(), dtype=int)
    total_rows = int(part_lengths.sum())
    target_rows = int(output_cfg.get("target_rows_per_part"))
    n_parts = max(1, math.ceil(total_rows / target_rows))

    base = total_rows // n_parts
    remainder = total_rows % n_parts
    chunk_sizes = [(base + 1) if i < remainder else base for i in range(n_parts)]

    delayed_src_parts = ddf_in.to_delayed()
    meta = ddf_in._meta

    @delayed
    def _slice_pdf(pdf, start, end):
        return pdf.iloc[start:end]

    def _concat_pieces(pieces):
        return delayed(pd.concat)(pieces, ignore_index=True)

    delayed_new_parts = []
    i_part = 0
    i_off = 0
    src_n = len(delayed_src_parts)

    for need in chunk_sizes:
        remaining = need
        pieces = []
        while remaining > 0 and i_part < src_n:
            part_len = int(part_lengths[i_part])
            if i_off >= part_len:
                i_part += 1
                i_off = 0
                continue
            can_take = part_len - i_off
            take_now = min(remaining, can_take)
            d_piece = _slice_pdf(delayed_src_parts[i_part], i_off, i_off + take_now)
            pieces.append(d_piece)
            i_off += take_now
            remaining -= take_now
            if i_off >= part_len:
                i_part += 1
                i_off = 0
        if pieces:
            delayed_new_parts.append(_concat_pieces(pieces))

    return dd.from_delayed(delayed_new_parts, meta=meta)


# ---------------------------------------------------------------------------
# Escrita
# ---------------------------------------------------------------------------
_fname_safe_re = re.compile(r"[^A-Za-z0-9._-]+")

def _sanitize_token(s: str) -> str:
    s = str(s).strip()
    if s == "":
        return "empty"
    return _fname_safe_re.sub("_", s)


def _build_values_tag(values, col_name: str, max_list: int = 12) -> str:
    vals = [v for v in values if v is not None and not (isinstance(v, float) and np.isnan(v))]
    if not vals:
        return f"{col_name}_none"
    vals = sorted(map(_sanitize_token, vals))
    if len(vals) <= max_list:
        return f"{col_name}_" + "_".join(vals)
    head = "_".join(vals[:max_list])
    h = hashlib.sha1(",".join(vals).encode("utf-8")).hexdigest()[:8]
    return f"{col_name}_{head}_plus{len(vals)-max_list}_{h}"


@delayed
def _write_part(pdf: pd.DataFrame, base_dir: str, base_suffix: str, i: int, ext: str, col_for_name: str | None):
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
        warnings.warn(f"col_for_filename='{col_for_name}' não encontrada; ignorando")
        col_for_name = None

    delayed_parts = ddf_out.to_delayed()
    tasks = [
        _write_part(part, data_dir, suffix, i, ext, col_for_name)
        for i, part in enumerate(delayed_parts)
    ]
    written_paths = dask.compute(*tasks)
    return written_paths


# ---------------------------------------------------------------------------
# HATS collection opcional
# ---------------------------------------------------------------------------

def maybe_build_collection(collection_cfg: dict[str, Any], data_dir: str, run_path: str, suffix: str, output_cfg: dict[str, Any], client: Client, ra_col: str, dec_col: str):
    if not collection_cfg.get("build"):
        return None
    try:
        from hats_import.collection.arguments import CollectionArguments
        from hats_import.collection.run_import import run
        from hats_import.catalog.file_readers import ParquetReader, CsvReader, FitsReader
    except Exception as exc:  # pragma: no cover - só se hats-import não instalado
        raise RuntimeError("hats-import não está disponível no ambiente") from exc

    save_as = output_cfg.get("save_as", "parquet")
    if save_as == "parquet":
        data_files = "*.parquet"
        file_reader = ParquetReader()
    elif save_as == "csv":
        data_files = "*.csv"
        file_reader = CsvReader()
    else:
        raise ValueError("HATS collection suporta apenas parquet ou csv")

    files = list(Path(data_dir).glob(data_files))
    args = (
        CollectionArguments(
            output_artifact_name=f"{suffix}_collection",
            output_path=run_path,
            progress_bar=True,
        )
        .catalog(
            output_artifact_name="catalog",
            ra_column=ra_col,
            dec_column=dec_col,
            input_file_list=files,
            file_reader=file_reader,
        )
        .add_margin(
            output_artifact_name=f"margin_{str(collection_cfg.get('margin_threshold',10.0)).rstrip('0').rstrip('.') }arcs",
            margin_threshold=collection_cfg.get("margin_threshold", 10.0),
        )
    )

    report_path = Path(run_path) / "logs" / "collection_performance_report.html"
    with performance_report(filename=str(report_path)):
        run(args, client)
    return report_path


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

def run_pipeline(config_path: str, cwd: str):
    cfg = load_yml(config_path) or {}

    logdir = create_logdir(cwd)
    logger, logname = setup_logger(logdir=logdir)
    process_info_path = Path(cwd, "process.yml")
    process_info_path.touch(exist_ok=True)

    def add_info(key, value):
        info = {}
        if process_info_path.exists():
            info = load_yml(process_info_path) or {}
        info[key] = value
        dump_yml(process_info_path, info)

    add_info("log_file", str(Path(logdir.name, logname)))

    input_cfg = cfg.get("input", {})
    dust_cfg = cfg.get("dust", {})
    output_cfg = cfg.get("output", {})
    collection_cfg = cfg.get("collection", {})

    configure_dustmaps_path(dust_cfg)

    suffix, will_mag, will_dered_flux, will_dered_mag = decide_suffix_and_flags(
        input_cfg,
        input_cfg.get("compute_magnitude", True),
        input_cfg.get("compute_dereddening", True),
    )

    run_path = Path(cwd)
    data_dir = run_path / "data"
    logs_dir = run_path / "logs"
    dask_logs_dir = run_path / "dask_logs"
    data_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    dask_logs_dir.mkdir(parents=True, exist_ok=True)

    # arquivos de entrada
    input_files = [f for f in glob.glob(os.path.join(input_cfg.get("catalog_folder", ""), input_cfg.get("catalog_pattern", "*.parquet")))]
    if not input_files:
        raise FileNotFoundError("Nenhum arquivo de entrada encontrado com catalog_pattern")

    logger.info("Total de arquivos de entrada: %d", len(input_files))

    # executor / dask
    cluster = get_executor(cfg.get("cluster", {}))
    client = Client(cluster)
    cluster = client.cluster
    cluster_comm = getattr(cluster, "comm", None)
    if cluster_comm:
        wait(cluster_comm)
    client.run(lambda: gc.collect())

    # construir grafo
    delayed_dfs = [
        delayed(process_file_df)(
            p,
            cfg_path=config_path,
            will_mag=will_mag,
            will_dered_flux=will_dered_flux,
            will_dered_mag=will_dered_mag,
        )
        for p in input_files
    ]
    ddf = dd.from_delayed(delayed_dfs)

    ddf_out = reorder_and_rechunk(ddf, output_cfg)

    report_path = logs_dir / "dask-performance-report.html"
    save_perf = cfg.get("logs", {}).get("save_dask_performance", True)
    if save_perf:
        try:
            ctx = performance_report(filename=str(report_path))
        except Exception as exc:
            logger.warning("Performance report disabled (Bokeh/Jinja template issue?): %s", exc)
            ctx = nullcontext()
    else:
        ctx = nullcontext()

    try:
        with ctx:
            written_paths = write_partitions(ddf_out, output_cfg, str(data_dir), suffix)
    except Exception as exc:
        logger.warning("Performance report failed; rerunning without it: %s", exc)
        with nullcontext():
            written_paths = write_partitions(ddf_out, output_cfg, str(data_dir), suffix)

    add_info("outputs", [{"path": str(Path(p).relative_to(cwd)), "role": "main"} for p in written_paths])

    # opcional hats collection
    try:
        collection_report = maybe_build_collection(collection_cfg, str(data_dir), str(run_path), suffix, output_cfg, client, input_cfg.get("ra_col"), input_cfg.get("dec_col"))
        if collection_report:
            add_info("collection_report", str(collection_report.relative_to(cwd)))
    except Exception as exc:
        logger.warning("Falha ao gerar HATS collection: %s", exc)

    client.close()
    cluster.close()

    # copy config and logs for consistency (avoid copying onto itself)
    copy_file(config_path, str(run_path / "config.yml"))
    src_logs = Path(logdir).resolve()
    dst_logs = (run_path / "process_info").resolve()
    if src_logs != dst_logs:
        copy_directory(str(src_logs), str(dst_logs))

    logger.info("Time elapsed (s): %s", "%.2f" % 0)


__all__ = ["run_pipeline"]
