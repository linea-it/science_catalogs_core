"""Per-file processing logic."""

import numpy as np
import pandas as pd
from astropy.coordinates import SkyCoord
import astropy.units as u

from utils.config import as_float_or_none, as_none
from utils.dust import get_dust_query
from utils.io_readers import detect_and_read

MAG_CONV = np.log(10) * 0.4


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

    mag_offset = as_float_or_none(output_cfg.get("mag_offset"))
    A_EBV = dict(output_cfg.get("A_EBV", {}))

    df = detect_and_read(path, which_release, input_user_selected_cols)

    filt = input_cfg.get("filter", {})
    if filt.get("enabled"):
        col = filt.get("column")
        val = filt.get("value")
        if col not in df.columns:
            raise ValueError(f"Boolean column '{col}' not found in {path}")
        df = df[df[col] == val]
        if filt.get("drop_column_after_filter") and col in df.columns:
            df = df.drop(columns=[col])

    init = input_cfg.get("initial_cut", {})
    if init.get("enabled"):
        cut_col = init.get("column")
        if cut_col not in df.columns:
            raise ValueError(f"Initial-cut column '{cut_col}' not found in {path}")
        col_type = str(init.get("column_type", "flux")).strip().lower()
        mag_val = as_float_or_none(init.get("mag_value"))
        flux_val = as_float_or_none(init.get("flux_value"))
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
                raise ValueError("3D dustmap requires distance_col_pc or distance_fixed_pc")
            coords = SkyCoord(ra=df[ra_col].values * u.deg, dec=df[dec_col].values * u.deg, distance=dist_pc * u.pc)
        else:
            coords = SkyCoord(ra=df[ra_col].values * u.deg, dec=df[dec_col].values * u.deg)
        df["E_BV"] = dq(coords)

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
            invalid_val |= _apply_cmp(
                values,
                inv.get("limit_value_for_col", 999.0),
                inv.get("limit_comparison_for_col", "greater_or_equal"),
                inv.get("use_absolute_for_col_limits", True),
            )
        if inv.get("set_limit_for_err"):
            invalid_err |= _apply_cmp(
                errors,
                inv.get("limit_value_for_err", 999.0),
                inv.get("limit_comparison_for_err", "greater_or_equal"),
                inv.get("use_absolute_for_err_limits", True),
            )

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
            col_repl = as_float_or_none(inv.get("col_value_to_replace"))
            err_repl = as_float_or_none(inv.get("err_value_to_replace"))

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


__all__ = ["process_file_df", "MAG_CONV"]
