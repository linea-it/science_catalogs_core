"""Small config helpers for the Science Catalogs pipeline."""

from typing import Any, Tuple

ALLOWED_INPUT_TYPES = {"flux", "flux_dered", "mag", "mag_dered"}


def as_none(value):
    """Normalize empty/NaN-ish values to None."""
    import numpy as np

    if value is None:
        return None
    if isinstance(value, float) and np.isnan(value):
        return None
    if isinstance(value, str) and value.strip().lower() in ("", "none", "null", "nan", "~"):
        return None
    return value


def as_float_or_none(value):
    """Cast to float when possible, otherwise return None."""
    value = as_none(value)
    if value is None:
        return None
    return float(value)


def decide_suffix_and_flags(
    input_cfg: dict[str, Any], compute_mag: bool, compute_dered: bool
) -> Tuple[str, bool, bool, bool]:
    """
    Derive output suffix and transformation flags from the input config.

    Returns:
        suffix, will_compute_mag, will_deredden_flux, will_deredden_mag
    """

    input_col_type = input_cfg.get("input_col_type", "flux")
    if input_col_type not in ALLOWED_INPUT_TYPES:
        raise ValueError(f"Invalid input_col_type='{input_col_type}'. Allowed: {sorted(ALLOWED_INPUT_TYPES)}")

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


__all__ = ["as_none", "as_float_or_none", "decide_suffix_and_flags", "ALLOWED_INPUT_TYPES"]
