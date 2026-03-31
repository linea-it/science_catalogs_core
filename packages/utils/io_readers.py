"""File readers and format detection."""

from pathlib import Path
from typing import Any, Iterable

import pandas as pd
from astropy.io import fits


def read_fits_to_df_no_fix(filename, columns=None):
    with fits.open(filename, memmap=True) as hdul:
        data = hdul[1].data
        df = pd.DataFrame(data)
        if columns is not None and len(columns) > 0:
            df = df[columns]
    return df


def detect_and_read(path: str, which_release: str, columns: Iterable[str] | None):
    """Read CSV/Parquet/FITS, falling back on release hints."""
    suffix = Path(path).suffix.lower()
    columns = list(columns) if columns else None

    if suffix == ".csv":
        return pd.read_csv(path, usecols=columns)
    if suffix in {".parquet", ".pq", ".parq"}:
        try:
            return pd.read_parquet(path, columns=columns)
        except Exception:
            return pd.read_csv(path, usecols=columns)
    if suffix in {".fits", ".fit"}:
        return read_fits_to_df_no_fix(path, columns=columns)

    if which_release in ["LSST_DP02", "LSST_DP1", "DES_Y6_GOLD_PARQUET"]:
        try:
            return pd.read_parquet(path, columns=columns)
        except Exception:
            return pd.read_csv(path, usecols=columns)
    if which_release == "DES_DR2":
        return read_fits_to_df_no_fix(path, columns=columns)

    # last resort
    return pd.read_csv(path, usecols=columns)


__all__ = ["detect_and_read", "read_fits_to_df_no_fix"]
