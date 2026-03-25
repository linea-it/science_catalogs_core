"""Partitioning and ordering helpers for Dask DataFrames."""

import math
from typing import Any, List

import numpy as np
import pandas as pd
from dask import delayed
from dask import dataframe as dd


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
            raise ValueError("order_by must be a string or list")
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


__all__ = ["reorder_and_rechunk"]
