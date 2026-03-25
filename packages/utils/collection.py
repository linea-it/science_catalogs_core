"""Optional HATS collection generation."""

from pathlib import Path
from typing import Any

from dask.distributed import Client, performance_report


def maybe_build_collection(
    collection_cfg: dict[str, Any],
    data_dir: str,
    run_path: str,
    suffix: str,
    output_cfg: dict[str, Any],
    client: Client,
    ra_col: str,
    dec_col: str,
):
    if not collection_cfg.get("build"):
        return None
    try:
        from hats_import.collection.arguments import CollectionArguments
        from hats_import.collection.run_import import run
        from hats_import.catalog.file_readers import ParquetReader, CsvReader
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("hats-import not available in the environment") from exc

    save_as = output_cfg.get("save_as", "parquet")
    if save_as == "parquet":
        data_files = "*.parquet"
        file_reader = ParquetReader()
    elif save_as == "csv":
        data_files = "*.csv"
        file_reader = CsvReader()
    else:
        raise ValueError("HATS collection supports only parquet or csv")

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


__all__ = ["maybe_build_collection"]
