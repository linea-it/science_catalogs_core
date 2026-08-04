"""Pipeline orchestration."""

from copy import deepcopy
import gc
from contextlib import nullcontext
from pathlib import Path

from dask.distributed import Client, performance_report, wait
from science_catalogs import prepare_catalog, write_catalog

from executor import get_executor
from utils.common import (
    copy_file,
    copy_directory,
    create_logdir,
    dump_yml,
    load_yml,
    setup_logger,
)


def _adapt_config_for_science_catalogs(cfg: dict) -> dict:
    """Translate legacy app settings into the science_catalogs package schema."""
    adapted = deepcopy(cfg)
    output_cfg = adapted.setdefault("output", {})
    collection_cfg = adapted.get("collection", {})

    if collection_cfg.get("build") and output_cfg.get("save_as") != "hats":
        source_format = output_cfg.get("save_as", "parquet")
        output_cfg["hats_source_save_as"] = output_cfg.get("hats_source_save_as", source_format)
        output_cfg["save_as"] = "hats"
        if output_cfg.get("hats_margin_threshold") is None:
            output_cfg["hats_margin_threshold"] = collection_cfg.get("margin_threshold")

    return adapted


def run_pipeline(config_path: str, cwd: str):
    cfg = load_yml(config_path) or {}
    adapted_cfg = _adapt_config_for_science_catalogs(cfg)

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

    run_path = Path(cwd)
    data_dir = run_path / "data"
    logs_dir = run_path / "logs"
    dask_logs_dir = run_path / "process_info" / "dask_logs"
    data_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    dask_logs_dir.mkdir(parents=True, exist_ok=True)

    cluster = get_executor(adapted_cfg.get("cluster", {}))
    client = Client(cluster)
    cluster = client.cluster
    cluster_comm = getattr(cluster, "comm", None)
    if cluster_comm:
        wait(cluster_comm)
    client.run(lambda: gc.collect())

    if cfg != adapted_cfg:
        logger.info("Translated legacy collection.build settings into science_catalogs output config.")

    report_path = logs_dir / "dask-performance-report.html"
    save_perf = adapted_cfg.get("logs", {}).get("save_dask_performance", True)
    if save_perf:
        try:
            ctx = performance_report(filename=str(report_path))
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning(
                "Performance report disabled (Bokeh/Jinja template issue?): %s", exc
            )
            ctx = nullcontext()
    else:
        ctx = nullcontext()

    try:
        with ctx:
            prepared = prepare_catalog(config_path, config=adapted_cfg, client=client)
            written_paths = write_catalog(prepared, str(data_dir), client=client)
    except Exception as exc:  # pylint: disable=broad-except
        logger.warning("Performance report failed; rerunning without it: %s", exc)
        with nullcontext():
            prepared = prepare_catalog(config_path, config=adapted_cfg, client=client)
            written_paths = write_catalog(prepared, str(data_dir), client=client)

    logger.info("Processed %d input files", len(prepared.input_files))
    logger.info("Wrote %d output artifact(s)", len(written_paths))

    output_dir_entry = {
        "path": str(data_dir.relative_to(cwd)),
        "role": "main",
    }

    add_info("outputs", [output_dir_entry])

    client.close()
    cluster.close()

    copy_file(config_path, str(run_path / "config.yml"))
    src_logs = Path(logdir).resolve()
    dst_logs = (run_path / "process_info").resolve()
    if src_logs != dst_logs:
        copy_directory(str(src_logs), str(dst_logs))

    logger.info("Pipeline finished.")


__all__ = ["run_pipeline"]
