"""Pipeline orchestration."""

import gc
import glob
from contextlib import nullcontext
from pathlib import Path

from dask import delayed
from dask import dataframe as dd
from dask.distributed import Client, performance_report, wait

from utils.collection import maybe_build_collection
from utils.config import decide_suffix_and_flags
from utils.dust import configure_dustmaps_path
from executor import get_executor
from utils.partitioning import reorder_and_rechunk
from processing import process_file_df
from utils.common import (
    copy_file,
    copy_directory,
    create_logdir,
    dump_yml,
    load_yml,
    setup_logger,
)
from utils.writers import write_partitions


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

    inputs = cfg.get("input", {})
    dust = cfg.get("dust", {})
    output_cfg = cfg.get("output", {})
    collection_cfg = cfg.get("collection", {})

    configure_dustmaps_path(dust)

    suffix, will_mag, will_dered_flux, will_dered_mag = decide_suffix_and_flags(
        inputs,
        inputs.get("compute_magnitude", True),
        inputs.get("compute_dereddening", True),
    )

    run_path = Path(cwd)
    data_dir = run_path / "data"
    logs_dir = run_path / "logs"
    dask_logs_dir = run_path / "dask_logs"
    data_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    dask_logs_dir.mkdir(parents=True, exist_ok=True)

    input_files = [
        f
        for f in glob.glob(
            Path(inputs.get("catalog_folder", "")).expanduser().as_posix()
            + "/"
            + inputs.get("catalog_pattern", "*.parquet")
        )
    ]
    if not input_files:
        raise FileNotFoundError("No input files found for catalog_pattern")

    logger.info("Total input files: %d", len(input_files))

    cluster = get_executor(cfg.get("cluster", {}))
    client = Client(cluster)
    cluster = client.cluster
    cluster_comm = getattr(cluster, "comm", None)
    if cluster_comm:
        wait(cluster_comm)
    client.run(lambda: gc.collect())

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
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning(
                "Performance report disabled (Bokeh/Jinja template issue?): %s", exc
            )
            ctx = nullcontext()
    else:
        ctx = nullcontext()

    try:
        with ctx:
            write_partitions(ddf_out, output_cfg, str(data_dir), suffix)
    except Exception as exc:  # pylint: disable=broad-except
        logger.warning("Performance report failed; rerunning without it: %s", exc)
        with nullcontext():
            write_partitions(ddf_out, output_cfg, str(data_dir), suffix)

    output_dir_entry = {
        "path": str(data_dir.relative_to(cwd)),
        "role": "main",
    }

    add_info("outputs", [output_dir_entry])

    try:
        collection_report = maybe_build_collection(
            collection_cfg,
            str(data_dir),
            str(run_path),
            suffix,
            output_cfg,
            client,
            inputs.get("ra_col"),
            inputs.get("dec_col"),
        )
        logger.info("HATS collection report generated at: %s", collection_report)
        if collection_report:
            add_info("collection_report", str(collection_report.relative_to(cwd)))
    except Exception as exc:  # pylint: disable=broad-except
        logger.info("Failed to generate HATS collection: %s", exc)

    client.close()
    cluster.close()

    copy_file(config_path, str(run_path / "config.yml"))
    src_logs = Path(logdir).resolve()
    dst_logs = (run_path / "process_info").resolve()
    if src_logs != dst_logs:
        copy_directory(str(src_logs), str(dst_logs))

    logger.info("Pipeline finished.")


__all__ = ["run_pipeline"]
