from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import yaml

from pipeline_runner import run_pipeline


def _write_config(
    tmp_path: Path,
    repo_root: Path,
    base_config: dict,
    *,
    save_as: str,
) -> Path:
    cfg = deepcopy(base_config)
    cfg["cluster"]["local"]["processes"] = False
    cfg["cluster"]["local"]["dashboard_address"] = None
    cfg["input"]["catalog_folder"] = str((repo_root / "examples" / "input").resolve())
    cfg["dust"]["path_to_dustmaps"] = str((repo_root / "examples" / "dustmaps").resolve())
    cfg["output"]["save_as"] = save_as

    if save_as == "hats":
        cfg["output"]["hats_source_save_as"] = "parquet"
        cfg["output"]["hats_artifact_name"] = "demo_hats_catalog"
        cfg["output"]["hats_margin_threshold"] = 10.0

    config_path = tmp_path / f"config_{save_as}.yaml"
    with config_path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(cfg, handle, sort_keys=False)
    return config_path


def test_run_pipeline_writes_parquet_outputs(
    tmp_path: Path,
    repo_root: Path,
    base_config: dict,
) -> None:
    config_path = _write_config(tmp_path, repo_root, base_config, save_as="parquet")
    run_dir = tmp_path / "parquet_run"

    run_pipeline(str(config_path), str(run_dir))

    parquet_files = sorted((run_dir / "data").glob("*.parquet"))
    assert parquet_files
    assert (run_dir / "config.yml").exists()
    assert (run_dir / "process.yml").exists()
    assert (run_dir / "process_info" / "pipeline.log").exists()

    with (run_dir / "process.yml").open(encoding="utf-8") as handle:
        process_info = yaml.safe_load(handle)

    assert process_info["outputs"] == [{"path": "data", "role": "main"}]


def test_run_pipeline_writes_hats_outputs(
    tmp_path: Path,
    repo_root: Path,
    base_config: dict,
) -> None:
    config_path = _write_config(tmp_path, repo_root, base_config, save_as="hats")
    run_dir = tmp_path / "hats_run"

    run_pipeline(str(config_path), str(run_dir))

    hats_root = run_dir / "data" / "demo_hats_catalog"
    assert hats_root.exists()
    assert (hats_root / "collection.properties").exists()
    assert (hats_root / "catalog" / "hats.properties").exists()
    assert (hats_root / "catalog" / "partition_info.csv").exists()
    assert (hats_root / "catalog" / "skymap.fits").exists()
    assert (hats_root / "margin_10arcs" / "hats.properties").exists()
