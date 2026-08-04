import os
from typing import Any, List, Optional

from pydantic import BaseModel, Field
import yaml

DATASETS_DIR = os.getenv("DATASETS_DIR", "./data-example")


class Local(BaseModel):
    n_workers: int = 2
    threads_per_worker: int = 2
    memory_limit: str = "4GiB"


class Slurm(BaseModel):
    interface: Optional[str] = "eno1"
    queue: Optional[str] = "cpu"
    cores: int = 20
    processes: int = 1
    memory: str = "50GB"
    walltime: str = "02:00:00"
    account: Optional[str] = "hpc-bpglsst"
    save_jobs_info: bool = False
    dask_scale_number: int = 1
    extra_dask_configs: bool = False
    dask_config: dict[str, Any] = Field(default_factory=dict)
    job_extra_directives: List[str] = Field(default_factory=list)


class Cluster(BaseModel):
    executor: str = "local"
    local: Local = Local()
    slurm: Slurm = Slurm()


class InputFilter(BaseModel):
    enabled: bool = False
    column: str = ""
    value: Any = 1
    drop_column_after_filter: bool = False


class InitialCut(BaseModel):
    enabled: bool = False
    column: str = ""
    column_type: str = "flux"
    mag_value: Optional[float] = None
    flux_value: Optional[float] = None


class Input(BaseModel):
    catalog_folder: str = f"{DATASETS_DIR}/lsst_dp02_catalogs"
    catalog_pattern: str = "*.parquet"
    user_selected_cols: List[str] = Field(default_factory=list)
    which_release: str = "LSST_DP02"
    is_id_in_index: bool = False
    filter: InputFilter = InputFilter()
    initial_cut: InitialCut = InitialCut()
    col_pattern: str = "FLUX_BAND"
    err_pattern: str = "FLUXERR_BAND"
    selected_bands: List[str] = ["u", "g", "r", "i", "z", "y"]
    ra_col: str = "ra"
    dec_col: str = "dec"
    input_col_type: str = "flux"
    input_col_model: Optional[str] = None
    compute_magnitude: bool = True
    compute_dereddening: bool = True
    keep_input_columns_when_computing_mag_or_dered: bool = False


class Dust(BaseModel):
    use_dustmap: str = "sfd"
    path_to_dustmaps: str = f"{DATASETS_DIR}/dustmaps"
    distance_col_pc: Optional[str] = None
    distance_fixed_pc: Optional[float] = None


class Output(BaseModel):
    base_path: str = "."
    save_as: str = "parquet"
    hats_source_save_as: str = "parquet"
    hats_artifact_name: Optional[str] = None
    hats_margin_threshold: Optional[float] = None
    col_for_filename: Optional[str] = None
    order_by: Optional[Any] = Field(default_factory=list)
    target_rows_per_part: Optional[int] = None
    col_final_pattern: str = "mag_BAND"
    err_final_pattern: str = "magerr_BAND"
    band_case: str = "lower_case"
    mag_offset: float = 31.4
    A_EBV: dict[str, float] = {
        "u": 4.239,
        "g": 3.303,
        "r": 2.285,
        "i": 1.698,
        "z": 1.263,
        "y": 1.088,
    }


class InvalidHandling(BaseModel):
    replace_invalid_values: bool = False
    cross_invalidate: bool = False
    how_to_replace_col_values: str = "all"
    how_to_replace_err_values: str = "all"
    col_value_to_replace: Optional[float] = None
    err_value_to_replace: Optional[float] = None
    is_nan_and_inf_invalid_for_col: bool = True
    is_nan_and_inf_invalid_for_err: bool = True
    set_limit_for_col: bool = False
    limit_value_for_col: float = 999.0
    limit_comparison_for_col: str = "greater_or_equal"
    use_absolute_for_col_limits: bool = True
    set_limit_for_err: bool = False
    limit_value_for_err: float = 999.0
    limit_comparison_for_err: str = "greater_or_equal"
    use_absolute_for_err_limits: bool = True
    round_col: bool = False
    round_col_decimal_cases: int = 5
    round_err: bool = False
    round_err_decimal_cases: int = 5


class Collection(BaseModel):
    build: bool = False
    margin_threshold: float = 10.0


class Logs(BaseModel):
    save_dask_performance: bool = False


class Config(BaseModel):
    cluster: Cluster = Cluster()
    input: Input = Input()
    dust: Dust = Dust()
    output: Output = Output()
    invalid_handling: InvalidHandling = InvalidHandling()
    collection: Collection = Collection()
    logs: Logs = Logs()


if __name__ == "__main__":
    cfg = Config()
    data = cfg.model_dump()
    with open("config.yaml", "w", encoding="utf-8") as fh:
        yaml.dump(data, fh)
    print("config.yaml generated from defaults")
