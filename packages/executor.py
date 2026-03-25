import logging
from typing import Any

from dask.distributed import LocalCluster
from dask_jobqueue import SLURMCluster


def get_executor(executor_cfg: dict[str, Any]):
    """Create a Dask cluster (local or Slurm) from the `cluster` YAML block."""

    logger = logging.getLogger(__name__)
    name = executor_cfg.get("executor", "local")

    if name == "local":
        args = executor_cfg.get("local", {})
        logger.info("Creating LocalCluster with %s", args)
        cluster = LocalCluster(**args)
        return cluster

    if name == "slurm":
        args = executor_cfg.get("slurm", {})
        job_extra_directives = args.get("job_extra_directives", []) or []

        cluster = SLURMCluster(
            interface=args.get("interface"),
            queue=args.get("queue"),
            cores=args.get("cores"),
            processes=args.get("processes"),
            memory=args.get("memory"),
            walltime=args.get("walltime"),
            job_extra_directives=job_extra_directives,
        )
        scale = int(args.get("dask_scale_number", 1) or 1)
        cluster.scale(jobs=scale)
        return cluster

    raise ValueError(f"Executor '{name}' not supported")
