import logging
from typing import Any

from dask.distributed import LocalCluster
from dask_jobqueue import SLURMCluster


def get_executor(executor_cfg: dict[str, Any]):
    """Cria cluster Dask local ou Slurm a partir do bloco `cluster` do YAML.

    executor_cfg deve ter as chaves:
      - executor: "local" | "slurm"
      - local: {n_workers, threads_per_worker, memory_limit}
      - slurm: {interface, queue, cores, processes, memory, walltime, account,
                save_jobs_info, dask_scale_number, job_extra_directives, extra_dask_configs, dask_config}
    """

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
        if args.get("save_jobs_info"):
            # caller deve ter ajustado os paths nos job_extra_directives
            pass
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

    raise ValueError(f"Executor '{name}' não suportado")
