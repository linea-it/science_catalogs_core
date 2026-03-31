## Science Catalogs Pipeline

Creates photometric “science catalogs” with Dask.

Requirements
- micromamba available in PATH (to create the `pipe_sc` env)

Install
```
./install.sh
```
Creates/updates the micromamba env defined in `environment.yaml`.

Configure
```
cp config.template.yaml config.yaml      # generic template
# or
cp config.des_dr2.yaml config.yaml       # DES DR2 example (from the notebook)
```
Optional: generate `config.yaml` and `env.sh` interactively with your paths:
```
./setup.sh
```

Run
```
./run.sh config.yaml run001
```
Outputs: `run001/data`, `run001/logs`, `run001/process_info`, `run001/config.yml`.

Project layout
- run.sh, install.sh, setup.sh — orchestration scripts
- environment.yaml — micromamba env (`pipe_sc`)
- config.template.yaml, config.des_dr2.yaml — config templates/examples
- scripts/sc-run — pipeline entrypoint
- packages/ — core modules (`core.py`, `executor.py`, `utils.py`)
- tsm/ — Training Set Maker bundle kept for reference

Reuse
`packages/core.py` exposes `run_pipeline(config_path, cwd)`. Add `packages/` to `PYTHONPATH` or extract it to a shared package to reuse across pipelines.

Acknowledgements
Developed at LIneA as part of contributions to Rubin/LSST, using the LINCC software layer (`hats`, `hats-import`, `lsdb`).
