## Science Catalogs Pipeline

Pipeline app that runs `science-catalogs` with local orchestration, logs, and run artifacts.

Requirements
- micromamba available in PATH (to create the `pipe_sc` env)

Install
```
./install.sh
```
Creates/updates the micromamba env defined in `environment.yaml`.

Configure
```
cp config.test.yaml config.yaml
```

Run
```
./run.sh config.yaml run001
```
Outputs: `run001/data`, `run001/logs`, `run001/process_info`, `run001/config.yml`.

Tests
```
/home/singulani/.local/bin/micromamba run --root-prefix /home/singulani/micromamba -n pipe_sc pytest -q
```
Current coverage includes one smoke test for `parquet` output and one for `hats` output.

Project layout
- run.sh, install.sh, setup.sh — orchestration scripts
- environment.yaml — micromamba env (`pipe_sc`)
- config.template.yaml, config.test.yaml — config templates/examples
- scripts/sc-run — pipeline entrypoint
- packages/ — thin app layer for execution, logging, and compatibility wrappers

Reuse
The reusable catalog logic lives in the `science-catalogs` PyPI package. This repository keeps only the pipeline app layer.

Acknowledgements
Developed at LIneA as part of contributions to Rubin/LSST, using the LINCC software layer (`hats`, `hats-import`, `lsdb`).
