import logging
import pathlib
import shutil
from typing import Any

import yaml


def create_logdir(cwd: str) -> pathlib.Path:
    logdir = pathlib.Path(cwd, "process_info")
    logdir.mkdir(parents=True, exist_ok=True)
    return logdir


def setup_logger(name="science_catalogs", logdir="."):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logname = "pipeline.log"

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    filename = pathlib.Path(logdir, logname)
    file_handler = logging.FileHandler(filename)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(file_handler)

    return logger, logname


def load_yml(filepath: str) -> Any:
    with open(filepath, encoding="utf-8") as _file:
        return yaml.safe_load(_file)


def dump_yml(filepath, content, encoding="utf-8"):
    with open(filepath, "w", encoding=encoding) as _file:
        yaml.dump(content, _file)


def copy_file(src: str, dst: str):
    shutil.copy2(src, dst)


def copy_directory(src_dir: str, dst_dir: str):
    shutil.copytree(src_dir, dst_dir, dirs_exist_ok=True)
