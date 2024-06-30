from dagster import Definitions, FilesystemIOManager

# Import the asset collections from the __init__.py file
from .assets import (
    open_targets_evidence_assets,
    pm_assets,
    train_assets,
    CS_abstracts
)

from .io_managers import ParquetIOManager

all_assets = [
    *open_targets_evidence_assets,
    *pm_assets,
    *train_assets,
    *CS_abstracts
]

defs = Definitions(
    assets=all_assets,
    resources={
        "fs_io_manager": FilesystemIOManager(),
        "polars_parquet_io_manager": ParquetIOManager()
    },
)


