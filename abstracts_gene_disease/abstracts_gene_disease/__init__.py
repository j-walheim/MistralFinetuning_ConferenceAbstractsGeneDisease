from dagster import Definitions, FilesystemIOManager, define_asset_job

# Import the asset collections from the __init__.py file
from .assets import (
    open_targets_evidence_assets,
    pm_assets,
    train_assets,
#    CS_abstracts
)

all_assets = [
    *open_targets_evidence_assets,
    *pm_assets,
    *train_assets,
#    *CS_abstracts
]



lit_job = define_asset_job(
    name="lit_job", selection="raw_literature"
)



defs = Definitions(
    assets=all_assets,
    resources={
        "fs_io_manager": FilesystemIOManager(),
    },
)


