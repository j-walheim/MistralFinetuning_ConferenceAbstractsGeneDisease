from dagster import Definitions, FilesystemIOManager, define_asset_job

# Import the asset collections from the __init__.py file
from .assets import (
    open_targets_evidence_assets,
    pm_assets,
    train_assets,
    CS_abstracts
)

all_assets = [
    *open_targets_evidence_assets,
    *pm_assets,
    *train_assets,
    *CS_abstracts
]


# # Addition: define a job that will materialize the assets
# ot_literature = define_asset_job("ot_literature", selection=[open_targets_evidence_assets])
# cs_abstracts = define_asset_job("cs_abstracts", selection=[CS_abstracts])
# pm_abstracts = define_asset_job("pm_abstracts", selection=[pm_assets])
# train_data = define_asset_job("train_assets", selection=[train_assets])
# all_assets_job = define_asset_job(name="all_assets_job")

lit_job = define_asset_job(
    name="lit_job", selection="raw_literature"
)



defs = Definitions(
    assets=all_assets,
    resources={
        "fs_io_manager": FilesystemIOManager(),
    },
    # jobs=[all_assets_job,
    #       lit_job,
    #       ot_literature,
    #       cs_abstracts,
    #       pm_abstracts,
    #       train_data],  
)


