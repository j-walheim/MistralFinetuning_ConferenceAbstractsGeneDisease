import os
import polars as pl
from upath import UPath
from dagster import InputContext, OutputContext, UPathIOManager


import os
import polars as pl
from dagster import IOManager, InputContext, OutputContext
from dagster.core.storage.upath_io_manager import UPathIOManager
from upath import UPath
from typing import Union

import os
import polars as pl
from dagster import IOManager, InputContext, OutputContext
from typing import Union


class ParquetIOManager(UPathIOManager):
    extension: str = ".parquet"

    def _get_path(self, context, filename):
        storage_dir = os.path.join(os.getenv("DAGSTER_HOME", ""), 'storage')
        os.makedirs(storage_dir, exist_ok=True)
        
        if getattr(context, 'has_partition_key', False):
            return os.path.join(storage_dir, context.asset_key.path[-1], 
                                context.partition_key + '_' + filename)
        else:
            return os.path.join(storage_dir, context.asset_key.path[-1], filename)

    def dump_to_path(self, context: OutputContext, obj: pl.DataFrame, path: UPath):

        full_path = self._get_path(context, f"{context.name}.parquet")

        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        obj.write_parquet(full_path)

    def load_from_path(self, context: InputContext, path: UPath) -> pl.LazyFrame:
        full_path = self._get_path(context.upstream_output, f"{context.upstream_output.name}.parquet")
        return pl.scan_parquet(full_path)

