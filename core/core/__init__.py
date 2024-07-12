from dagster import Definitions, load_assets_from_modules

from .assets import scraped_assets

all_assets = load_assets_from_modules([scraped_assets])

defs = Definitions(
    assets=all_assets,
)
