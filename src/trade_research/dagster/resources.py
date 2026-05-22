from dagster import InitResourceContext, resource

from trade_research.config import get_settings
from trade_research.storage.timescale import TimescaleStore


@resource
def timescale_store(context: InitResourceContext) -> TimescaleStore:
    settings = get_settings()
    store = TimescaleStore(settings.database_url)
    resource_config = context.resource_config or {}
    if resource_config.get("initialize", True):
        store.initialize()
    return store
