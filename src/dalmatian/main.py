import uvicorn

from dalmatian.config import get_settings
from dalmatian.observability import configure_logging


def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    uvicorn.run("dalmatian.api:app", host="0.0.0.0", port=8000, log_config=None)
