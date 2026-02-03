import uvicorn

from src.log_handler import LogHandler
from src.api import app

# Configure logging
log_handler = LogHandler.from_file("config.logging.yaml")
log_handler.start_logger()

uvicorn.run(app, host="0.0.0.0", port=8000, log_level=None)