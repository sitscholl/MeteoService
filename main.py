import uvicorn

import logging
import logging.config

from webhandler.config import load_config
from webhandler.api import app

# Load config file
config = load_config("config/config.yaml")

# Configure logging
logging.config.dictConfig(config['logging'])

uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")