import uvicorn

from webhandler.api import app

uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")