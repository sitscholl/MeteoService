import os
import uvicorn

from src.log_handler import LogHandler


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def main():
    log_config_path = os.getenv("METEOSERVICE_LOG_CONFIG", "config/logging.yaml")
    log_handler = LogHandler.from_file(log_config_path)
    log_handler.start_logger()

    uvicorn.run(
        "src.api:app",
        host=os.getenv("UVICORN_HOST", "0.0.0.0"),
        port=int(os.getenv("UVICORN_PORT", "8000")),
        workers=int(os.getenv("UVICORN_WORKERS", "1")),
        log_level=os.getenv("UVICORN_LOG_LEVEL", "info"),
        log_config=None,
        access_log=_env_bool("UVICORN_ACCESS_LOG", True),
    )


if __name__ == "__main__":
    main()
