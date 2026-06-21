import logging
import logging.config
import sys
import yaml

from pathlib import Path

from typing import Dict, Any

logger = logging.getLogger(__name__)

# List of noisy loggers to silence
NOISY_LOGGERS = [
    'matplotlib',
    'asyncio',
    'requests',
    'httpx'
]

class LogHandler:

    def __init__(self, config: Dict[str, Any] | None = None):
        self.config = config

    @classmethod
    def from_file(cls, config_file: str | Path):
        config_file = Path(config_file)
        if not config_file.exists():
            logger.info(f"No config file found at {config_file}. Using default logging configuration")
            return cls()
        else:
            try:
                with open(config_file) as f:
                    config = yaml.safe_load(f)
            except Exception as e:
                logger.warning(f"Error loading config from config_file {config_file}: {e}")
                return cls()

            return cls(config = config)

    @staticmethod
    def _coerce_log_level(log_level: str | int | None) -> int | None:
        if log_level is None:
            return None
        if isinstance(log_level, int):
            return log_level

        level_name = log_level.strip()
        if level_name.isdigit():
            return int(level_name)

        level = logging.getLevelName(level_name.upper())
        if isinstance(level, int):
            return level

        raise ValueError(f"Invalid log level: {log_level}")

    def _start_basic_logger(self, verbose: bool = False, log_level: int | None = None):
        # Set up a basic default logger
        log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        date_format = "%Y-%m-%d %H:%M:%S"
        level = log_level if log_level is not None else (logging.DEBUG if verbose else logging.INFO)
        
        logging.basicConfig(
            level=level,
            format=log_format,
            datefmt=date_format,
            stream=sys.stdout,
        )

    def start_logger(self, verbose = False, log_level: str | int | None = None):
        """
        Setup logging configuration from a file or with sensible defaults.
        
        Parameters
        ----------
        config_file : dict, optional
            Path to the logging configuration file.
        """
        level_override = self._coerce_log_level(log_level)
        if self.config:
            # Use configuration if provided
            logging.config.dictConfig(self.config)
            if level_override is not None:
                logging.getLogger().setLevel(level_override)
                for handler in logging.getLogger().handlers:
                    handler.setLevel(level_override)
                for logger_name in self.config.get("loggers", {}):
                    logging.getLogger(logger_name).setLevel(level_override)
            logger.debug("Loaded logging configuration")
        else:
            logger.debug("Using default logging configuration as no configuration was provided.")
            self._start_basic_logger(verbose = verbose, log_level = level_override)
        
        # Silence noisy third-party libraries
        if not verbose:
            self.silence_noisy_loggers()

    def silence_noisy_loggers(self, log_level=logging.WARNING):
        """
        Set higher log level for commonly noisy third-party libraries.
        
        Parameters
        ----------
        log_level : int, optional
            Log level to set for third-party libraries, default is WARNING.
        """       
        logger.debug('Silencing noisy loggers')
        for logger_name in NOISY_LOGGERS:
            logging.getLogger(logger_name).setLevel(log_level)
