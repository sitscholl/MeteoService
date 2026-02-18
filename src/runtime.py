from pathlib import Path
from dataclasses import dataclass
import yaml

import logging

from .database.db import MeteoDB
from .provider_manager import ProviderManager
from .gapfinder import Gapfinder
from .query_manager import QueryManager
from .resample import DEFAULT_RESAMPLE_COLMAP, ColumnResampler

logger = logging.getLogger(__name__)

def load_config_file(config_file: str | Path) -> dict:
    """Load configuration from YAML file."""
    try:
        with open(config_file, 'r') as file:
            config = yaml.safe_load(file)
            logger.info(f"Loaded config file from {config_file}")
    except FileNotFoundError:
        logger.error(f"Configuration file {config_file} not found")
        raise
    except yaml.YAMLError as e:
        logger.error(f"Error parsing configuration file: {e}")
        raise

    #Make sure log directory exists
    for handler_name, handler in config.get('logging', {}).get('handlers', {}).items():
        if "filename" in handler.keys():
            Path(handler['filename']).parent.mkdir(parents=True, exist_ok=True)

    return config

@dataclass
class RuntimeContext:
    config: dict
    config_file: str | Path | None = None

    @classmethod
    def from_config_file(cls, config_file: str | Path):
        config = load_config_file(config_file)
        return cls(config=config, config_file=config_file)

    def __post_init__(self):
        if self.config is None:
            raise ValueError("RuntimeContext requires a config dictionary")
        self.initialize_runtime(self.config)

    def initialize_runtime(self, config: dict):

        logger.info("Initializing Runtime Context")

        ## Timezone
        self.default_timezone = config.get('api', {}).get('default_timezone', 'Europe/Rome')
        
        ## Resampling settings
        min_sample_size_cfg = config.get('resampling', {}).get('min_sample_size', 1)
        if isinstance(min_sample_size_cfg, dict):
            self.resample_min_sample_size = min_sample_size_cfg
        else:
            self.resample_min_sample_size = int(min_sample_size_cfg)
        self.resample_colmap = (
            config.get('resampling', {}).get('column_aggfuncs', DEFAULT_RESAMPLE_COLMAP).copy()
        )
        self.column_resampler = ColumnResampler(
            resample_colmap=self.resample_colmap,
            min_sample_size=self.resample_min_sample_size,
            day_start_hour=config.get('resampling', {}).get('day_start_hour'),
            day_end_hour=config.get('resampling', {}).get('day_end_hour'),
        )

        ## Providers
        self.provider_manager = ProviderManager(config['providers'])

        ## Database
        self.db = MeteoDB(config.get('database', {}).get('path', 'sqlite:///database.db'))

        ## Gapfinder
        self.gapfinder = Gapfinder()

        ## Query Manager
        query_cfg = config.get('query_manager', {})
        max_concurrent_requests = int(query_cfg.get('max_concurrent_requests', 3))
        cache_lag_minutes = int(query_cfg.get('cache_lag_minutes', 0))
        self.query_manager = QueryManager(
            max_concurrent_requests=max_concurrent_requests,
            cache_lag_minutes=cache_lag_minutes,
        )
       

    def update_runtime(self, config_file: str | Path):
        self.config_file = Path(config_file)
        self.config = load_config_file(self.config_file)
        self.initialize_runtime(self.config)

if __name__ == '__main__':
    logging.basicConfig(level = logging.DEBUG, force = True)
    runtime = RuntimeContext.from_config_file('config.example.yaml')
