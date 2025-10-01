from pathlib import Path
import importlib
import pkgutil

class ProviderManager:

    registry = {}

    def __init__(self, ignore_modules = ['base', 'provider_manager']):

        package_dir = Path(__file__).resolve().parent
        for _, name, _ in pkgutil.iter_modules([package_dir]):
            if name not in ignore_modules:
                _ = importlib.import_module(str(Path(package_dir, name)))

    def __init_subclass__(cls, **kwargs):
        ProviderManager.registry[cls.provider_name] = cls

    def get_provider(self, provider_name: str):
        return self.registry.get(provider_name)