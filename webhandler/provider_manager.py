from pathlib import Path
import importlib
import pkgutil
import inspect
from typing import Dict, Type, Optional

class ProviderManager:
    """
    Manager class for automatically discovering and registering meteorological data providers.
    
    This class scans the meteo package for provider classes and maintains a registry
    for easy access to provider implementations.
    """
    
    def __init__(self, ignore_modules: list[str] = None):
        """
        Initialize the provider manager and discover all available providers.
        
        Args:
            ignore_modules: List of module names to ignore during discovery
        """
        if ignore_modules is None:
            ignore_modules = ['base', '__pycache__']
            
        self.registry: Dict[str, Type] = {}
        self._discover_providers(ignore_modules)
    
    def _discover_providers(self, ignore_modules: list[str]):
        """
        Discover and import all provider modules in the meteo package.
        
        Args:
            ignore_modules: List of module names to ignore
        """
        # Get the meteo package directory relative to this file
        current_dir = Path(__file__).parent
        meteo_dir = current_dir / 'meteo'
        
        if not meteo_dir.exists():
            raise ImportError(f"Meteo directory not found at {meteo_dir}")
        
        # Import all modules in the meteo package
        meteo_package = 'webhandler.meteo'
        
        for _, module_name, _ in pkgutil.iter_modules([str(meteo_dir)]):
            if module_name not in ignore_modules:
                try:
                    module = importlib.import_module(f'{meteo_package}.{module_name}')
                    self._register_providers_from_module(module)
                except ImportError as e:
                    print(f"Warning: Could not import module {module_name}: {e}")
    
    def _register_providers_from_module(self, module):
        """
        Register all provider classes found in a module.
        
        Args:
            module: The imported module to scan for providers
        """
        for name, obj in inspect.getmembers(module, inspect.isclass):
            # Check if the class has a provider_name attribute and is not the base class
            if (hasattr(obj, 'provider_name') and 
                obj.__module__ == module.__name__ and  # Only classes defined in this module
                name != 'BaseMeteoHandler'):
                
                self.registry[obj.provider_name] = obj
    
    def get_provider(self, provider_name: str) -> Optional[Type]:
        """
        Get a provider class by its name.
        
        Args:
            provider_name: The name of the provider to retrieve
            
        Returns:
            The provider class if found, None otherwise
        """
        return self.registry.get(provider_name)
    
    def list_providers(self) -> list[str]:
        """
        Get a list of all registered provider names.
        
        Returns:
            List of provider names
        """
        return list(self.registry.keys())
    
    def create_provider(self, provider_name: str, **kwargs):
        """
        Create an instance of a provider by name.
        
        Args:
            provider_name: The name of the provider to create
            **kwargs: Arguments to pass to the provider constructor
            
        Returns:
            An instance of the requested provider
            
        Raises:
            ValueError: If the provider is not found
        """
        provider_class = self.get_provider(provider_name)
        if provider_class is None:
            raise ValueError(f"Provider '{provider_name}' not found. Available providers: {self.list_providers()}")
        
        return provider_class(**kwargs)