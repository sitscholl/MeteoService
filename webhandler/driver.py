from typing import Optional
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import logging

logger = logging.getLogger(__name__)

class Driver:
    """A context manager class for handling Chrome WebDriver initialization and cleanup.
    
    This class provides a convenient way to create and manage a Chrome WebDriver instance
    with various configuration options. It can be used with a context manager (with statement)
    to ensure proper cleanup of resources.
    
    Attributes:
        download_dir (str): Directory where downloads will be saved
        user_dir (Optional[str]): User data directory for Chrome
        headless (bool): Whether to run Chrome in headless mode
        simulate_slow_conn (bool): Whether to simulate slow network conditions
        driver (Optional[webdriver]): The Selenium WebDriver instance
    """
    
    def __init__(
        self,
        download_dir: str | None = None,
        user_dir: str | None = None,
        headless: bool = True,
        simulate_slow_conn: bool = False
    ):
        """Initialize the Driver with the given configuration.
        
        Args:
            download_dir: Directory where downloads will be saved
            user_dir: Optional user data directory for Chrome
            headless: Whether to run Chrome in headless mode
            simulate_slow_conn: Whether to simulate slow network conditions
        """
        self.download_dir = download_dir
        self.user_dir = user_dir
        self.headless = headless
        self.simulate_slow_conn = simulate_slow_conn
        self.driver: webdriver | None = None
        
    def _configure_options(self) -> Options:
        """Configure and return Chrome options.
        
        Returns:
            Options: Configured Chrome options
        """
        options = Options()
        
        # Basic Chrome arguments
        chrome_args = [
            "--disable-search-engine-choice-screen",
            "--start-maximized",
            "--window-size=1920,1080",
            "--no-sandbox",
            "--no-gpu",
            "--disable-extensions",
            "--dns-prefetch-disable"
        ]
        
        # Add headless mode if requested
        if self.headless:
            chrome_args.append("--headless")
            
        # Add all arguments to options
        for arg in chrome_args:
            options.add_argument(arg)
            
        # Configure user directory if specified
        if self.user_dir:
            options.add_argument(f"user-data-dir={self.user_dir}")
            
        # Configure download preferences
        if self.download_dir:
            prefs = {
                "download.default_directory": self.download_dir,
                "download.directory_upgrade": True,
                "download.prompt_for_download": False,
            }
            options.add_experimental_option("prefs", prefs)
        
        return options

    def _setup_driver(self):
        """
        Set up and configure the Chrome driver using webdriver_manager.
        """ 
        try:
            # Use webdriver_manager to automatically handle driver installation
            service = Service(ChromeDriverManager().install())
            chrome_options = self._configure_options()

            self.driver = webdriver.Chrome(
                service=service,
                options=chrome_options
            )

            # Configure timezone
            tz_params = {'timezoneId': 'Europe/Rome'}
            self.driver.execute_cdp_cmd('Emulation.setTimezoneOverride', tz_params)
            
            # Configure network conditions if requested
            if self.simulate_slow_conn:
                self.driver.set_network_conditions(
                    offline=False,
                    latency=5,  # additional latency (ms)
                    download_throughput=500 * 1024,  # maximal throughput
                    upload_throughput=500 * 1024  # maximal throughput
                )
            
            # Set implicit wait timeout
            self.driver.implicitly_wait(30)

            logger.info("Chrome driver initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize Chrome driver: {str(e)}")
            raise
        
    def __enter__(self) -> webdriver:
        """Initialize and return the WebDriver when entering the context.
        
        Returns:
            WebDriver: The configured Chrome WebDriver instance
            
        Raises:
            Exception: If driver initialization fails
        """
        self._setup_driver()
        return self.driver
            
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Clean up the WebDriver when exiting the context.
        
        Args:
            exc_type: The type of the exception that occurred, if any
            exc_val: The instance of the exception that occurred, if any
            exc_tb: The traceback of the exception that occurred, if any
        """
        if self.driver:
            try:
                self.driver.quit()
                logger.info('Browser closed successfully.')
            except Exception as e:
                logger.warning(f"Error while closing browser: {e}")
            finally:
                self.driver = None
