import os
import platform

class Config:
    BASE_DIR = os.path.abspath(os.path.dirname(__file__))
    ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, "..")) # , ".."
    
    # Data and log paths
    DATA_CSV_PATH = os.path.join(BASE_DIR, "data", "csv")
    LOG_PATH = os.path.join(BASE_DIR, "log_files")
    LOG_PREFIX = "Log_File"
    SCRIPTS_PATH = os.path.join(BASE_DIR, "scripts")
    TEMP_PATH = os.path.join(BASE_DIR, "temp")  # New temp folder
    
    # Subdirectories for CSV processing
    FILTERED_URL_PATH = os.path.join(DATA_CSV_PATH, "filtered_url")
    UPDATED_NAME_PATH = os.path.join(DATA_CSV_PATH, "updated_name")
    UPDATED_URL_PATH = os.path.join(DATA_CSV_PATH, "updated_url")
    DOMAIN_ABOUT_PATH = os.path.join(DATA_CSV_PATH, "domain_about")
    EMAILS_PATH = os.path.join(DATA_CSV_PATH, "emails")
    VERIFIED_EMAILS_PATH = os.path.join(DATA_CSV_PATH, "verified")
    
    # Tor configuration
    TOR_BASE_PATH = os.path.join(ROOT_DIR, "config", "tor")
    OS_TYPE = platform.system().lower()
    if OS_TYPE == "windows":
        TOR_EXECUTABLE = os.path.join(TOR_BASE_PATH, "windows", "tor.exe")
        CHROMEDRIVER_PATH = os.path.join(ROOT_DIR, "config", "drivers", "chromedriver-win64", "chromedriver.exe")
        GECKODRIVER_PATH = os.path.join(ROOT_DIR, "config", "drivers", "geckodriver-win64", "geckodriver.exe")
    elif OS_TYPE == "linux":
        TOR_EXECUTABLE = os.path.join(TOR_BASE_PATH, "linux", "tor")
        CHROMEDRIVER_PATH = os.path.join(ROOT_DIR, "config", "drivers", "chromedriver-linux64", "chromedriver")
        GECKODRIVER_PATH = os.path.join(ROOT_DIR, "config", "drivers", "geckodriver-linux64", "geckodriver")
    elif OS_TYPE == "darwin":  # macOS
        TOR_EXECUTABLE = os.path.join(TOR_BASE_PATH, "macos", "tor")
        CHROMEDRIVER_PATH = os.path.join(ROOT_DIR, "config", "drivers", "chromedriver-mac64", "chromedriver")
        GECKODRIVER_PATH = os.path.join(ROOT_DIR, "config", "drivers", "geckodriver-mac64", "geckodriver")
    else:
        TOR_EXECUTABLE = None
        CHROMEDRIVER_PATH = None
        GECKODRIVER_PATH = None
    
    # LinkedIn Chrome profile configuration
    CHROME_PROFILE_BASE_PATH = os.path.join(ROOT_DIR, "config", "chrome_profiles", "Default")
    LINKEDIN_PROFILE_DIR = os.path.join(CHROME_PROFILE_BASE_PATH, "LinkedInProfile")
    CHROME_PROFILE_PATH = os.path.join(os.environ.get("USERPROFILE", ""), "AppData", "Local", "Google", "Chrome", "User Data", "Default")  # Fallback to system path
    
    # Environment variable overrides
    TOR_EXECUTABLE = os.getenv("TOR_EXECUTABLE", TOR_EXECUTABLE)
    LINKEDIN_PROFILE_DIR = os.getenv("LINKEDIN_PROFILE_DIR", LINKEDIN_PROFILE_DIR)
    CHROMEDRIVER_PATH = None # os.getenv("CHROMEDRIVER_PATH", CHROMEDRIVER_PATH)
    GECKODRIVER_PATH = None # os.getenv("GECKODRIVER_PATH", GECKODRIVER_PATH)
    
    # Flask settings
    SECRET_KEY = os.urandom(24)
    
    # Ensure directories exist
    @staticmethod
    def init_dirs():
        for path in [
            Config.DATA_CSV_PATH,
            Config.LOG_PATH,
            Config.TEMP_PATH,
            Config.FILTERED_URL_PATH,
            Config.UPDATED_NAME_PATH,
            Config.UPDATED_URL_PATH,
            Config.DOMAIN_ABOUT_PATH,
            Config.EMAILS_PATH,
            Config.VERIFIED_EMAILS_PATH,
            Config.CHROME_PROFILE_BASE_PATH,

            os.path.join(Config.ROOT_DIR, "config", "drivers", "chromedriver-win64"),
            os.path.join(Config.ROOT_DIR, "config", "drivers", "geckodriver-win64")
        ]:
            os.makedirs(path, exist_ok=True)
    
    @staticmethod
    def verify_drivers():
        """Verify that driver executables exist."""
        for driver_path in [Config.CHROMEDRIVER_PATH, Config.GECKODRIVER_PATH, Config.TOR_EXECUTABLE]:
            if driver_path and not os.path.exists(driver_path):
                print(f"Warning: Driver not found at {driver_path}")
            else:
                print(f"Driver found at {driver_path}")