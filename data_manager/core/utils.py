# app/core/setup.py
import logging
from pathlib import Path
import signal
import sys


def setup_logging(config):
    """
    Configure application-wide logging based on settings in the configuration.

    Args:
        config (dict): Application configuration containing logging settings.
    """
    log_path = Path(config["logging"]["file_path"])
    
    try:
        log_path.parent.mkdir(exist_ok=True)
        
        logging.basicConfig(
            level=getattr(logging, config["logging"]["level"], logging.INFO),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_path),
                logging.StreamHandler(sys.stdout)
            ]
        )
    except (IOError, PermissionError) as e:
        # Fall back to console-only logging if file logging fails
        print(f"Warning: Could not set up file logging: {e}")
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler(sys.stdout)]
        )

def setup_signal_handling(app, analytics, logger):
    """
    Configure graceful shutdown on SIGINT and SIGTERM signals.

    Args:
        app (QApplication): The Qt application instance.
        analytics (AnalyticsEngine): Instance of the analytics engine.
        logger (logging.Logger): Logger to record shutdown events.
    """
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
        analytics.close()
        app.quit()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

def setup_exception_hook(logger):
    """
    Configure a global exception hook to log all uncaught exceptions.

    Args:
        logger (logging.Logger): Logger to record exception details.
    """
    def exception_hook(exctype, value, traceback):
        logger.critical("Uncaught exception:", exc_info=(exctype, value, traceback))
        sys.__excepthook__(exctype, value, traceback)  # Invoke default handler.
    
    sys.excepthook = exception_hook
