# data_manager/data_manager.py
import sys
import logging
from PyQt6.QtWidgets import QApplication
from PyQt6.QtGui import QGuiApplication
from core.analytics import AnalyticsEngine
from core.config import load_config
from core.utils import setup_logging, setup_signal_handling, setup_exception_hook

def main():

    # Normal application startup
    config = load_config()
    setup_logging(config)
    logger = logging.getLogger(__name__)

    try:
        logger.info("Starting Application...")
        
        # Configure High DPI settings if enabled in the configuration.
        if config["display"].get("enable_high_dpi", False):
            QApplication.setHighDpiScaleFactorRoundingPolicy(
                QGuiApplication.highDpiScaleFactorRoundingPolicy()
            )
        
        app = QApplication(sys.argv)
        app.setApplicationName(config["application"].get("name", "Goodreads Analytics Tool"))
        app.setApplicationVersion(config["application"].get("version", "0.0.1"))
        
        # Initialize the analytics engine with the loaded configuration.
        # The engine will determine if we need setup or can go straight to the main interface
        analytics = AnalyticsEngine(config)
        
        # Setup graceful shutdown signal handling.
        setup_signal_handling(app, analytics, logger)
        
        # Setup global exception hook to log any unhandled exceptions.
        setup_exception_hook(logger)
        
        # Start the Qt event loop.
        return app.exec()
        
    except Exception as e:
        logger.error(f"Application failed to start: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())