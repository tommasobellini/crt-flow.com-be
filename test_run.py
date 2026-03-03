import scanner
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info("Test starting...")
try:
    scanner.main()
except Exception as e:
    logger.error(f"Error: {e}")
logger.info("Test finished.")
