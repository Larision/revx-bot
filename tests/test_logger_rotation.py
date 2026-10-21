import logging
import unittest
from logging.handlers import RotatingFileHandler
from pathlib import Path
from tempfile import TemporaryDirectory

from config import LOG_BACKUP_COUNT, LOG_MAX_BYTES, LOG_PATH


class LoggerConfigurationTests(unittest.TestCase):
    def test_log_path_is_under_logs_directory(self):
        self.assertEqual(LOG_PATH.parent, Path("logs"))

    def test_rotating_handler_rolls_over_at_configured_size(self):
        with TemporaryDirectory() as temp_dir:
            log_path = Path(temp_dir) / "gridbot.log"
            handler = RotatingFileHandler(
                log_path,
                maxBytes=20,
                backupCount=1,
                encoding="utf-8",
            )
            logger = logging.getLogger("test_logger_rotation")
            logger.handlers.clear()
            logger.propagate = False
            logger.setLevel(logging.INFO)
            logger.addHandler(handler)

            logger.info("first log entry")
            logger.info("second log entry")
            handler.close()
            logger.handlers.clear()

            self.assertTrue(log_path.exists())
            self.assertTrue(log_path.with_name("gridbot.log.1").exists())

    def test_rotation_limits_are_configured(self):
        self.assertEqual(LOG_MAX_BYTES, 10 * 1024 * 1024)
        self.assertEqual(LOG_BACKUP_COUNT, 10)
