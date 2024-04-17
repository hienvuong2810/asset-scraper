import logging
import datetime
import os
import sys


class LoggingCounter(logging.Handler):
    def __init__(self):
        super().__init__()
        self.warning_count = 0
        self.error_count = 0

    def emit(self, record):
        if record.levelno == logging.WARNING:
            self.warning_count += 1
        elif record.levelno == logging.ERROR:
            self.error_count += 1


if not os.path.exists("logs/"):
    os.makedirs("logs")
logging.basicConfig(
    filename=f"logs/{str(datetime.date.today())}.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s",
)

logger = logging.getLogger()

logging_counter = LoggingCounter()
logger.addHandler(logging_counter)
logger.addHandler(logging.StreamHandler())
