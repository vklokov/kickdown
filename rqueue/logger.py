import logging
import sys


class Logger:
    def __init__(self):
        self.logger = logging.getLogger("rqueue")
        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(
                logging.Formatter("%(asctime)s %(levelname)s %(message)s")
            )
            self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

    def info(self, msg: str, extra: dict | None = None):
        self.logger.info(msg, extra=extra or {})

    def error(self, msg: str, extra: dict | None = None):
        self.logger.error(msg, extra=extra or {})

    def debug(self, msg: str, extra: dict | None = None):
        self.logger.debug(msg, extra=extra or {})

    def warning(self, msg: str, extra: dict | None = None):
        self.logger.warning(msg, extra=extra or {})
