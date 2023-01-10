import logging

logger = logging.getLogger("odap_logger")
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
formatter = logging.Formatter("%(levelname)s - %(asctime)s - %(message)s")
console_handler.setFormatter(formatter)

logger.addHandler(console_handler)
