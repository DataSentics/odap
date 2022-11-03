import logging

logger = logging.getLogger("odap_logger")
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(message)s")
console_handler.setFormatter(formatter)

logger.addHandler(console_handler)
