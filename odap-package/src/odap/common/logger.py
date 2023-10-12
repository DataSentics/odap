import logging


def is_loguru_installed():
    try:
        import loguru  # pylint: disable=import-outside-toplevel,unused-import

        return True
    except ModuleNotFoundError:
        pass

    return False


def instantiate_simple_logger():
    if logger:
        return logger

    _logger = logging.getLogger("odap_logger")
    _logger.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    formatter = logging.Formatter("%(levelname)s - %(asctime)s - %(message)s")
    console_handler.setFormatter(formatter)

    _logger.addHandler(console_handler)
    return _logger


def instantiate_logger():
    if is_loguru_installed():
        from loguru import logger as loguru_logger  # pylint: disable=import-outside-toplevel,unused-import

        return loguru_logger

    return instantiate_simple_logger()


logger = instantiate_logger()
