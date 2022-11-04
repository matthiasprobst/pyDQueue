import logging
import pathlib
from logging.handlers import RotatingFileHandler
from typing import Tuple, Union

DEFAULT_LOGGING_LEVEL = logging.INFO


def create_package_logger(log_dir: Union[str, pathlib.Path],
                          name: str) -> Tuple[logging.Logger,
                                              RotatingFileHandler,
                                              logging.StreamHandler]:
    """Create logger based on name"""
    log_dir = pathlib.Path(log_dir)
    log_filename = log_dir / name
    log_filename.unlink(missing_ok=True)
    _logdir = pathlib.Path(log_filename)

    # Initialize logger, set high level to prevent ipython debugs. File level is
    # set below
    _logger = logging.getLogger(name)
    _logger.setLevel(DEFAULT_LOGGING_LEVEL)

    _formatter = logging.Formatter(
        '%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
        datefmt='%Y-%m-%d_%H:%M:%S')

    _file_handler = RotatingFileHandler(_logdir / f'{name}.log', maxBytes=int(5e6), backupCount=2)
    _file_handler.setLevel(DEFAULT_LOGGING_LEVEL)
    _file_handler.setFormatter(_formatter)

    _stream_handler = logging.StreamHandler()
    _stream_handler.setLevel(DEFAULT_LOGGING_LEVEL)
    _stream_handler.setFormatter(_formatter)

    _logger.addHandler(_file_handler)
    _logger.addHandler(_stream_handler)

    return _logger
