import logging
from logging.handlers import TimedRotatingFileHandler

_project_name = "chash4py"

_logger = logging.getLogger(_project_name)
_logger.setLevel(logging.INFO)

# for file
#_fh = TimedRotatingFileHandler(_project_name + ".log", when="midnight")
#_fh.setLevel(logging.INFO)

# for console
_ch = logging.StreamHandler()
_ch.setLevel(logging.INFO)

_format = "%(asctime)s [%(levelname)s] %(pathname)s %(funcName)s (%(lineno)s) - %(message)s"
_formatter = logging.Formatter(_format)
#_fh.setFormatter(_formatter)
_ch.setFormatter(_formatter)

# add handlers
#_logger.addHandler(_fh)
_logger.addHandler(_ch)

def get_logger():
    return _logger
