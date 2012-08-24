import settings
import logging
from logging.handlers import RotatingFileHandler

class Logger(object):
    _instance = None
    
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(Logger, cls).__new__(
                                cls, *args, **kwargs)
            
            FORMAT = '%(asctime)-15s %(message)s'
            formatter = logging.Formatter(FORMAT)
            
            log = logging.getLogger('MyLogger')
            log.setLevel(logging.DEBUG)
            
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            log.addHandler(console_handler)
            
            file_handler = RotatingFileHandler(settings.LOG_FILENAME, maxBytes=1024*10000, backupCount=10)
            file_handler.setFormatter(formatter)
            log.addHandler(file_handler)
            cls._instance.log = log
            
        return cls._instance
    

