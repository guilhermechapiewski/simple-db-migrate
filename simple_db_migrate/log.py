import logging
import os
from datetime import datetime

class LOG(object):
    logger = None
    def __init__(self, log_dir):
        if log_dir:
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
            self.logger = logging.getLogger('simple-db-migrate')

            now = datetime.now()
            filename = "%s/%s.log" %(os.path.abspath(log_dir), now.strftime("%Y%m%d%H%M%S"))
            hdlr = logging.FileHandler(filename)
            formatter = logging.Formatter('%(message)s')
            hdlr.setFormatter(formatter)
            self.logger.addHandler(hdlr)
            self.logger.setLevel(logging.DEBUG)

    def debug(self, msg):
        if self.logger:
            self.logger.debug(msg)

    def info(self, msg):
        if self.logger:
            self.logger.info(msg)

    def error(self, msg):
        if self.logger:
            self.logger.error(msg)

    def warn(self, msg):
        if self.logger:
            self.logger.warn(msg)
