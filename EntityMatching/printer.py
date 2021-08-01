import logging
import global_vars

import loggers

loggers.init()


def log(text, destinations):
    for d in destinations:
        if 'exp' in d and global_vars.experiment is False: continue
        if 'exp' not in d and global_vars.experiment is True: continue
        logging.getLogger(d).debug(text)
