import logging
import global_vars

import loggers

loggers.init()

ALL_OUTPUTS = (global_vars.LOG, global_vars.CONSOLE, global_vars.EXP_LOG, global_vars.EXP_CONSOLE)
CONSOLES = (global_vars.CONSOLE, global_vars.EXP_CONSOLE)
FILES = (global_vars.LOG, global_vars.EXP_LOG)


def log(destinations, *text):
    for d in destinations:
        if 'exp' in d and global_vars.experiment is False: continue
        if 'exp' not in d and global_vars.experiment is True: continue
        if 'console' in d and global_vars.verbose_console is False: continue
        if 'log' in d and global_vars.verbose_file is False: continue
        logging.getLogger(d).debug(' '.join(map(str, text)))
