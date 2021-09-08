import logging
import global_vars

import loggers

loggers.init()

ALL_OUTPUTS = (global_vars.LOG, global_vars.CONSOLE, global_vars.EXP_LOG, global_vars.EXP_CONSOLE)
CONSOLES = (global_vars.CONSOLE, global_vars.EXP_CONSOLE)
FILES = (global_vars.LOG, global_vars.EXP_LOG)


def log(*text, destinations, important=False):
    for d in destinations:
        if 'exp' in d and global_vars.experiment is False: continue
        if 'exp' not in d and global_vars.experiment is True: continue
        if 'console' in d and global_vars.verbose_console is False: continue
        if 'log' in d and global_vars.verbose_file is False: continue
        if important is True:
            logging.getLogger(d).debug(' '.join(map(str, text)))
            continue
        # if 'Evaluation' not in text[0] and global_vars.evaluation_only is True and 'Iteration' not in text[0]: continue
        if ('\t\t\t' in text[0] or '\t\t\t\t' in text[0] or '\t\t\t\t\t' in text[0] or '\t\t\t\t\t\t' in text[0]) \
                and global_vars.analytical_output is False:
            return
        logging.getLogger(d).debug(' '.join(map(str, text)))
