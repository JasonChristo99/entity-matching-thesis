import global_vars
import logging
import os
import sys
import time

setup = False


def init():
    global setup
    if setup:
        return
    setup = True
    timestamp = time.strftime("%Y%m%d-%H%M%S")

    NORMAL_LOGS_FOLDER = 'logs'
    EXP_LOGS_FOLDER = 'experiment_logs'
    if not os.path.exists(EXP_LOGS_FOLDER):
        os.makedirs(EXP_LOGS_FOLDER)
    if not os.path.exists(NORMAL_LOGS_FOLDER):
        os.makedirs(NORMAL_LOGS_FOLDER)

    # logging.basicConfig(level=logging.DEBUG)
    # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    formatter = logging.Formatter('%(asctime)s - %(message)s')

    log = logging.getLogger(global_vars.LOG)
    hdlr1 = logging.FileHandler(filename=NORMAL_LOGS_FOLDER + '\\log_' + timestamp + '.txt', mode='w')
    hdlr1.setFormatter(formatter)
    log.setLevel(logging.DEBUG)
    log.addHandler(hdlr1)

    exp_log = logging.getLogger(global_vars.EXP_LOG)
    hdlr2 = logging.FileHandler(filename=EXP_LOGS_FOLDER + '\\log_' + timestamp + '.txt', mode='w')
    hdlr2.setFormatter(formatter)
    exp_log.setLevel(logging.DEBUG)
    exp_log.addHandler(hdlr2)

    console = logging.getLogger(global_vars.CONSOLE)
    hdlr3 = logging.StreamHandler(sys.stdout)
    hdlr3.setFormatter(formatter)
    console.setLevel(logging.DEBUG)
    console.addHandler(hdlr3)

    exp_console = logging.getLogger(global_vars.EXP_CONSOLE)
    hdlr4 = logging.StreamHandler(sys.stdout)
    hdlr4.setFormatter(formatter)
    exp_console.setLevel(logging.DEBUG)
    exp_console.addHandler(hdlr4)
