import os
import pandas
import global_vars
import time

NORMAL_LOGS_FOLDER = 'logs'
EXP_LOGS_FOLDER = 'experiment_logs'
script_dir = os.path.dirname(__file__)  # <-- absolute dir the script is in


def write_log():
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    rel_path: str
    abs_file_path: str
    if global_vars.experiment:
        if not os.path.exists(EXP_LOGS_FOLDER):
            os.makedirs(EXP_LOGS_FOLDER)

        rel_path = EXP_LOGS_FOLDER + "\\log_" + timestamp + ".txt"
        abs_file_path = os.path.join(script_dir, rel_path)
        with open(abs_file_path, "w") as text_file:
            text_file.write(global_vars.exp_log.getvalue())
    else:
        if not os.path.exists(NORMAL_LOGS_FOLDER):
            os.makedirs(NORMAL_LOGS_FOLDER)

        rel_path = NORMAL_LOGS_FOLDER + "\\log_" + timestamp + ".txt"
        abs_file_path = os.path.join(script_dir, rel_path)
        with open(abs_file_path, "w") as text_file:
            text_file.write(global_vars.log.getvalue())
