import pandas
import global_vars


def write_log():
    with open("logs/log_" + pandas.Timestamp.now() + ".txt", "w") as text_file:
        text_file.write(global_vars.log.getvalue())
