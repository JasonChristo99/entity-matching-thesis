import logging
import unittest

from EntityMatching import printer, global_vars


class MyTestCase(unittest.TestCase):
    def test_something(self):
        # print('CONSOLE', global_vars.exp_console.getvalue())
        # print('LOG', global_vars.exp_log.getvalue())
        # printer.print_advanced('TEST', [global_vars.log_dict["exp_log"], global_vars.log_dict["exp_console"]])
        # print('CONSOLE', global_vars.exp_console.getvalue())
        # print('LOG', global_vars.exp_log.getvalue())
        # self.assertEqual(global_vars.exp_log.getvalue(), 'TEST\n')
        # printer.print_advanced('TEST', None)
        # logging.basicConfig(level=logging.DEBUG)
        # ch = logging.FileHandler(filename='log.txt', mode='w')
        # print(ch.level)
        # ch.setLevel(level=logging.DEBUG)
        # print(ch.level)
        # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        # ch.setFormatter(formatter)
        # LOG1 = 'log1'
        # log = logging.getLogger(LOG1)
        # print(log.isEnabledFor(logging.DEBUG), log.level)
        # log.addHandler(ch)
        # print(log.isEnabledFor(logging.DEBUG), log.level)
        # log.log(level=logging.DEBUG, msg='RTEST')
        # global_vars.log.debug('BLA')
        # logging.getLogger(global_vars.EXP_LOG).info('BLA BLA')
        # logging.getLogger(global_vars.EXP_LOG).debug('BLA')
        # printer.log('TEST',[global_vars.LOG, global_vars.EXP_LOG, global_vars.CONSOLE, global_vars.EXP_CONSOLE])
        printer.log('BLA', 'TEST', destinations=[global_vars.EXP_LOG])
        # self.assertEqual(global_vars.exp_log.getvalue(), 'TEST\n')


if __name__ == '__main__':
    unittest.main()
