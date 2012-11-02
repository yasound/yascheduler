#!/usr/bin/env python
from os.path import abspath, dirname

PROJECT_ROOT = abspath(dirname(__file__))

activate_this = PROJECT_ROOT + "/../vtenv/bin/activate_this.py"
execfile(activate_this, dict(__file__=activate_this))

from radio_scheduler import RadioScheduler

import argparse
from logger import Logger

if __name__ == "__main__":
    Logger().log.info('starting scheduler')

    parser = argparse.ArgumentParser(description='Run yascheduler.')
    parser.add_argument('--disable_ping', '-p', dest='disable_ping', action='store_const', const=True, default=False, help='disable streamer checking and ping, disable removal of dead streamers')
    parser.add_argument('--flush', '-f', dest='flush', action='store_const', const=True, default=False, help='flush yascheduler data and exit (does not start yascheduler main process)')
    parser.add_argument('--check_programming', '-c', dest='check_programming', action='store_const', const=True, default=False, help='adds events to regularly verify if radios programming is ok')

    args = parser.parse_args()
    enable_ping = not args.disable_ping
    flush = args.flush
    check_programming = args.check_programming

    Logger().log.info('enable streamers ping = %s' % (enable_ping))
    Logger().log.info('enable programming check = %s' % (check_programming))
    Logger().log.info('flush = %s' % (flush))

    scheduler = RadioScheduler(enable_ping_streamers=enable_ping, enable_programming_check=check_programming)

    if flush:
        scheduler.flush()
    else:
        scheduler.run()
