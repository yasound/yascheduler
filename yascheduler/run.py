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
    parser.add_argument('--flush', '-f', dest='flush', action='store_const', const=True, default=False, help='flush yascheduler data before running: restart radios without streamers and listeners')

    args = parser.parse_args()
    enable_ping = not args.disable_ping
    flush = args.flush

    Logger().log.info('enable_ping = %s' % (enable_ping))
    Logger().log.info('flush = %s' % (flush))
    Logger().log.info('dev mode = %s' % (settings.DEVELOPMENT_MODE))

    scheduler = RadioScheduler(enable_ping_streamers=enable_ping, flush=flush)
    scheduler.run()
