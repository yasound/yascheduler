#!/usr/bin/env python
from os.path import abspath, dirname

PROJECT_ROOT = abspath(dirname(__file__))

activate_this = PROJECT_ROOT + "/../vtenv/bin/activate_this.py"
execfile(activate_this, dict(__file__=activate_this))

from radio_scheduler import RadioScheduler

import argparse
from logger import Logger
import settings


def interrupt_handler(signum, frame):
    print 'interrupt !!!'
    scheduler.stop()

if __name__ == "__main__":
    Logger().log.info('starting scheduler')

    parser = argparse.ArgumentParser(description='Run yascheduler.')
    parser.add_argument('--disable_ping', '-p', dest='disable_ping', action='store_const', const=True, default=False, help='disable streamer checking and ping, disable removal of dead streamers')
    parser.add_argument('--flush', '-f', dest='flush', action='store_const', const=True, default=False, help='flush yascheduler data and exit (does not start yascheduler main process)')
    parser.add_argument('--check_programming', '-c', dest='check_programming', action='store_const', const=True, default=False, help='adds events to regularly verify if radios programming is ok')
    parser.add_argument('--time_profile', '-t', dest='time_profile', action='store_const', const=True, default=False, help='add time profile logs')
    parser.add_argument('--radio_offset', '-o', dest='radio_offset', type=int, action='store', default=None, help='radio offset to get the list of radios for this scheduler instance')
    parser.add_argument('--radio_limit', '-l', dest='radio_limit', type=int, action='store', default=None, help='radio count to get the list of radios for this scheduler instance')

    args = parser.parse_args()
    enable_ping = not args.disable_ping
    flush = args.flush
    check_programming = args.check_programming
    time_profile = args.time_profile
    radio_offset = args.radio_offset
    radio_limit = args.radio_limit

    if radio_offset == None:
        radio_offset = settings.default_radio_offset
    if radio_limit == None:
        radio_limit = settings.default_radio_limit

    Logger().log.info('enable streamers ping = %s' % (enable_ping))
    Logger().log.info('enable programming check = %s' % (check_programming))
    Logger().log.info('enable time profiling = %s' % (time_profile))
    Logger().log.info('flush = %s' % (flush))
    Logger().log.info('radio offset = %s' % (radio_offset))
    Logger().log.info('radio limit = %s' % (radio_limit))

    scheduler = RadioScheduler(enable_ping_streamers=enable_ping, enable_programming_check=check_programming, enable_time_profiling=time_profile, radio_offset=radio_offset, radio_limit=radio_limit)

    if flush:
        scheduler.flush()
    else:
        import signal
        signal.signal(signal.SIGINT, interrupt_handler)
        scheduler.run()



