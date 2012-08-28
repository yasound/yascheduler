#!/usr/bin/env python
from os.path import abspath, dirname

PROJECT_ROOT = abspath(dirname(__file__))

activate_this = PROJECT_ROOT + "/../vtenv/bin/activate_this.py"
execfile(activate_this, dict(__file__=activate_this))

from radio_scheduler import RadioScheduler

import argparse

parser = argparse.ArgumentParser(description='Run yascheduler.')
parser.add_argument('--disable_ping', '-p', dest='disable_ping', action='store_const', const=True, default=False, help='disable streamer checking and ping, disable removal of dead streamers')

args = parser.parse_args()
enable_ping = not args.disable_ping

scheduler = RadioScheduler(enable_ping_streamers=enable_ping)
scheduler.run()
