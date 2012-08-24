#!/usr/bin/env python
from os.path import abspath, dirname

PROJECT_ROOT = abspath(dirname(__file__))

activate_this = PROJECT_ROOT + "/../vtenv/bin/activate_this.py"
execfile(activate_this, dict(__file__=activate_this))

from radio_scheduler import RadioScheduler

scheduler = RadioScheduler()
scheduler.test()
