#!/usr/bin/env python
from os.path import abspath, dirname
import sys
PROJECT_ROOT = abspath(dirname(__file__))

activate_this = PROJECT_ROOT + "/../vtenv/bin/activate_this.py"
execfile(activate_this, dict(__file__=activate_this))
import unittest

class AllTests():
    def suite(self): #Function stores all the modules to be tested
        modules_to_test = ('tests',)
        alltests = unittest.TestSuite()
        for module in map(__import__, modules_to_test):
            alltests.addTest(unittest.findTestCases(module))
        return alltests

s = AllTests()
unittest.main(defaultTest='s.suite')
