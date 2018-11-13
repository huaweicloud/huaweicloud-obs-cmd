#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from obscmd.testutils import BaseHcmdCommandParamsTest
from obscmd.testutils import unittest
from tests import BaseObsCommandTest


class TestSurlCommand(BaseObsCommandTest):
    cmd = 'obs surl'

    def test_help(self):
        cmdline = '%s help' % self.cmd
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]
        self.assertIn('NAME', stdout)
        self.assertIn('DESCRIPTION', stdout)

    def test_surl(self):

        cmdline = '%s GET --obspath %s --expires 200' % (self.cmd, self.fullbucket)
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]
        print(stdout)
        self.assertIn('http', stdout)


