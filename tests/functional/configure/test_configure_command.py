#!/usr/bin/env python
# -*- coding: UTF-8 -*-
from obscmd.testutils import BaseHcmdCommandParamsTest


class TestConfigureCommand(BaseHcmdCommandParamsTest):

    cmd = 'configure'

    def test_help(self):
        cmdline = '%s help' % self.cmd
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]
        self.assertIn('configure', stdout)
        self.assertIn('DESCRIPTION', stdout)