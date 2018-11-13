#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from obscmd.cmds.obs.obsutil import join_bucket_key
from tests import BaseObsCommandTest


class TestRblcCommand(BaseObsCommandTest):

    cmd = 'obs rblc '

    def test_help(self):
        cmdline = '%s help' % self.cmd
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]
        self.assertIn('rblc', stdout)
        self.assertIn('DESCRIPTION', stdout)

    def test_delete_all_lifecycle(self):

        cmdline = '%s %s' % (self.cmd, join_bucket_key(self.bucket))
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]

        self.assertIn('complete', stdout)

