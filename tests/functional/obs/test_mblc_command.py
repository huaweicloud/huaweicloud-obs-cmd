#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from obscmd.cmds.obs.obsutil import join_bucket_key
from tests import BaseObsCommandTest


class TestMblcCommand(BaseObsCommandTest):

    cmd = 'obs mblc '

    def test_help(self):
        cmdline = '%s help' % self.cmd
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]
        self.assertIn('mblc', stdout)
        self.assertIn('DESCRIPTION', stdout)

    def test_set_lifecycle(self):
        cmdline = '%s %s %s' % (self.cmd, join_bucket_key(self.bucket),
                                            '{"baseinfo":{"status":"Enabled","rulename":"rule1","prefix":"test"},"transition":[{"storageClass":"WARM","days":30},{"storageClass":"COLD","days":60}],"noncurrentVersionTransition":[{"storageClass":"WARM","noncurrentDays":30},{"storageClass":"COLD","noncurrentDays":60}],"expiration":{"days":61},"noncurrentVersionExpiration":{"noncurrentDays":61}}')
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]
        self.assertIn('complete', stdout)
