#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from obscmd.cmds.obs.obsutil import join_bucket_key
from tests import BaseObsCommandTest


class TestMblogCommand(BaseObsCommandTest):

    cmd = 'obs mblog '

    def test_help(self):
        cmdline = '%s help' % self.cmd
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]
        self.assertIn('mblog', stdout)
        self.assertIn('DESCRIPTION', stdout)

    def test_set_log(self):
        bucket = self.bucket
        log_str = '{"targetBucket":"' + bucket + '","targetPrefix":""}'
        cmdline = '%s %s --log %s' % (self.cmd, join_bucket_key(self.bucket), log_str)
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]

        self.assertIn('complete', stdout)

    def test_notset_log(self):
        cmdline = '%s %s' % (self.cmd, join_bucket_key(self.bucket))
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]

        self.assertIn('complete', stdout)
