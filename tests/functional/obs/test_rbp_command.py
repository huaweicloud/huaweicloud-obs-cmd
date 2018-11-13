#!/usr/bin/env python
# -*- coding: UTF-8 -*-
from obscmd.cmds.obs.obsutil import join_bucket_key
from obscmd.testutils import FileCreator
from tests import BaseObsCommandTest


class TestRbpCommand(BaseObsCommandTest):

    cmd = 'obs rbp '

    def test_help(self):
        cmdline = '%s help' % self.cmd
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]
        self.assertIn('NAME', stdout)
        self.assertIn('DESCRIPTION', stdout)

    def test_delete_policy_from_json(self):
        policy = '{"Version":"2008-10-17","Id":"Policy2","Statement":[{"Sid":"Stmt1375240018061",' \
                 '"Effect":"Allow","Principal":{"AWS":["arn:aws:iam::userid:root"]},' \
                 '"Action":["s3:GetBucketPolicy"],"Resource":["arn:aws:s3:::%s"]}]}' % self.bucket

        cmdline = 'obs mbp %s %s' % (join_bucket_key(self.bucket), policy)
        self.run_cmd(cmdline, expected_rc=0)

        cmdline = 'obs infob %s --policy' % join_bucket_key(self.bucket)
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]
        self.assertIn('Policy2', stdout)

        cmdline = '%s %s' % (self.cmd, join_bucket_key(self.bucket))
        self.run_cmd(cmdline, expected_rc=0)

        cmdline = 'obs infob %s --policy' % join_bucket_key(self.bucket)
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]
        self.assertNotIn('Policy2', stdout)

