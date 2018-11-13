#!/usr/bin/env python
# -*- coding: UTF-8 -*-
from obscmd.cmds.obs.obsutil import join_bucket_key
from obscmd.testutils import FileCreator
from tests import BaseObsCommandTest


class TestMbpCommand(BaseObsCommandTest):

    cmd = 'obs mbp '

    def test_help(self):
        cmdline = '%s help' % self.cmd
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]
        self.assertIn('NAME', stdout)
        self.assertIn('DESCRIPTION', stdout)

    def test_make_polic_from_json(self):
        policy = '{"Version":"2008-10-17","Id":"Policy2","Statement":[{"Sid":"Stmt1375240018061",' \
                 '"Effect":"Allow","Principal":{"AWS":["arn:aws:iam::userid:root"]},' \
                 '"Action":["s3:GetBucketPolicy"],"Resource":["arn:aws:s3:::%s"]}]}' % self.bucket

        cmdline = '%s %s %s' % (self.cmd, join_bucket_key(self.bucket), policy)
        self.run_cmd(cmdline, expected_rc=0)

        cmdline = 'obs infob %s --policy' % join_bucket_key(self.bucket)
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]
        self.assertIn('Policy2', stdout)

    def test_make_polic_from_file(self):
        policy = '{"Version":"2008-10-17","Id":"Policy2","Statement":[{"Sid":"Stmt1375240018061",' \
                 '"Effect":"Allow","Principal":{"AWS":["arn:aws:iam::userid:root"]},' \
                 '"Action":["s3:GetBucketPolicy"],"Resource":["arn:aws:s3:::%s"]}]}' % self.bucket

        fullpath = self.files.create_file('foo', policy)
        cmdline = '%s %s file://%s' % (self.cmd, join_bucket_key(self.bucket), fullpath)
        self.run_cmd(cmdline, expected_rc=0)

        cmdline = 'obs infob %s --policy' % join_bucket_key(self.bucket)
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]
        self.assertIn('Policy2', stdout)