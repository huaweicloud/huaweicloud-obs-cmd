#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from obscmd.cmds.obs.obsutil import join_bucket_key
from obscmd.testutils import FileCreator
from tests import BaseObsCommandTest


class TestRbCommand(BaseObsCommandTest):

    cmd = 'obs rb '

    def test_help(self):
        cmdline = '%s help' % self.cmd
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]
        self.assertIn('rb', stdout)
        self.assertIn('DESCRIPTION', stdout)

    def test_delete_bucket(self):
        tmp_bucket = 'tmpbucket'

        cmdline = 'obs mb %s' % join_bucket_key(tmp_bucket)
        self.run_cmd(cmdline, expected_rc=0)

        cmdline = 'obs ls'
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]
        self.assertIn(tmp_bucket, stdout)

        cmdline = '%s %s' % (self.cmd, join_bucket_key(tmp_bucket))
        self.run_cmd(cmdline, expected_rc=0)

        cmdline = 'obs ls'
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]
        self.assertNotIn(tmp_bucket, stdout)
