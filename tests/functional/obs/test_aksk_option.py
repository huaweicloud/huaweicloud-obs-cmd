#!/usr/bin/env python
# -*- coding: UTF-8 -*-
from tests import BaseObsCommandTest


class TestLsCommand(BaseObsCommandTest):
    cmd = 'obs ls '

    def test_list_bucket(self):
        cmdline = '%s' % self.cmd
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]
        self.assertIn(self.fullbucket, stdout)

    def test_list_bucket_with_wrong_aksk(self):
        cmdline = '%s --ak sdfsd' % self.cmd
        stderr = self.run_cmd(cmdline, expected_rc=255)[1]
        # self.assertIn('InvalidAccessKeyId', stderr)
