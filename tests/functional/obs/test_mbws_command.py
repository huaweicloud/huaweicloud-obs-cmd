#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from obscmd.cmds.obs.obsutil import join_bucket_key
from tests import BaseObsCommandTest


class TestMbwsCommand(BaseObsCommandTest):

    cmd = 'obs mbws '

    def test_help(self):
        cmdline = '%s help' % self.cmd
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]
        self.assertIn('mbws', stdout)
        self.assertIn('DESCRIPTION', stdout)

    def test_set_website(self):

        cmdline = '%s %s %s' % (self.cmd, join_bucket_key(self.bucket), '{"indexDocument":{"suffix":"index.html"},"errorDocument":{"key":"error.html"},"routingRules":[{"condition":{"httpErrorCodeReturnedEquals":404},"redirect":{"protocol":"http","replaceKeyWith":"NotFound.html"}}]}')
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]

        self.assertIn('complete', stdout)
