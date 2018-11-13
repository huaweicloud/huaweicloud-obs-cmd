#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import os

from tests import BaseObsCommandTest


class TestInfobCommand(BaseObsCommandTest):
    cmd = 'obs infob'

    def test_help(self):
        cmdline = '%s help' % self.cmd
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]
        self.assertIn('NAME', stdout)
        self.assertIn('DESCRIPTION', stdout)

    def test_infob_with_no_arguments(self):
        cmdline = '%s %s' % (self.cmd, self.fullbucket)
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]
        self.assertIn('acl', stdout)
        self.assertIn('lifecycle', stdout)
        self.assertIn('location', stdout)
        self.assertIn('metadata', stdout)
        self.assertIn('policy', stdout)
        self.assertIn('storage', stdout)

    def test_infob_with_single_arguments(self):
        cmdline = '%s %s --acl' % (self.cmd, self.fullbucket)
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]
        self.assertIn('acl', stdout)
        self.assertNotIn('lifecycle', stdout)
        self.assertNotIn('location', stdout)
        self.assertNotIn('metadata', stdout)
        self.assertNotIn('policy', stdout)
        self.assertNotIn('storage', stdout)
