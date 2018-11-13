#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from obscmd.testutils import BaseHcmdCommandParamsTest
from obscmd.utils import random_string


class TestMbCommand(BaseHcmdCommandParamsTest):

    cmd = 'obs mb '


    def setUp(self):
        self.fullbucket = 'obs://%s' % random_string(8).lower()
        pass

    def tearDown(self):
        pass

    def delete_bucket(self, fullbucket):
        self.run_cmd('obs rb %s' % fullbucket)

    def test_help(self):
        cmdline = '%s help' % self.cmd
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]
        self.assertIn('NAME', stdout)
        self.assertIn('DESCRIPTION', stdout)

    def test_make_bucket_with_no_arguments(self):

        command = self.cmd + self.fullbucket
        stdout = self.run_cmd(command, expected_rc=0)[0]
        self.assertIn(self.fullbucket, stdout)

        stdout = self.run_cmd('obs ls')[0]
        self.assertIn(self.fullbucket, stdout)

        self.delete_bucket(self.fullbucket)

    def test_make_bucket_already_exist(self):
        command = self.cmd + self.fullbucket
        stdout = self.run_cmd(command, expected_rc=0)[0]
        self.assertIn(self.fullbucket, stdout)

        stdout = self.run_cmd(command)[0]
        self.assertIn('already exist', stdout)

        self.delete_bucket(self.fullbucket)

    def test_make_bucket_with_location(self):
        location = 'cn-north-1'
        cmdline = '%s %s --location %s' % (self.cmd, self.fullbucket, location)
        stdout = self.run_cmd(cmdline)[0]
        self.assertIn(self.fullbucket, stdout)

        cmdline = 'obs infob %s --location' % (self.fullbucket)
        stdout = self.run_cmd(cmdline)[0]
        self.assertIn(location, stdout)

        self.delete_bucket(self.fullbucket)

    def test_make_bucket_with_wrong_location(self):

        location = 'cn-north-11'
        cmdline = '%s %s --location %s' % (self.cmd, self.fullbucket, location)
        stderr = self.run_cmd(cmdline, expected_rc=255)[1]
        self.assertIn('InvalidLocationConstraint', stderr)

    def test_make_bucket_with_storage(self):
        command = '%s %s --storage STANDARD' % (self.cmd, self.fullbucket)
        stdout = self.run_cmd(command)[0]
        self.assertIn(self.fullbucket, stdout)
        self.delete_bucket(self.fullbucket)

    def test_make_bucket_with_wrong_storage(self):
        command = self.cmd + 'obs://tests --storage sTANDARD'
        stderr = self.run_cmd(command, expected_rc=2)[1]
        self.assertIn('Invalid choice', stderr)
