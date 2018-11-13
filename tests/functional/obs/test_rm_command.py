#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from obscmd.cmds.obs.obsutil import join_bucket_key
from obscmd.testutils import FileCreator
from tests import BaseObsCommandTest


class TestRmCommand(BaseObsCommandTest):

    cmd = 'obs rm'


    def test_help(self):
        cmdline = '%s help' % self.cmd
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]
        self.assertIn('rm', stdout)
        self.assertIn('DESCRIPTION', stdout)

    def test_rm_single_file(self):
        # upload a file
        fullfile = self.files.create_file('foo.txt')
        cmdline = 'obs cp %s %s' % (fullfile, self.fullbucket)
        self.run_cmd(cmdline, expected_rc=0)

        cmdline = '%s %s/foo.txt' % (self.cmd, self.fullbucket)
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]
        self.assertIn('foo.txt', stdout)

        cmdline = 'obs ls %s' % self.fullbucket
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]
        self.assertIn('total: 0', stdout)

    def test_rm_dir_files(self):
        fulldir = self.files.create_dir('doc', 10)
        cmdline = 'obs cp %s %s/doc --recursive' % (fulldir, self.fullbucket)
        self.run_cmd(cmdline, expected_rc=0)

        cmdline = 'obs ls %s/doc/' % self.fullbucket
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]
        self.assertIn('10 objects', stdout)

        cmdline = '%s %s/doc' % (self.cmd, self.fullbucket)
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]

        cmdline = 'obs ls %s/doc/' % self.fullbucket
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]
        self.assertIn('0 objects', stdout)
