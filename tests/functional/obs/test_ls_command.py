#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import os

from tests import BaseObsCommandTest


class TestLsCommand(BaseObsCommandTest):
    cmd = 'obs ls '

    def test_list_bucket(self):
        cmdline = '%s' % self.cmd
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]
        self.assertIn(self.fullbucket, stdout)

    def test_errors_out_with_extra_arguments(self):
        cmdline = '%s --extra-argument-foo' % self.cmd
        stderr = self.run_cmd(cmdline, expected_rc=255)[1]
        self.assertIn('Unknown options', stderr)
        self.assertIn('--extra-argument-foo', stderr)

    def test_list_dir_and_files(self):

        # upload dir
        fulldir = self.files.create_dir('doc', 10)
        cmdline = 'obs cp %s %s/doc --recursive' % (fulldir, self.fullbucket)
        self.run_cmd(cmdline, expected_rc=0)

        # upload a file
        fullfile = self.files.create_file('foo.txt')
        cmdline = 'obs cp %s %s' % (fullfile, self.fullbucket)
        self.run_cmd(cmdline, expected_rc=0)

        cmdline = '%s %s' % (self.cmd, self.fullbucket)
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]

        self.assertIn(self.fullbucket+'/doc/\n', stdout)
        self.assertIn(self.fullbucket + '/foo.txt\n', stdout)

    def test_list_more_than_1000_objects(self):

        # upload dir
        fulldir = self.files.create_dir('doc', 1002)
        cmdline = 'obs cp %s %s/doc --recursive' % (fulldir, self.fullbucket)
        self.run_cmd(cmdline, expected_rc=0)
        print(self.fullbucket)

        cmdline = '%s %s --recursive' % (self.cmd, self.fullbucket)
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]
        self.assertIn('1002 objects', stdout)

    def test_fail_wrong_bucket_name(self):
        self.run_cmd('obs ls obs://obs-sh-test111', expected_rc=255)

    def test_list_with_outfile(self):

        # upload dir
        fulldir = self.files.create_dir('doc', 10)
        outfile = os.path.join(fulldir, 'result.txt')

        cmdline = 'obs cp %s %s/doc --recursive' % (fulldir, self.fullbucket)
        self.run_cmd(cmdline, expected_rc=0)

        cmdline = '%s %s --recursive --outfile %s' % (self.cmd, self.fullbucket, outfile)
        self.run_cmd(cmdline, expected_rc=0)
        self.assertGreater(os.path.getsize(outfile), 0)