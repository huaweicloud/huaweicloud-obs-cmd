#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import os

import mock
import time

from obscmd.cmds.obs.obsutil import join_bucket_key
from obscmd.testutils import FileCreator
from obscmd.utils import get_dir_file_num, file2md5
from tests import BaseObsCommandTest


class TestCPCommand(BaseObsCommandTest):
    cmd = 'obs cp'

    def test_help(self):
        cmdline = '%s help' % self.cmd
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]
        self.assertIn('NAME', stdout)
        self.assertIn('DESCRIPTION', stdout)

    def test_upload_single_file(self):
        filename = 'foo.txt'
        full_path = self.files.create_file(filename, 'mycontent')
        obspath = join_bucket_key(self.bucket, filename)
        cmdline = '%s %s %s' % (self.cmd, full_path, obspath)
        self.run_cmd(cmdline, expected_rc=0)

    def test_upload_dir_with_recursive(self):
        dirname = 'tmpdir'
        fulldir = self.files.create_dir(dirname, 10)
        obspath = join_bucket_key(self.bucket, dirname)
        cmdline = '%s %s %s --recursive' % (self.cmd, fulldir, obspath)
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]

        self.assertIn('upload complete 10 files', stdout)

    def test_upload_dir_with_no_recursive(self):
        dirname = 'tmpdir'
        fulldir = self.files.create_dir(dirname, 10)
        obspath = join_bucket_key(self.bucket, dirname)
        cmdline = '%s %s %s' % (self.cmd, fulldir, obspath)
        stderr = self.run_cmd(cmdline, expected_rc=255)[1]
        self.assertIn('upload directory need recursive option', stderr)

    def test_upload_dir_with_update(self):
        dirname = 'tmpdir'
        fulldir = self.files.create_dir(dirname, 10)
        obspath = join_bucket_key(self.bucket, dirname)
        cmdline = '%s %s %s --recursive --update' % (self.cmd, fulldir, obspath)
        self.run_cmd(cmdline, expected_rc=0)
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]
        self.assertIn('No files to', stdout)

    def test_upload_dir_with_exclude(self):
        dirname = 'tmpdir'
        names = ['test.txt', 'test2.txt', 'app.jpg', 'app2.jpg', 'app3.jpg']
        fulldir = self.files.create_dir(dirname, len(names), names)
        obspath = join_bucket_key(self.bucket, dirname)
        cmdline = '%s %s %s --recursive --exclude *.jpg' % (self.cmd, fulldir, obspath)
        self.run_cmd(cmdline, expected_rc=0)
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]
        self.assertIn('upload complete 2 files', stdout)

    def test_upload_dir_with_exclude_include(self):
        dirname = 'tmpdir'
        names = ['test.txt', 'test2.txt', 'app.jpg', 'app2.jpg', 'app3.jpg']
        fulldir = self.files.create_dir(dirname, len(names), names)
        obspath = join_bucket_key(self.bucket, dirname)
        cmdline = '%s %s %s --recursive --exclude *.jpg --include *3.*' % (self.cmd, fulldir, obspath)
        self.run_cmd(cmdline, expected_rc=0)
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]
        self.assertIn('upload complete 3 files', stdout)

    def test_upload_dir_with_multiprocessing(self):
        dirname = 'tmpdir'
        names = ['test.txt', 'test2.txt', 'app.jpg', 'app2.jpg', 'app3.jpg']
        sizes = ['10K', '2M', '222B', '234.1K']
        fulldir = self.files.create_dir(dirname, len(names), names, sizes)
        obspath = join_bucket_key(self.bucket, dirname)

        cmdline = '%s %s %s --recursive --tasknum 1' % (self.cmd, fulldir, obspath)
        delta_time1 = self.run_cmd_for_count_time(cmdline, expected_rc=0)

        self.clear_bucket_files()

        cmdline = '%s %s %s --recursive --tasknum 8' % (self.cmd, fulldir, obspath)
        delta_time2 = self.run_cmd_for_count_time(cmdline, expected_rc=0)

        self.assertGreater(delta_time1, delta_time2)

    def test_upload_download_copy_single_file(self):
        filename = 'foo.txt'
        full_path = self.files.create_file(filename, 'mycontent')

        # upload
        obspath = join_bucket_key(self.bucket)
        cmdline = '%s %s %s' % (self.cmd, full_path, obspath)
        self.run_cmd(cmdline, expected_rc=0)

        # download
        obspath = join_bucket_key(self.bucket, filename)
        cmdline = '%s %s %s' % (self.cmd, obspath, self.files.rootdir)
        self.run_cmd(cmdline, expected_rc=0)

        # copy
        srcobspath = join_bucket_key(self.bucket, filename)
        destobspath = join_bucket_key(self.bucket, 'temp')
        cmdline = '%s %s %s' % (self.cmd, srcobspath, destobspath)
        self.run_cmd(cmdline, expected_rc=0)

    def test_upload_downlad_copy_dir_with_recursive(self):
        dirname = 'tmpdir'

        # upload
        fulldir = self.files.create_dir(dirname, 10)
        obspath = join_bucket_key(self.bucket, dirname)
        cmdline = '%s %s %s --recursive' % (self.cmd, fulldir, obspath)
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]
        self.assertIn('upload complete 10 files', stdout)

        # download
        # create an another empty local dir
        localdir = self.files.create_dir('otherdir')
        cmdline = '%s %s %s --recursive' % (self.cmd, obspath, localdir)
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]
        self.assertIn('download complete 10 files', stdout)
        self.assertEqual(get_dir_file_num(localdir), 10)

        # copy
        destobspath = join_bucket_key(self.bucket, 'temp')
        cmdline = '%s %s %s --recursive' % (self.cmd, obspath, destobspath)
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]
        self.assertIn('copy complete 10 files', stdout)

    def test_downlad_copy_multiprocess(self):
        dirname = 'tmpdir'

        # upload
        fulldir = self.files.create_dir(dirname, 10)
        obspath = join_bucket_key(self.bucket, dirname)
        cmdline = '%s %s %s --recursive' % (self.cmd, fulldir, obspath)
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]
        self.assertIn('upload complete 10 files', stdout)

        # download
        # create an another empty local dir
        localdir = self.files.create_dir('otherdir')
        cmdline = '%s %s %s --recursive --tasknum 1' % (self.cmd, obspath, localdir)
        delta_time1 = self.run_cmd_for_count_time(cmdline, expected_rc=0)

        # localdir = self.files.create_dir('otherdir')
        cmdline = '%s %s %s --recursive --tasknum 4' % (self.cmd, obspath, localdir)
        delta_time2 = self.run_cmd_for_count_time(cmdline, expected_rc=0)
        self.assertGreater(delta_time1, delta_time2)

        # copy
        destobspath = join_bucket_key(self.bucket, 'temp1')
        cmdline = '%s %s %s --recursive --tasknum 1' % (self.cmd, obspath, destobspath)
        delta_time1 = self.run_cmd_for_count_time(cmdline, expected_rc=0)

        destobspath = join_bucket_key(self.bucket, 'temp2')
        cmdline = '%s %s %s --recursive --tasknum 4' % (self.cmd, obspath, destobspath)
        delta_time2 = self.run_cmd_for_count_time(cmdline, expected_rc=0)

        self.assertGreater(delta_time1, delta_time2)

    def test_upload_download_big_single_file_md5(self):
        filename = 'foo.txt'
        full_path = self.files.create_size_file(filename, '23M')
        oldmd5 = file2md5(full_path)
        time.sleep(2)
        cmdline = '%s %s %s' % (self.cmd, full_path, self.fullbucket)
        print(cmdline)
        self.run_cmd(cmdline, expected_rc=0)

        # # #
        obspath = join_bucket_key(self.bucket, filename)
        tmp_dir = self.files.create_dir('bigfiles')
        cmdline = '%s %s %s' % (self.cmd, obspath, tmp_dir)
        self.run_cmd(cmdline, expected_rc=0)
        new_full_path = os.path.join(tmp_dir, filename)
        newmd5 = file2md5(new_full_path)

        self.assertEqual(oldmd5, newmd5)
