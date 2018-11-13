#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import mock

from obscmd.cmds.obs.obsutil import join_bucket_key
from obscmd.testutils import FileCreator
from obscmd.utils import get_dir_file_num
from tests import BaseObsCommandTest


class TestCPCommand(BaseObsCommandTest):
    cmd = 'obs cp '

    def test_upload_with_4_processes(self):
        """
        compare spending time for flow width 10K/s and 10M/s
        10K/s will sleep few seconds 
        :return: 
        """
        dirname = 'tmpdir'
        names = ['test.txt', 'test2.txt', 'app.jpg', 'app2.jpg', 'app3.jpg', 'ap.jpg', 'ap2.jpg', 'ap3.jpg']
        sizes = ['10K', '2M', '222B', '234.1K', '2M', '2M', '2M']
        fulldir = self.files.create_dir(dirname, len(names), names, sizes)
        obspath = join_bucket_key(self.bucket, dirname)

        # test upload
        cmdline = '%s %s %s --recursive --tasknum 4' % (self.cmd, fulldir, obspath)
        with mock.patch('obscmd.utils.get_flowwidth_from_flowpolicy') as flowwidth:
            flowwidth.side_effect = [100 * 1024 ** 1, 100 * 1024 ** 2]
            delta_time1 = self.run_cmd_for_count_time(cmdline, expected_rc=0)
            self.clear_bucket_files()
            delta_time2 = self.run_cmd_for_count_time(cmdline, expected_rc=0)

        self.assertGreater(delta_time1, delta_time2)


    def test_download_with_4_processes(self):
        dirname = 'tmpdir'
        names = ['test.txt', 'test2.txt', 'app.jpg', 'app2.jpg', 'app3.jpg', 'ap.jpg', 'ap2.jpg', 'ap3.jpg']
        sizes = ['10K', '2M', '222B', '234.1K', '2M', '2M', '2M']
        fulldir = self.files.create_dir(dirname, len(names), names, sizes)
        obspath = join_bucket_key(self.bucket, dirname)
        cmdline = '%s %s %s --recursive --tasknum 4' % (self.cmd, fulldir, obspath)
        self.run_cmd(cmdline, expected_rc=0)
        # test download
        dirname = 'tmpdir2'
        fulldir = self.files.create_dir(dirname)
        cmdline = '%s %s %s --recursive --tasknum 4' % (self.cmd, obspath, fulldir)
        with mock.patch('obscmd.utils.get_flowwidth_from_flowpolicy') as flowwidth:
            flowwidth.side_effect = [100 * 1024 ** 1, 100 * 1024 ** 2]
            delta_time1 = self.run_cmd_for_count_time(cmdline, expected_rc=0)
            self.clear_bucket_files()
            delta_time2 = self.run_cmd_for_count_time(cmdline, expected_rc=0)

        self.assertGreater(delta_time1, delta_time2)



