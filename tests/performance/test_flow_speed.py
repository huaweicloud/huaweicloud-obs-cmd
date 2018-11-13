#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import mock

from obscmd.cmds.obs.obsutil import join_bucket_key
from obscmd.testutils import FileCreator
from obscmd.utils import get_dir_file_num, unitstr_to_bytes, bytes_to_unitstr
from tests import BaseObsCommandTest


class TestCPCommand(BaseObsCommandTest):
    cmd = 'obs cp '

    def print_run_time(self, size, seconds, tasknum, cmd):
        total = unitstr_to_bytes(size)
        flowspeed = bytes_to_unitstr(total/seconds)
        msg = '%s/s\t\d tasks\t%s' % (flowspeed, tasknum, cmd)
        print(msg)

    def test_upload_download_single_bigfile_on_ecs(self):
        """
        compare spending time for no flow width limit, the flow width must
        greater than 100M/s
        :return: 
        """
        obspath = join_bucket_key(self.bucket)


        with mock.patch('obscmd.utils.get_flowwidth_from_flowpolicy') as flowwidth:
            flowwidth.return_value = 100 * 1024 ** 3

            # upload
            file500M = self.files.create_size_file('file500M', '500M')
            cmdline = '%s %s %s --tasknum 4' % (self.cmd, file500M, obspath)
            delta_time1 = self.run_cmd_for_count_time(cmdline, expected_rc=0)
            self.print_run_time('500M', delta_time1, 4, cmdline)
            self.assertLess(delta_time1, 500 / 100.0)

            cmdline = '%s %s %s --tasknum 8' % (self.cmd, file500M, obspath)
            delta_time1 = self.run_cmd_for_count_time(cmdline, expected_rc=0)
            self.print_run_time('500M', delta_time1, 8, cmdline)
            self.assertLess(delta_time1, 500/100.0)


            file10G = self.files.create_size_file('file10G', '10G')


            cmdline = '%s %s %s --tasknum 4' % (self.cmd, file10G, obspath)
            delta_time1 = self.run_cmd_for_count_time(cmdline, expected_rc=0)
            self.print_run_time('10G', delta_time1, 4, cmdline)
            self.assertLess(delta_time1, 10*1024 / 100.0)

            cmdline = '%s %s %s --tasknum 8' % (self.cmd, file10G, obspath)
            delta_time1 = self.run_cmd_for_count_time(cmdline, expected_rc=0)
            self.print_run_time('10G', delta_time1, 8, cmdline)
            self.assertLess(delta_time1, 10*1024 / 100.0)

            # download
            fulltmpdir = self.files.create_dir('bigfiles')
            cmdline = '%s %s/file10G %s --tasknum 4' % (self.cmd, obspath, fulltmpdir)
            delta_time1 = self.run_cmd_for_count_time(cmdline, expected_rc=0)
            self.print_run_time('10G', delta_time1, 4, cmdline)
            self.assertLess(delta_time1, 10 * 1024 / 100.0)

            cmdline = '%s %s/file10G %s --tasknum 8' % (self.cmd, obspath, fulltmpdir)
            delta_time1 = self.run_cmd_for_count_time(cmdline, expected_rc=0)
            self.print_run_time('10G', delta_time1, 8, cmdline)
            self.assertLess(delta_time1, 10 * 1024 / 100.0)


    def test_upload_multip_smallfiles_on_ecs(self):
        """
        compare spending time for no flow width limit, the flow width must
        greater than 100M/s
        :return: 
        """
        dirname = 'tmpdir'
        filenum = 100
        sizes = ['100K' for _ in range(filenum)]
        fulldir = self.files.create_dir(dirname, filenum, sizelist=sizes)
        obspath = join_bucket_key(self.bucket, dirname)

        with mock.patch('obscmd.utils.get_flowwidth_from_flowpolicy') as flowwidth:
            flowwidth.return_value = 100 * 1024 ** 3

            # upload
            cmdline = '%s %s %s --recursive --tasknum 8' % (self.cmd, fulldir, obspath)
            delta_time1 = self.run_cmd_for_count_time(cmdline, expected_rc=0)
            self.assertLess(delta_time1, 100 * 100.0 / (35 * 1024))

            # downlaod
            cmdline = '%s %s %s --recursive --tasknum 8' % (self.cmd, obspath, fulldir)
            delta_time1 = self.run_cmd_for_count_time(cmdline, expected_rc=0)
            self.assertLess(delta_time1, 100 * 100.0 / (35 * 1024))




