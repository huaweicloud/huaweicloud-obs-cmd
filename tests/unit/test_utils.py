#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import os

from obscmd.exceptions import ParamValidationError
from obscmd.testutils import unittest, skip_if_windows, mock, FileCreator
from obscmd.utils import (DotDict, get_full_path, bytes_to_unitstr, unitstr_to_bytes,
                          hour_time_to_int, get_flowwidth_from_flowpolicy, get_dir_file_num, check_value_threshold,
                          file2md5, calculate_etag, move_file)

KB = 1024 ** 1
MB = 1024 ** 2
GB = 1024 ** 3


class TestDoctDict(unittest.TestCase):

    def test_dot(self):
        dct = {'foo': 'bar'}
        ddct = DotDict(dct)
        self.assertEqual(ddct.foo, dct['foo'])

    def test_nest_dot(self):
        dct = {'foo': 'bar'}
        ddct = DotDict(dct)
        ndct = DotDict({'baz': ddct})
        self.assertEqual(ndct.baz.foo, dct['foo'])

    def test_param(self):
        dct1 = DotDict(a=1, b=3)
        self.assertEqual(dct1.a, 1)


class TestFullPath(unittest.TestCase):

    def test_user_path(self):
        self.assertEqual(get_full_path('~'), os.path.expanduser('~'))


class TestGetDirFileNum(unittest.TestCase):
    def test_dir_file_num(self):
        fc = FileCreator()
        fullpath = fc.create_dir('tmpdir', 10)
        self.assertEqual(get_dir_file_num(fullpath), 10)

        fc.create_full_dir(fullpath, 'nextdir', 5)
        self.assertEqual(get_dir_file_num(fullpath), 15)
        fc.remove_all()


class TestBytesReadable(unittest.TestCase):
    def test_1byte(self):
        self.assertEqual(bytes_to_unitstr(1), '1B')

    def test_bytes_less_kb(self):
        self.assertEqual(bytes_to_unitstr(1023), '1023B')

    def test_bytes_kb(self):
        self.assertEqual(bytes_to_unitstr(1024 * 1.0), '1.0K')

    def test_bytes_more_kb(self):
        self.assertEqual(bytes_to_unitstr(1024 * 1.1), '1.1K')

    def test_bytes_mb(self):
        self.assertEqual(bytes_to_unitstr(1024 * 1024 * 1.0), '1.0M')

    def test_bytes_more_mb(self):
        self.assertEqual(bytes_to_unitstr(1024 * 1024 * 23.7), '23.7M')

    def test_bytes_gb(self):
        self.assertEqual(bytes_to_unitstr(1024 * 1024 * 1024 * 1.0), '1.0G')

    def test_bytes_more_gb(self):
        self.assertEqual(bytes_to_unitstr(1024 * 1024 * 1024 * 3.6), '3.6G')


class TestUnitstrBytes(unittest.TestCase):
    def test_1b(self):
        self.assertEqual(unitstr_to_bytes('1B'), 1)

    def test_bytes_less_kb(self):
        self.assertEqual(unitstr_to_bytes('1023B'), 1023)

    def test_bytes_kb(self):
        self.assertEqual(unitstr_to_bytes('1K'), int(1024 * 1.0))

    def test_bytes_kb_lowercase(self):
        self.assertEqual(unitstr_to_bytes('1.1k'), int(1.1 * KB))

    def test_bytes_mb_lowercase(self):
        self.assertEqual(unitstr_to_bytes('23.7m'), int(23.7 * MB))

    def test_bytes_mb_lowercase(self):
        self.assertEqual(unitstr_to_bytes('100M'), int(100 * MB))

    def test_bytes_more_gb(self):
        self.assertEqual(unitstr_to_bytes('3.6G'), int(3.6 * GB))

    def test_exception(self):
        self.assertRaises(ParamValidationError, unitstr_to_bytes, '23a')

    def test_no_unit(self):
        self.assertEqual(unitstr_to_bytes('123133.3'), 123133)

class TestHourTimeToInt(unittest.TestCase):
    def test_0(self):
        self.assertEqual(hour_time_to_int('00:00'), 0)
        self.assertEqual(hour_time_to_int('00:0'), 0)
        self.assertEqual(hour_time_to_int('0:00'), 0)

    def test_hour_without_minute(self):
        self.assertEqual(hour_time_to_int('09:00'), 900)
        self.assertEqual(hour_time_to_int('23:00'), 2300)

    def test_hour_minute(self):
        self.assertEqual(hour_time_to_int('01:01'), 101)
        self.assertEqual(hour_time_to_int('11:01'), 1101)


class TestGetFlowwidthFromFlowpolicy(unittest.TestCase):

    def setUp(self):
        self.jsonstr = '{"7:30-12:00": "1.1M", "12:00-15:30": "2.5M", "16:00-20:30": "110K", "21:00-06:30": "110M"}'

    def tearDown(self):
        pass

    def test_nomal_time(self):
        mstrftime = mock.Mock(return_value='08:12')
        with mock.patch('time.strftime', mstrftime):
            self.assertEqual(get_flowwidth_from_flowpolicy(self.jsonstr), int(1.1*MB))

    def test_endtime_greater_than_starttime(self):
        mstrftime = mock.Mock(return_value='04:12')
        with mock.patch('time.strftime', mstrftime):
            self.assertEqual(get_flowwidth_from_flowpolicy(self.jsonstr), int(110*MB))

    def test_time_range_border(self):
        mstrftime = mock.Mock(return_value='20:30')
        with mock.patch('time.strftime', mstrftime):
            self.assertEqual(get_flowwidth_from_flowpolicy(self.jsonstr), None)


class TestCheckValueThreshold(unittest.TestCase):
    def test_check_value_threshold(self):
        self.assertEqual(check_value_threshold(89, 100, 200), 100)
        self.assertEqual(check_value_threshold(102, 100, 200), 102)
        self.assertEqual(check_value_threshold(300, 100, 200), 200)


# class TestFile2Md5(unittest.TestCase):
#     def test_file2md5(self):
#         oldpath = '/Users/lovelife/data/obs/file33M'
#         newpath = '/Users/lovelife/git/ureactor/cmd/bin/file33M'
#         self.assertEqual(os.path.getsize(oldpath), os.path.getsize(newpath))
#         oldmd5 = file2md5(oldpath)
#         newmd5 = file2md5(newpath)
#         self.assertEqual(oldmd5, newmd5)
#
#     def test_file33M(self):
#         oldpath = '/Users/lovelife/data/obs/file33M'
#         # oldpath = '/Users/lovelife/Downloads/sqlitestudio-3.1.1.dmg'
#         oldpath = '/Users/lovelife/git/ureactor/cmd/bin/obscmd.cmd'
#         # oldmd5 = file2md5(oldpath)
#         oldmd5 = calculate_etag(oldpath)
#         print(oldmd5)

class TestMoveFile(unittest.TestCase):
    def test_move_file(self):
        fc = FileCreator()
        fullpath = fc.create_dir('tmpdir')
        srcfile = fc.create_file('file1.test', 'ddd')

        destfile = os.path.join(fullpath, 'file1.test')
        move_file(srcfile, destfile)
        self.assertEqual(os.path.exists(srcfile), False)
        self.assertEqual(os.path.exists(destfile), True)
        fc.remove_all()
