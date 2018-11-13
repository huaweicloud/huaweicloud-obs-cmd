#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import os
import unittest

import mock

from obscmd.testutils import create_bucket, FileCreator
from obscmd.testutils import BaseCLIDriverTest
from obscmd.utils import get_dir_file_num


class TestCreateBucket(BaseCLIDriverTest):
    def test_bucket_already_owned_by_you(self):

        self.assertEqual('bucket', 'bucket')


def create_full_dir():
    pass


class TestFileCreator(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.fc = FileCreator()

    @classmethod
    def tearDownClass(cls):
        cls.fc.remove_all()

    def test_create_file(self):
        fullpath = self.fc.create_file('file')
        self.assertEqual(os.path.exists(fullpath), True)
        self.assertEqual(os.path.getsize(fullpath), 8)

    def test_create_size_file(self):
        fullpath = self.fc.create_size_file('file.txt', '8K')
        self.assertEqual(os.path.getsize(fullpath), 8*1024)

    def test_create_dir(self):
        fulldir = self.fc.create_dir('dir', 10, names=['file.txt', 'file2.txt'], sizelist=['1.1K', '251B'])
        self.assertEqual(get_dir_file_num(fulldir), 10)
        self.assertEqual(os.path.exists(os.path.join(fulldir, 'file.txt')), True)
        self.assertEqual(os.path.getsize(os.path.join(fulldir, 'file.txt')), int(1.1*1024))

    def test_create_full_dir(self):
        fulldir = self.fc.create_full_dir(self.fc.rootdir, 'newdir')
        self.assertEqual(fulldir, os.path.join(self.fc.rootdir, 'newdir'))




