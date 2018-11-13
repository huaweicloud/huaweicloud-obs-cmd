#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import unittest

from obscmd.configloader import raw_config_parse
from obscmd.testutils import FileCreator


class TestConfigLoader(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fc = FileCreator()

    @classmethod
    def tearDownClass(cls):
        cls.fc.remove_all()

    def test_config_loader(self):

        content = """
        [log]
        maxsize = 1G
        maxbytes = 10M
        backupcount = 1024
        
        [task]
        partsize = 1M
        tasknum = 4
        checkpoint_dir = ~/.obscmd/checkpoint
        flowwidth = 1M
        flowtime = 5
        flowpolicy = {"8:00-12:00": "1.1M", "12:00-16:00": "2.5M", "16:00-21:00": "110K", "21:00-08:00": "110M"}
        """
        fullpath = self.fc.create_file('conf', content)

        config = raw_config_parse(fullpath)

        self.assertEqual(config.log.maxsize, '1G')
        self.assertEqual(config.log.backupcount, '1024')
        self.assertEqual(config.task.partsize, '1M')
