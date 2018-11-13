#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import time

from obscmd.cmds.obs.obsutil import create_client_with_default_config, ObsCmdUtil, join_bucket_key
from obscmd.testutils import BaseHcmdCommandParamsTest, FileCreator, create_bucket
from obscmd.utils import random_string

TEST_BUCKET = 'obs://dwweee'

class BaseObsCommandTest(BaseHcmdCommandParamsTest):

    @classmethod
    def setUpClass(cls):
        cls.client = create_client_with_default_config()
        cls.bucket = TEST_BUCKET
        if not ObsCmdUtil(cls.client).head_bucket(TEST_BUCKET):
            # cls.bucket = ObsCmdUtil(cls.client).create_bucket(TEST_BUCKET, location='cn-east-2')
            cls.bucket = ObsCmdUtil(cls.client).create_bucket(TEST_BUCKET)
            time.sleep(20)
        cls.fullbucket = join_bucket_key(cls.bucket)


    @classmethod
    def tearDownClass(cls):
        cls.client.close()

    def setUp(self):
        super(BaseObsCommandTest, self).setUp()
        self.files = FileCreator()

    def tearDown(self):
        super(BaseObsCommandTest, self).tearDown()
        if self.bucket:
            ObsCmdUtil(self.client).clear_bucket(self.bucket)
        self.files.remove_all()

    def clear_bucket_files(self):
        ObsCmdUtil(self.client).clear_bucket(self.bucket)

    def run_cmd_for_count_time(self, cmd, expected_rc=0):
        start_time = time.time()
        self.run_cmd(cmd, expected_rc)
        end_time = time.time()
        return end_time - start_time

