#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import unittest

from obscmd.cmds.obs.obsutil import check_resp, split_bucket_key, get_bucket, join_bucket_key, join_obs_path, \
    get_object_name
from obscmd.exceptions import InternalError
from obscmd.utils import DotDict


class TestCheckResp(unittest.TestCase):
    def test_check_resp_with_exception(self):
        resp = DotDict({'status': 301, 'code': 'ExceptionError', 'msg': "some error"})
        self.assertRaises(InternalError, check_resp, resp)

    def test_check_resp_with_no_exception(self):
        resp = DotDict({'status': 200, 'code': 'ok', 'msg': "ok"})
        self.assertEqual(check_resp(resp), None)


class TestSplitBucketKey(unittest.TestCase):
    def test_split_bucket_key(self):
        path = 'obs://bucket/keys'
        self.assertEqual(split_bucket_key(path), ('bucket', 'keys'))

        path = 'obs://bucket'
        self.assertEqual(split_bucket_key(path), ('bucket', ''))


class TestJoinBucketKey(unittest.TestCase):
    def test_bucket_with_key(self):
        self.assertEqual(join_bucket_key('bucket', '/foo'), 'obs://bucket/foo')
        self.assertEqual(join_bucket_key('bucket/', '/doc/foo'), 'obs://bucket/doc/foo')
        self.assertEqual(join_bucket_key('bucket', '\\doc\\foo/'), 'obs://bucket/doc/foo/')

    def test_bucket_without_key(self):
        self.assertEqual(join_bucket_key('bucket'), 'obs://bucket')


class TestJoinObsPath(unittest.TestCase):
    def test_join_obs_path(self):
        self.assertEqual(join_obs_path('obs://bucket', 'doc/foo/'), 'obs://bucket/doc/foo/')
        self.assertEqual(join_obs_path('obs://bucket/', 'doc/foo/'), 'obs://bucket/doc/foo/')
        self.assertEqual(join_obs_path('obs://bucket/', '/doc/foo'), 'obs://bucket/doc/foo')


class TestGetBucket(unittest.TestCase):
    def test_get_bucket(self):
        path = 'obs://bucket/keys'
        self.assertEqual(get_bucket(path), 'bucket')


class TestGetObjectName(unittest.TestCase):
    def test_get_object_name(self):
        self.assertEqual(get_object_name('obs://bucket'), '')
        self.assertEqual(get_object_name('obs://bucket/key'), 'key')
        self.assertEqual(get_object_name('obs://bucket/key/key2'), 'key2')
