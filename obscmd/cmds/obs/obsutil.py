#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import json
import os
import threading
import time
import math

from obscmd import globl
from obscmd.constant import STORAGE_CLASS, STORAGE_CLASS_TR, HEADER_PARAMS
from obscmd.utils import calculate_etag
from obs import ObsClient, DeleteObjectsRequest, Object, ListMultipartUploadsRequest, CreateBucketHeader, \
    GetObjectRequest, GetObjectHeader

from obscmd.compat import safe_decode, is_windows
from obscmd.config import config, MAX_PART_NUM
from obscmd.exceptions import InternalError
import ast


"""
common obs util functions
"""

LIST_OBJECTS_MAX = 1000


from obscmd.utils import count_time

class ObjectMetadata(object):
    """
    metadata of the object 
    """
    def __init__(self, bucket, key):
        self.bucket = bucket
        self.key = key

class ObsCmdUtil(object):
    """
    some common function used in obs command
    """
    def __init__(self, client):
        self.client = client

    def head_bucket(self, bucket):
        resp = self.head_bucket_nocheck(bucket)
        try:
            check_resp(resp)
        except InternalError:
            return False
        return True

    @count_time
    def head_bucket_nocheck(self, bucket):
        resp = self.client.headBucket(bucket)
        return resp

    @count_time
    def create_bucket(self, bucket, acl_control=None, storage=None, location=None):
        header = CreateBucketHeader(aclControl=acl_control, storageClass=storage)
        resp = self.client.createBucket(bucket, header=header, location=location)
        check_resp(resp)
        return bucket

    @count_time
    def remove_bucket(self, bucket):
        resp = self.client.deleteBucket(bucket)
        check_resp(resp)


    def clear_bucket(self, bucket):
        keys = self.get_objects_info(bucket, None, 'key')
        self.delete_all_objects(bucket, keys)

        # abort multipart upload tasks
        self.remove_object_multipart(bucket)
        return keys

    ###############################################
    ########### upload ############################
    ###############################################

    @count_time
    def remove_object_multipart(self, bucket, key=None):
        """
        if key=None, remove bucket multipart
        :param bucket: bucket name
        :param key: 
        :return: 
        """
        multipart = ListMultipartUploadsRequest(prefix=key)
        resp = self.client.listMultipartUploads(bucket, multipart)
        check_resp(resp)
        while resp.body is None:
            time.sleep(1)
            resp = self.client.listMultipartUploads(bucket, multipart)
            check_resp(resp)
        for item in resp.body.upload:
            resp = self.client.abortMultipartUpload(bucket, item.key, item.uploadId)
            check_resp(resp)
        return 0

    @count_time
    def abort_multipart_upload(self, bucket, key, uploadid):
        resp = self.client.abortMultipartUpload(bucket, key, uploadid)

    @count_time
    def complete_multipart_upload(self, bucke, key, uploadid, complete_multipart_uploadrequest):
        resp = self.client.completeMultipartUpload(bucke, key, uploadid, complete_multipart_uploadrequest)
        return resp

    @count_time
    def upload_part(self, bucketName, objectKey, partNumber, uploadId, object=None, isFile=False, partSize=None,
                   offset=0, sseHeader=None, isAttachMd5=False, md5=None, content=None):
        resp = self.client.uploadPart(bucketName, objectKey, partNumber, uploadId, object, isFile, partSize,
                       offset, sseHeader, isAttachMd5, md5, content)
        return resp

    @count_time
    def copy_part(self, bucketName, objectKey, partNumber, uploadId, copySource, copySourceRange):
        resp = self.client.copyPart(bucketName=bucketName, objectKey=objectKey, partNumber=partNumber,
                 uploadId=uploadId, copySource=copySource, copySourceRange=copySourceRange)
        return resp

    @count_time
    def initiate_multipart_upload(self, bucketName, objectKey, acl=None, storageClass=None,
                                  metadata=None, websiteRedirectLocation=None, contentType=None, sseHeader=None, expires=None, extensionGrants=None):

        resp = self.client.initiateMultipartUpload(bucketName, objectKey, acl, storageClass,
                                    metadata, websiteRedirectLocation, contentType, sseHeader,
                                    expires, extensionGrants)
        check_resp(resp)
        return resp

    @count_time
    def force_remove_bucket(self, bucket):
        """
        batch remove all objects in the bucket firstly, and then 
        abort multipart upload tasks，remove the empty bucket last
        :param bucket: 
        :return: 
        """

        keys = self.clear_bucket(bucket)
        self.remove_bucket(bucket)
        return keys

    def get_local_etag(self, filepath, partsize, part_threshold):
        size = os.path.getsize(filepath)
        # for partnum > 10000
        tmpsize = reset_partsize(size, partsize) if size >= part_threshold else part_threshold
        local_etag = calculate_etag(filepath, tmpsize)
        return local_etag

    def check_etag_with_obs_local(self, bucket, key, localpath, partsize=None, part_threshold=None):
        """
        compare local file etag and obs etag
        """
        obj = self.get_object_metadatas(bucket, key)
        obs_etag = getattr(obj, HEADER_PARAMS['etag'], None)
        # obs_etag = self.get_object_etag(bucket, key)
        if partsize is None:
            partsize = getattr(obj, HEADER_PARAMS['partsize'], None)
            # partsize = self.get_object_metadata_from_header(bucket, key, 'partsize')
            if partsize:
                partsize = int(partsize)
                local_etag = calculate_etag(localpath, partsize)
            else:
                local_etag = calculate_etag(localpath)
        else:
            local_etag = self.get_local_etag(localpath, partsize, part_threshold)
        if obs_etag == local_etag:
            return 'ok', obs_etag, local_etag, join_bucket_key(bucket, key), localpath
        else:
            return 'failed', obs_etag, local_etag, join_bucket_key(bucket, key), localpath


    def check_dir_etag_with_local_obs(self, bucket, key, kv, partsize=None, part_threshold=None):
        """
        compare files etag in obs dir and local dir
        :param bucket: 
        :param key: 
        :param kv: (filepath, key)
        :partsize: None for downloading, otherwise for uploading
        :return: 
        """
        key_etag_ret = []

        if partsize:
            key_etags = self.get_objects_info(bucket, key, ('key', 'etag'))
            key_etag_kv = {}
            for key, etag in key_etags:
                key_etag_kv[key] = etag

            for filepath, key in kv:
                obs_etag = key_etag_kv[key]
                local_etag = self.get_local_etag(filepath, partsize, part_threshold)
                if obs_etag == local_etag:
                    key_etag_ret.append(('ok', obs_etag, local_etag, join_bucket_key(bucket, key), filepath))
                else:
                    key_etag_ret.append(('faild', obs_etag, local_etag, join_bucket_key(bucket, key), filepath))
        else:
            for filepath, key in kv:
                key_etag_ret.append(self.check_etag_with_obs_local(bucket, key, filepath))

        return key_etag_ret

    def get_object_size(self, bucket, key):
        size, etag, last_modified = self.get_object_metadata(bucket, key)
        return size

    def get_object_etag(self, bucket, key):
        size, etag, last_modified = self.get_object_metadata(bucket, key)
        return etag

    @count_time
    def get_object_metadata_from_header(self, bucket, key, topic):
        resp = self.client.getObjectMetadata(bucket, key)
        check_resp(resp)
        for item in resp.header:
            if item[0] == topic:
                return item[1]
        return None

    def get_object_metadata(self, bucket, key):
        resp = self.get_object_metadata_nocheck(bucket, key)
        check_resp(resp)
        return resp.body.contentLength, resp.body.etag, resp.body.lastModified

    def get_object_metadatas(self, bucket, key):
        """
        header:[('content-length', '5368709120'), 
            ('id-2', '32AAAQAAEAABAAAQAAEAABAAAQAAEAABCSzfGZSVr/YHfJxhFkOU8m6VF76nRK+S'), 
            ('last-modified', 'Tue, 30 Oct 2018 09:58:20 GMT'), 
            ('connection', 'close'), 
            ('request-id', '00000166C4DB624C860DA3F7B8F366E2'), 
            ('etag', '"ec4bcc8776ea04479b786e063a9ace45"'), 
            ('x-reserved', 'amazon, aws and amazon web services are trademarks or registered trademarks of Amazon Technologies, Inc'), 
            ('date', 'Tue, 30 Oct 2018 12:03:23 GMT'), 
            ('version-id', 'G0011166C468E5E1FFFF860B000007A4'), 
            ('partsize', '10485760'), 
            ('content-type', 'text/plain')]
        """
        resp = self.get_object_metadata_nocheck(bucket, key)
        check_resp(resp)
        obj = ObjectMetadata(bucket, key)
        for item in resp.header:
            for v in dict(HEADER_PARAMS).keys():
                if v == item[0]:
                    prop = HEADER_PARAMS[v]
                    prop_value = item[1]
                    setattr(obj, prop, prop_value)
        return obj

    @count_time
    def get_object_metadata_nocheck(self, bucket, key, versionId=None, sseHeader=None, origin=None,
                              requestHeaders=None):
        resp = self.client.getObjectMetadata(bucket, key, versionId, sseHeader, origin,
                              requestHeaders)
        return resp

    @count_time
    def list_dirs_objects(self, bucket, prefix=None):
        """
        list dirs ends with /
        and objects
        :param bucket: 
        :param prefix: 
        :return: 
        """
        resp = self.client.listObjects(bucket, prefix=prefix, delimiter='/')
        check_resp(resp)
        dirs = [safe_decode(item['prefix']) for item in resp.body.commonPrefixs]
        objects = resp.body.contents
        return dirs, objects

    @count_time
    def list_objects(self, bucket, prefix=None):
        """
        can only list 1000 objects
        :param bucket: 
        :param prefix: 
        :return: 
        """
        resp = self.client.listObjects(bucket, prefix=prefix)
        check_resp(resp)
        return resp.body.contents

    @count_time
    def list_all_objects(self, bucket, prefix=None, marker=None):
        resp = self.client.listObjects(bucket, prefix=prefix, marker=marker)
        check_resp(resp)
        items = resp.body.contents

        while resp.body.is_truncated:
            resp = self.client.listObjects(bucket, prefix=prefix, marker=resp.body.next_marker)
            check_resp(resp)
            items += resp.body.contents
        # if resp.body.is_truncated:
        #     items = items + self.list_all_objects(bucket, prefix=prefix, marker=resp.body.next_marker)
        return items

    def get_objects_info(self, bucket, prefix=None, topics=None, limit=None):
        """
        list obs objects information
        :param bucket: bucket name
        :param topics: info topic, choices are [etag, key, lastModified, owner, size, storageClass]
        
        values are like following:
        etag: "d7deee6cced029b4ca652a59c768538e-2"
        key: 'test.txt'
        lastModified: '2018/08/02 17:45:55'
        owner: {'owner_name': 'jiaowoxiaoming', 'owner_id': '8be8301bdfc7469aaf721d0504826420'}
        size: 15668606
        storageClass: 'STANDARD'
        
        use example:
        
        get_objects_info('mybucket', None, 'key')
        ret value: ['doc/test1.txt', 'doc/test2.txt']
        
        get_objects_info('mybucket', 'doc', ('key', 'size'), 2)
        ret value: [['doc/test1.txt', 123125], ['doc/test2.txt', 4352345], ]
        :return: 
        """

        single = False if isinstance(topics, (list, tuple)) else True
        if limit and limit <= LIST_OBJECTS_MAX:
            items = self.list_objects(bucket, prefix)
        else:
            items = self.list_all_objects(bucket, prefix)

        if limit:
            items = items[:limit]
        infos = []
        if single:
            infos = [safe_decode(item[topics]) for item in items]
        else:
            for item in items:
                info = [safe_decode(item[topic]) for topic in topics]
                infos.append(info)

        return infos

    @count_time
    def put_file(self, bucket, objkey, filepath, metadata=None):
        times = 0
        retry_times = 3
        while times < retry_times:
            try:
                resp = self.client.putFile(bucket, objkey, filepath, metadata)
                check_resp(resp)
            except Exception as e:
                times += 1
                if times == retry_times:
                    raise e
                time.sleep(.001)
            else:
                return resp

    @count_time
    def put_content(self, bucket, objkey, content=None):
        resp = self.client.putContent(bucket, objkey, content)
        check_resp(resp)
        return resp

    @count_time
    def copy_object(self, srcb, srck, destb, destk, metadata=None):

        resp = self.client.copyObject(srcb, srck, destb, destk, metadata)
        check_resp(resp)
        return resp

    def download_file(self, bucket, key, filepath):
        resp = self.get_object(bucket, key, downloadPath=filepath)
        check_resp(resp)

    @count_time
    def get_object(self, bucketName, objectKey, downloadPath=None, getObjectRequest=GetObjectRequest(),
                  headers=GetObjectHeader(), loadStreamInMemory=False):
        resp = self.client.getObject(bucketName, objectKey, downloadPath, getObjectRequest,
                      headers, loadStreamInMemory)
        return resp

    def download_or_create_file(self, bucket, key, total, filepath):
        if total == 0:
            dirpath = os.path.dirname(filepath)
            if not os.path.isdir(dirpath):
                os.makedirs(dirpath, 0o755)
            with open(filepath, 'wb') as _:
                pass
        else:
            self.download_file(bucket, key, filepath)

    def delete_all_objects(self, bucket, keys):
        """
        delete all object for several times batch delete
        :param bucket: 
        :param keys: 
        :return: 
        """
        num = len(keys)
        if num <= LIST_OBJECTS_MAX:
            return self.batch_delete_objects(bucket, keys)

        times = int(num / LIST_OBJECTS_MAX)
        for i in range(times):
            start = i * LIST_OBJECTS_MAX
            resp = self.batch_delete_objects(bucket, keys[start:start+LIST_OBJECTS_MAX])
            check_resp(resp)
        if times * LIST_OBJECTS_MAX < num:
            resp = self.batch_delete_objects(bucket, keys[times * LIST_OBJECTS_MAX:])
            check_resp(resp)

    @count_time
    def batch_delete_objects(self, bucket, keys):
        """
        deleteObjects delete max 1000 objects once
        :param bucket: 
        :param keys: 
        :return: 
        """
        if len(keys) == 0:
            return 0
        delete_objects_request = DeleteObjectsRequest()
        delete_objects_request.objects = [Object(key=key) for key in keys]
        resp = self.client.deleteObjects(bucket, deleteObjectsRequest=delete_objects_request)
        check_resp(resp)
        # for key in keys:
        #     self.remove_object_multipart(bucket, key)
        return resp

    @count_time
    def delete_object(self, bucket, key, version=None):
        """
        delele a single object.
        :param bucket: 
        :param key: 
        :param version: 
        :return: 
        """
        resp = self.client.deleteObject(bucket, key, version)
        check_resp(resp)
        # self.remove_object_multipart(bucket, key)

    @count_time
    def get_bucket_storage(self, bucket):
        resp = self.client.getBucketMetadata(bucket)
        try:
            check_resp(resp)
        except:
            return STORAGE_CLASS[0]
        return STORAGE_CLASS_TR[resp.body.storageClass]


def multitask_with_sleep(process, func, func_arg, items, tasknum, flowwith=None):
    """
    multitasks for uploading and downloading with progressbar
    :param func: function for task dispatching
    :param func_arg: func arguments
    :param items: items for tasks to dispatch
    :param tasknum: max tasks for parallel
    :param flowwith: max flow width
    :param flowtime: the time for statistic 
    :return: 
    """
    from obscmd.compat import queue
    qitems = queue.Queue()
    for item in items:
        qitems.put(item)

    pid = os.getpid()
    procs = []
    start_time = time.time()
    start_flow = pbar_get_size()
    try:
        while not force_exit() and not qitems.empty() and not part_faild(pid):
            while not force_exit() and not qitems.empty() and len(procs) < tasknum and not part_faild(pid):

                now_flow = pbar_get_size()
                delta_flow = now_flow - start_flow
                delta_time = time.time() - start_time
                while flowwith and sleep_over() and delta_time > 0 and delta_flow / delta_time >= flowwith:
                    start_flow = now_flow
                    start_time = time.time()
                    # print(1.0 * delta_flow / flowwith - delta_time)
                    sleep_over()
                    globl.set_value_lock('isflow_sleep', True)
                    time.sleep(1.0 * delta_flow / flowwith - delta_time)
                    globl.set_value_lock('isflow_sleep', False)
                    now_flow = pbar_get_size()
                    delta_flow = now_flow - start_flow
                    delta_time = time.time() - start_time
                sleep_over()
                item = qitems.get()
                proc = process(target=func, args=(func_arg, item))
                proc.daemon = True
                sleep_over()
                proc.start()
                procs.append(proc)
                sleep_over()
            tmp_procs = procs
            for proc in tmp_procs:
                if not proc.is_alive():
                    procs = []
                    for proc in tmp_procs:
                        proc.join(0.001)
                        if proc.is_alive():
                            procs.append(proc)
                        else:
                            proc.join()
                            del(proc)
                    break
            del(tmp_procs)
        for proc in procs:
            proc.join()
            del(proc)
    except KeyboardInterrupt:
        # for pressing ctr+c in windows and linux system
        # windows can't use exit directly
        globl.set_value_lock('force_exit', True)
        if not is_windows :
            for proc in procs :
                proc.terminate()
                proc.join(0.001)
                # time.sleep(2)
                # exit(1)
        raise KeyboardInterrupt


def sleep_over():
    while globl.get_value('isflow_sleep').value:
        time.sleep(0.5)
    return True


def part_faild(pid):
    return pid in globl.get_value('part_task_failed')


def force_exit():
    return globl.get_value('force_exit').value


def multiprocess_with_sleep(func, func_arg, items, tasknum, flowwith=None):
    from obscmd.compat import Process
    multitask_with_sleep(Process, func, func_arg, items, tasknum, flowwith)


def multithreading_with_sleep(func, func_arg, items, tasknum, flowwith=None):
    from obscmd.multithreading import Process
    multitask_with_sleep(Process, func, func_arg, items, tasknum, flowwith)


def create_client(ak=None, sk=None, server=None):
    is_secure = False if config.client.secure == 'HTTP' else True
    return ObsClient(
        access_key_id=ak,
        secret_access_key=sk,
        server=server,
        is_secure=is_secure,
    )


def create_client_with_default_config():
    ak = config.client.access_key_id
    sk = config.client.secret_access_key
    server = config.client.server
    return create_client(ak, sk, server)


def check_resp(resp):
    if resp is not None and resp.status >= 300:
        raise InternalError(**{"status": resp.status, "reason": resp.reason, "code": resp.errorCode, "msg": resp.errorMessage})


def split_bucket_key(path):
    prefix = 'obs://'
    if path.startswith(prefix):
        path = path[len(prefix):]
    components = path.split('/')
    bucket = components[0]
    key = ""
    if len(components) > 1:
        key = '/'.join(components[1:])
    return bucket, key


def join_bucket_key(bucket, key=None):
    # case windows system path
    bucket = bucket.replace('\\', '/')
    key = safe_decode(key)
    if key:
        key = key.replace('\\', '/')
        obspath = 'obs://%s/%s' % (bucket.rstrip('/'), key.lstrip('/'))
    else:
        obspath = 'obs://%s' % bucket.rstrip('/')
    return obspath


def join_obs_path(path1, path2):
    if not path1:
        return path2
    if not path2:
        return path1
    return path1.rstrip('/') + '/' + path2.lstrip('/')


def get_bucket(path):
    bucket, key = split_bucket_key(path)
    return bucket


def get_object_key(path):
    bucket, key = split_bucket_key(path)
    return key


def get_object_name(path):
    bucket, key = split_bucket_key(path)
    return key.split('/')[-1]


def str_to_dict(str):
    return ast.literal_eval(str)


def pbar_add_size(size):
    lock = globl.get_value('pbar_lock')
    with lock:
        globl.get_value('pbar_value').value += size


def pbar_get_size():
    size = globl.get_value('pbar_value').value
    return size


def reset_partsize(total, old_partsize):
    num_counts = int(total / old_partsize)
    partsize = old_partsize
    if num_counts >= MAX_PART_NUM:
        partsize = int(math.ceil(float(total) / (MAX_PART_NUM - 1)))
    return partsize


def read_cp_files(cplist, size, optype):
    """
    计算文件夹下所有文件的断点续传文件已完成的总大小
    :param cplist: 
    :param size: 
    :param optype: 
    :return: 
    """
    total = 0
    for filepath in cplist:
        total += read_cp_file(filepath, size, optype)
    return total


def read_cp_file(filepath, size, upload_download='upload'):
    """
    读取检查点文件内容，获取已上传或下载文件段大小
    如果文件不存在则返回已获取的大小值，避免文件提前被api删除，进度条出现错误
    小文件和分段文件断点续传格式不一样
    {'total': total} in small file
    :param filepath: 
    :param size: 
    :param upload_download: 
    :return: 
    """
    item = 'downloadParts' if upload_download == 'download' else 'uploadParts'
    try:
        with open(filepath) as fp:
            info = json.load(fp)
            size = 0
            if 'total' in info:
                size = info['total']
            else:
                for part in info[item]:
                    if part['isCompleted']:
                        size += part['length']
    finally:
        return size
