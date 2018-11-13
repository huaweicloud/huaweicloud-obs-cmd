#!/usr/bin/python
# -*- coding:utf-8 -*-

import os
import json
import operator
import logging
# from obscmd import compat
import threading

import shutil

from obscmd import multithreading as compat, globl
from obscmd.cmds.obs.obsutil import multiprocess_with_sleep, check_resp, multithreading_with_sleep, ObsCmdUtil, \
    pbar_add_size, read_cp_file, split_bucket_key
from obscmd.config import TRY_MULTIPART_TIMES
from obscmd.utils import string2md5, move_file
from obs.const import LONG, IS_PYTHON2, UNICODE
from obs.model import BaseModel, CompletePart, CompleteMultipartUploadRequest, GetObjectRequest
from obs.util import safe_trans_to_gb2312, to_long, to_int

CRITICAL = logging.CRITICAL
ERROR = logging.ERROR
WARNING = logging.WARNING
INFO = logging.INFO
DEBUG = logging.DEBUG

logger = logging.getLogger("obscmd.file")


class Operation(object):
    def __init__(self, bucket, objkey, filename, partsize, tasknum, enable_checkpoint, checkpoint_file, obsclient):
        self.bucket = bucket
        self.objkey = objkey
        self.filename = filename
        self.partsize = partsize
        self.tasknum = tasknum
        self.enable_checkpoint = enable_checkpoint
        self.checkpoint_file = checkpoint_file
        self.obscmdutil = ObsCmdUtil(obsclient)

    def _get_record(self):
        # logger.info('load record file: %s' % self.checkpoint_file)
        if not os.path.exists(self.checkpoint_file):
            return None
        try:
            with open(safe_trans_to_gb2312(self.checkpoint_file), 'r') as f:
                content = json.load(f)
        except ValueError:
            logger.warning(
                'part task checkpoint file is broken. try to rebuild multipart task. file path is:{0}'.format(
                    self.checkpoint_file))
            # if 'upload' in self.checkpoint_file:
            #     self.obscmdutil.remove_object_multipart(self.bucket, self.objkey)
            self._delete_record()
            logger.warning('delete checkpoint, file path is:{0}'.format(self.checkpoint_file))
            return None
        else:
            return _parse_string(content)

    def _delete_record(self):
        if os.path.exists(safe_trans_to_gb2312(self.checkpoint_file)):
            os.remove(safe_trans_to_gb2312(self.checkpoint_file))
            logger.info('del record file success. path is:{0}'.format(self.checkpoint_file))

    def _write_record(self, record):
        with open(_to_unicode(self.checkpoint_file), 'w') as f:
            json.dump(record, f)
            # logger.info('write record file success. file path is {0}'.format(self.checkpoint_file))


class UploadOperation(Operation):
    """
    the upload checkpoint file 
    {
        "uploadFile": "/var/folders/qm/91dj5gns1t15c24r_l_f9rvc0000gn/T/tmpc2Vyky/file500M",
        "partEtags": [],
        "uploadParts": [{
            "isCompleted": false,
            "partNumber": 1,
            "length": 5242880,
            "offset": 0
        }, {
            "isCompleted": false,
            "partNumber": 2,
            "length": 5242880,
            "offset": 5242880
        }],
        "fileStatus": [524288000, 1533802015.0, "sdfwerwer2134"],
        "objectKey": "file500M",
        "uploadId": "000001651DB970309007ADEB9A68B45F",
        "bucketName": "7h2spihb",
        "partSize": 2343333
    }
    """

    def __init__(self, bucket, objkey, upload_file, partsize, tasknum, enable_checkpoint, checkpoint_file,
                 checksum, metadata, obsclient):
        super(UploadOperation, self).__init__(bucket, objkey, upload_file, partsize, tasknum, enable_checkpoint,
                                              checkpoint_file, obsclient)
        self.checksum = checksum
        self.metadata = metadata
        self.cmdtype = 'upload'

        try:
            self.size = os.path.getsize(self.filename)
            self.lastModified = os.path.getmtime(self.filename)
        except Exception:
            logger.warning('obtain uploadFile infomation error. Please check')
            self._delete_record()
            raise Exception('obtain uploadFile infomation error. Please check')
        resp_for_check_bucket = self.obscmdutil.head_bucket_nocheck(self.bucket)
        check_resp(resp_for_check_bucket)

        self._lock = compat.Lock()
        self._exception = []
        self._record = None

    def upload(self, flowwith):
        if self.enable_checkpoint:
            self._load_record()
        else:
            self._prepare()
        if os.path.exists(self.checkpoint_file):
            start_total = read_cp_file(self.checkpoint_file, 0, self.cmdtype)
            pbar_add_size(start_total)

        self._upload_parts = self._get_upload_parts()

        part_etag_info = {}
        for part_info in self._record['partEtags']:
            part_etag_info[part_info['partNum']] = part_info['etag']

        upload_info = []
        for part_info in self._record['uploadParts']:
            upload_info.append(part_info['isCompleted'])

        part_etag_infos = compat.Dict(part_etag_info)
        upload_infos = compat.List(upload_info)
        status = compat.Value('i', 0)
        multithreading_with_sleep(self._upload_part_for_process, (part_etag_infos, upload_infos, status),
                                  self._upload_parts, self.tasknum, flowwith)

        if False in upload_infos:
            if status.value == 1:
                self.obscmdutil.abort_multipart_upload(self.bucket, self.objkey, self._record['uploadId'])
                logger.warning('the code from server is 4**, please check')
                self._delete_record()
            if len(self._exception) > 0:
                raise Exception(self._exception[0])
            raise Exception('%s some parts are failed. Please try agagin, %s' % (self.cmdtype, self.filename))

        part_etags = []
        tmp_part_etags = sorted(part_etag_infos.items(), key=lambda d: d[0])
        for key, value in tmp_part_etags:
            part_etags.append(CompletePart(partNum=to_int(key), etag=value))

        resp = self.obscmdutil.complete_multipart_upload(self.bucket, self.objkey, self._record['uploadId'],
                                                         CompleteMultipartUploadRequest(part_etags))

        if 300 < resp.status < 500:
            logger.error('complete multipart.ErrorCode:{0}. ErrorMessage:{1}'.format(
                resp.errorCode, resp.errorMessage))
            self.obscmdutil.abort_multipart_upload(self.bucket, self.objkey, self._record['uploadId'])
        if self.enable_checkpoint:
            self._delete_record()

        return resp

    def _upload_part_for_process(self, func_args, item_args):
        part = item_args
        partEtag_infos, upload_infos, status = func_args

        if status.value == 0:
            resp = self.real_upload(part)
            i = 0
            while resp.status > 300 and i < TRY_MULTIPART_TIMES:
                logger.warning('retry %s part, %d times, part number: %d' % (self.cmdtype, i, part['partNumber']))
                resp = self.real_upload(part)
                i += 1

            if resp.status < 300:
                complete_part = CompletePart(to_int(part['partNumber']), resp.body.etag)
                partEtag_infos[to_int(part['partNumber'])] = resp.body.etag
                upload_infos[to_int(part['partNumber']) - 1] = True
                if self.enable_checkpoint:
                    with self._lock:
                        record = self._get_record()
                        record['uploadParts'][part['partNumber'] - 1]['isCompleted'] = True
                        record['partEtags'].append(complete_part)
                        self._write_record(record)
                pbar_add_size(part['length'])
                logger.info('%s part %d complete, %s, uploadid=%s' % (self.cmdtype, part['partNumber'], self.filename, self.uploadId))
            elif 300 < resp.status < 500:
                logger.warning('server error. ErrorCode:{0}, ErrorMessage:{1}'.format(
                    resp.errorCode, resp.errorMessage))
                self._exception.append('errorCode:{0}.errorMessage:{1}'.format(resp.errorCode, resp.errorMessage))
                with self._lock:
                    status.value = 1
                upload_infos[to_int(part['partNumber']) - 1] = False
                globl.append_list_lock('part_task_failed', os.getpid())

    def real_upload(self, part):
        resp = self.obscmdutil.upload_part(
            self.bucket, self.objkey, part['partNumber'], self._record['uploadId'], self.filename,
            isFile=True, partSize=part['length'], offset=part['offset'])
        return resp

    def _check_upload_record(self, record):
        try:
            if not operator.eq([record['bucketName'], record['objectKey'], record['uploadFile']],
                               [self.bucket, self.objkey, self.filename]):
                logger.warning('the bucketName or objectKey or uploadFile was changed. clear the record')
                return False
            if record['uploadId'] is None:
                logger.warning('{0} (uploadId) not exist, clear the record.'.format(record['upload_id']))
                return False

            if record['fileStatus'][0] != self.size:
                logger.warning('{0} was changed, clear the record.'.format(self.filename))
                return False

            if record['fileStatus'][1] < os.path.getmtime(self.filename):
                logger.warning('{0} content was changed, clear the record.'.format(self.filename))
                return False
        except:
            return False
        return True

    def _file_status(self):
        fileStatus = [self.size, self.lastModified]
        return fileStatus

    def _get_upload_parts(self):
        final_upload_parts = []
        for p in self._record['uploadParts']:
            if not p['isCompleted']:
                final_upload_parts.append(p)
        return final_upload_parts

    def _load_record(self):
        self._record = self._get_record()
        if self._record:
            self.uploadId = self._record['uploadId']
            if not self._check_upload_record(self._record):
                if self._record['uploadId'] is not None:
                    self.obscmdutil.abort_multipart_upload(self.bucket, self.objkey, self._record['uploadId'])
                logger.warning('checkpointFile is invalid, %s' % self.checkpoint_file)
                self._delete_record()
                # self.obscmdutil.remove_object_multipart(self.bucket, self.objkey)
                self._record = None

        if not self._record:
            self._prepare()

    def _prepare(self):
        file_status = self._file_status()
        upload_parts = slice_file(self.size, self.partsize)
        self.part_etags = []
        resp = self.obscmdutil.initiate_multipart_upload(self.bucket, self.objkey, metadata=self.metadata)
        self.uploadId = resp.body.uploadId
        self._record = {'bucketName': self.bucket, 'objectKey': self.objkey, 'uploadId': self.uploadId,
                        'uploadFile': self.filename, 'fileStatus': file_status, 'uploadParts': upload_parts,
                        'partEtags': self.part_etags, 'partSize': self.partsize}
        if self.enable_checkpoint:
            self._write_record(self._record)
        logger.info('prepare upload task success, %s, uploadId = %s' % (self.filename, self.uploadId))


##################################################


class DownloadOperation(Operation):
    def __init__(self, bucket, objkey, download_file, partsize, tasknum, enable_checkpoint, checkpoint_file,
                 header, versionid, obsclient):
        super(DownloadOperation, self).__init__(bucket, objkey, download_file, partsize, tasknum,
                                                enable_checkpoint,
                                                checkpoint_file, obsclient)
        self.header = header
        self.versionid = versionid
        self.cmdtype = 'download'
        self._lock = compat.Lock()
        self._record = None
        self._tmp_file = self._make_tmp_filepath(self.filename)

        metedata_resp = self.obscmdutil.get_object_metadata_nocheck(self.bucket, self.objkey, self.versionid)
        if metedata_resp.status < 300:
            self.lastModified = metedata_resp.body.lastModified
            self.size = metedata_resp.body.contentLength
        else:
            logger.warning(
                'touch the objetc {0} error. ErrorCode:{1}, ErrorMessage:{2}'.format(
                    self.objkey, metedata_resp.errorCode, metedata_resp.errorMessage))
            self._delete_record()
            raise Exception(
                'touch the objetc {0} error. ErrorCode:{1}, ErrorMessage:{2}'.format(
                    self.objkey, metedata_resp.status, metedata_resp.errorMessage))
        self._metedata_resp = metedata_resp
        self._exception = []

    def download(self, flowwith):
        if not self.enable_checkpoint:
            self._prepare()
        else:
            self._load_record()
        if os.path.exists(self.checkpoint_file):
            start_total = read_cp_file(self.checkpoint_file, 0, self.cmdtype)
            pbar_add_size(start_total)

        self._down_parts = [part for part in self._record['downloadParts'] if not part['isCompleted']]

        download_info = []
        for part_info in self._record['downloadParts']:
            download_info.append(part_info['isCompleted'])

        download_infos = compat.List(download_info)
        status = compat.Value('i', 0)

        multithreading_with_sleep(self._download_part_process, (download_infos, status),
                                  self._down_parts, self.tasknum, flowwith)

        if False in download_infos:
            if status.value == 1:
                self._clean_download()
                if len(self._exception) > 0:
                    raise Exception(self._exception[0])
            raise Exception('%s some parts are failed. Please try agagin, %s' % (self.cmdtype, self.filename))

        # move_file(self._tmp_file, self.filename)
        os.rename(self._tmp_file, self.filename)
        if self.enable_checkpoint:
            self._delete_record()
        logger.info('download success, %s' % self.filename)
        return self.obscmdutil.get_object_metadata_nocheck(
            self._record['bucketName'], self._record['objectKey'], self._record['versionId'])

    def _download_part_process(self, func_args, item_args):
        part = item_args
        download_infos, status = func_args
        if status.value == 0:
            get_object_request = GetObjectRequest(versionId=self._record['versionId'])
            self.header.range = '%d-%d' % (part['offset'], part['offset'] + part['length'] - 1)
            try:
                resp = self.obscmdutil.get_object(bucketName=self._record['bucketName'],
                                                  objectKey=self._record['objectKey'],
                                                  getObjectRequest=get_object_request, headers=self.header)

                i = 0
                while resp.status > 300 and i < TRY_MULTIPART_TIMES:
                    logger.warning('retry %s part, %d times, part number: %d' % (self.cmdtype, i, part['partNumber']))
                    resp = self.obscmdutil.get_object(bucketName=self._record['bucketName'],
                                                      objectKey=self._record['objectKey'],
                                                      getObjectRequest=get_object_request, headers=self.header)
                    i += 1

                if resp.status < 300:
                    respone = resp.body.response
                    chunk_size = 65536
                    if respone is not None:
                        with open(_to_unicode(self._tmp_file), 'rb+') as f:
                            f.seek(part['offset'], 0)
                            while True:
                                chunk = respone.read(chunk_size)
                                if not chunk:
                                    break
                                f.write(chunk)
                            respone.close()

                    download_infos[part['partNumber'] - 1] = True
                    logger.info('%s part %d complete, %s' % (self.cmdtype, part['partNumber'], self.filename))
                    if self.enable_checkpoint:
                        with self._lock:
                            record = self._get_record()
                            record['downloadParts'][part['partNumber'] - 1]['isCompleted'] = True
                            self._write_record(record)
                    pbar_add_size(part['length'])
                else:
                    with self._lock:
                        download_infos[part['partNumber'] - 1] = False
                        status.value = 1
                    self._exception.append(
                        'response from server error. ErrorCode:{0}, ErrorMessage:{1}'.format(resp.errorCode, resp.errorMessage))
                    globl.append_list_lock('part_task_failed', os.getpid())

            except Exception as e:
                download_infos[part['partNumber'] - 1] = False
                globl.append_list_lock('part_task_failed', os.getpid())
                msg = '%s part %d error, %s, %s' % (self.cmdtype, part['partNumber'], self.filename, e)
                self._exception.append(msg)
                logger.warning(msg)
                raise e

    def _load_record(self):
        self._record = self._get_record()
        if self._record and not self._check_download_record(self._record):
            self._clean_download()
            self._record = None
        if not self._record:
            self._prepare()

    def _prepare(self):
        # self._remove_downloaded_file()
        self._make_tmp_file()

        self.down_parts = slice_file(self.size, self.partsize)
        object_staus = [self.objkey, self.size, self.lastModified, self.versionid]

        self._record = {'bucketName': self.bucket, 'objectKey': self.objkey, 'versionId': self.versionid,
                        'downloadFile': self.filename, 'downloadParts': self.down_parts, 'objectStatus': object_staus,
                        'partSize': self.partsize
                        }
        logger.info('prepare download task success, %s' % self.filename)
        if self.enable_checkpoint:
            self._write_record(self._record)

    def _check_download_record(self, record):
        """
        first check checkpoint file base info
        than check complete parts size
        :param record: 
        :return: 
        """
        if not (os.path.exists(self._tmp_file) and os.path.getsize(self._tmp_file) == self.size):
            return False
        try:
            if not operator.eq([record['bucketName'], record['objectKey'], record['versionId'],
                                record['downloadFile']],
                               [self.bucket, self.objkey, self.versionid,
                                self.filename]):
                logger.warning('the bucketName or objectKey or downloadFile was changed. clear the record')
                return False
            object_meta_resp = self.obscmdutil.get_object_metadata_nocheck(self.bucket, self.objkey,
                                                                           self.versionid)
            check_resp(object_meta_resp)
            if not operator.eq(record['objectStatus'],
                               [self.objkey, object_meta_resp.body.contentLength,
                                object_meta_resp.body.lastModified, self.versionid]):
                logger.warning('the versionId or lastModified was changed. clear the record')
                return False
        except:
            return False
        return True


    def _remove_tmp_file(self):
        if os.path.exists(self._tmp_file):
            os.remove(self._tmp_file)

    def _remove_downloaded_file(self):
        if os.path.exists(self.filename):
            os.remove(self.filename)

    def _make_tmp_filepath(self, filepath, num=None):
        if num is None:
            filepath = '%s.download.tmp' % filepath
        else:
            filepath = '%s.download.tmp.%d' % (filepath, num)
        return filepath

    def _make_tmp_file(self):
        fpath, fname = os.path.split(self._tmp_file)
        if not os.path.exists(fpath):
            os.makedirs(fpath)
        with open(_to_unicode(self._tmp_file), 'wb') as f:
            f.seek(self.size - 1, 0)
            f.write(b'b')

    def _clean_download(self):
        self._delete_record()
        # self._remove_downloaded_file()
        self._remove_tmp_file()


class CopyOperation(UploadOperation):

    def __init__(self, bucket, objkey, src_obspath, partsize, tasknum, enable_checkpoint, checkpoint_file,
                 checksum, metadata, obsclient):
        super(UploadOperation, self).__init__(bucket, objkey, src_obspath, partsize, tasknum, enable_checkpoint,
                                        checkpoint_file, obsclient)
        self.checksum = checksum
        self.metadata = metadata
        self.cmdtype = 'copy'
        self.copy_source = self.filename.replace('obs://', '')
        srcb, srck = split_bucket_key(src_obspath)
        metedata_resp = self.obscmdutil.get_object_metadata_nocheck(srcb, srck)
        if metedata_resp.status < 300:
            self.lastModified = metedata_resp.body.lastModified
            self.size = metedata_resp.body.contentLength
        else:
            logger.warning(
                'touch the objetc {0} error. ErrorCode:{1}, ErrorMessage:{2}'.format(
                    self.objkey, metedata_resp.errorCode, metedata_resp.errorMessage))
            self._delete_record()
            raise Exception(
                'touch the objetc {0} error. ErrorCode:{1}, ErrorMessage:{2}'.format(
                    self.objkey, metedata_resp.status, metedata_resp.errorMessage))
        resp_for_check_bucket = self.obscmdutil.head_bucket_nocheck(self.bucket)
        check_resp(resp_for_check_bucket)

        self._lock = compat.Lock()
        self._exception = []
        self._record = None

    def copy(self, flowwith):
        self.upload(flowwith)

    def real_upload(self, part):
        copy_source_range = '%d-%d' % (part['offset'], part['offset'] + part['length'] - 1)
        resp = self.obscmdutil.copy_part(
            self.bucket, self.objkey, part['partNumber'],
            self._record['uploadId'], self.copy_source, copy_source_range)
        return resp

    def _check_upload_record(self, record):
        try:
            if not operator.eq([record['bucketName'], record['objectKey'], record['uploadFile']],
                               [self.bucket, self.objkey, self.filename]):
                logger.warning('the bucketName or objectKey or uploadFile was changed. clear the record')
                return False
            if record['uploadId'] is None:
                logger.warning('{0} (uploadId) not exist, clear the record.'.format(record['upload_id']))
                return False

            if record['fileStatus'][0] != self.size:
                logger.warning('{0} was changed, clear the record.'.format(self.filename))
                return False

            if record['fileStatus'][1] != self.lastModified:
                logger.warning('{0} content was changed, clear the record.'.format(self.filename))
                return False
        except:
            return False
        return True


class Part(BaseModel):
    allowedAttr = {'partNumber': LONG, 'offset': LONG, 'length': LONG, 'isCompleted': bool}

    def __init__(self, partNumber, offset, length, isCompleted=False):
        self.partNumber = partNumber
        self.offset = offset
        self.length = length
        self.isCompleted = isCompleted


def _parse_string(content):
    if IS_PYTHON2:
        if isinstance(content, dict):
            return dict([(_parse_string(key), _parse_string(value)) for key, value in content.iteritems()])
        elif isinstance(content, list):
            return [_parse_string(element) for element in content]
        elif isinstance(content, UNICODE):
            return content.encode('utf-8')
    return content


def _to_unicode(data):
    if isinstance(data, bytes):
        return data.decode('utf-8')
    return data


def slice_file(size, partsize):
    parts = []
    num_counts = int(size / partsize)
    if size % partsize != 0:
        num_counts += 1
    offset = 0
    for i in range(1, num_counts + 1, 1):
        length = partsize if i != num_counts else size - (i - 1) * partsize
        part = Part(to_long(i), to_long(offset), to_long(length), False)
        offset += partsize
        parts.append(part)
    return parts
