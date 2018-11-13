#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import json
import os
import time
import fnmatch

from obs import GetObjectHeader
from tqdm import tqdm

from obscmd.cmds.obs.obsutil import multiprocess_with_sleep, split_bucket_key, check_resp, join_bucket_key, ObsCmdUtil, \
    get_object_name, get_object_key, join_obs_path, pbar_add_size, pbar_get_size, reset_partsize
from obscmd.cmds.obs.subcmd import SubObsCommand
from obscmd.cmds.obs.transfer import UploadOperation, DownloadOperation, CopyOperation
from obscmd.constant import PUTFILE_MAX_SIZE
from obscmd.exceptions import CommandParamValidationError, MaxPartNumError, NotSupportError
from obscmd.utils import string2md5, get_files_size, bytes_to_unitstr, get_flowwidth_from_flowpolicy, \
    unitstr_to_bytes, get_full_path, check_value_threshold
from obscmd.config import CP_DIR, PARTSIZE_MINIMUM, PARTSIZE_MAXIMUM, FILE_LIST_DIR, MAX_PART_NUM, BAR_NCOLS, BAR_MS2S, \
    BAR_MININTERVAL, BAR_MINITERS, BAR_SLEEP_FOR_UPDATE
from obscmd import compat, globl
from obs.util import safe_encode


def progressbar(total, alive, desc=None):
    """
    progressbar for single file uploading, downloading, copying
    :param cpfilepath: checkpoint file
    :param total: file size
    :param alive: 0-init state; 1-task complete; 2-task failed
    :param desc: 
    :return: 
    """
    with tqdm(total=total, unit="B", unit_scale=True, mininterval=BAR_MININTERVAL, miniters=BAR_MINITERS, desc=desc,
              unit_divisor=1024, ncols=BAR_NCOLS, postfix={'td': 0, 'tps': 0}) as pbar:
        size = 0
        tmp_latency = '0ms'
        tmp_tps = '0'
        while size < total:
            size = pbar_get_size()
            size = size if size < total else total
            updata = size - pbar.n
            updata = check_value_threshold(updata, 0, total)

            pbar.set_description(desc)
            pbar.update(updata)
            td, tps = get_latency_tps()
            if td != tmp_latency or tps != tmp_tps:
                pbar.set_postfix(td=td, tps=tps)
                tmp_latency, tmp_tps = td, tps

            if alive.value > 0:
                if alive.value == 1 and not globl.get_value('force_exit').value:
                    pbar.update(total - pbar.n)
                break
            time.sleep(BAR_SLEEP_FOR_UPDATE)


def start_progressbar(total, desc=None):
    alive = compat.Value('i', 0)
    p = compat.Process(target=progressbar, args=(total, alive, desc))
    p.daemon = True
    p.start()
    return p, alive


def end_progressbar(process, alive, status):

    alive.value = 1 if status else 2
    process.join()
    time.sleep(0.1)


def get_latency_tps():
    values = globl.get_value('value')
    tmp_timestamp = values[-1][0] if values is not None and len(values) else 0

    latencies = []
    for timestamp, latency in values:
        if tmp_timestamp - timestamp <= 1:
            latencies.append(latency)
    try:
        td = sum(latencies)/len(latencies)
        # td = max(latencies)
    except:
        td = 0
    td = ('%dms' % td) if td < BAR_MS2S else ('%.2fs' % (1.0 * td / BAR_MS2S,))
    tps = len(latencies)
    return td, tps


class CpCommand(SubObsCommand):
    NAME = 'cp'
    DESCRIPTION = "Copies a local file or obs object to another location " \
                  "locally or in obs."
    USAGE = "cp <LocalPath> <ObsPath> or <ObsPath> <LocalPath> " \
            "or <ObsPath> <ObsPath> [--md5] [--recursive] [--update] [--exclude] [--include] [--tasknum]"
    ARG_TABLE = [
        {'name': 'srcpath', 'positional_arg': True,
         'help_text': USAGE},
        {'name': 'destpath', 'positional_arg': True,
         'help_text': USAGE},
        {'name': 'md5', 'action': 'store_true',
         'help_text': "after completing, compare local file md5 and obs etags."},
        {'name': 'recursive', 'action': 'store_true',
         'help_text': (
             "Command is performed on all files or objects "
             "under the specified directory or prefix.")},
        {'name': 'update', 'action': 'store_true',
         'help_text': (
             "upload the modified and new files in local dictionary")},
        {'name': 'include',
         'help_text': (
             "Don't exclude files or objects in the command that match the specified pattern")},
        {'name': 'exclude',
         'help_text': "Exclude all files or objects from the command that matches the specified pattern"},
        {'name': 'tasknum', 'cli_type_name': 'integer',
         'help_text': "number of tasks for parallel"},
        {'name': 'parttasknum', 'cli_type_name': 'integer',
         'help_text': "number of tasks for parallel part uploading or downloading"},
        {'name': 'partsize',
         'help_text': "partsize of bigfile for parallel"}
    ]

    EXAMPLES = """
        uploading a local file to obs

        The following cp command copies a single file to a specified bucket and
        key:

            obscmd obs cp test.txt obs://mybucket/test2.txt

        Output:
            upload: 100%||||||||||||||||||| 15.7M/15.7M [00:04<00:00, 84.1B/s]
            15.7M test.txt --> obs://mybucket/test2.txt
        
        If upload files to a bucket that is not exists:
            obscmd obs cp upload obs://tests --recursive
        
        Output:
            Internal error. Error code: NoSuchBucket, Error msg: The specified bucket does not exist
            
            upload: 100%||||||||||||||||||| 15.7M/15.7M [00:00<00:00, 31.2MB/s]
            
            upload complete 0 files:
            
            upload failed 3 files:
               14.9M	upload/20news-bydate.pkz --> obs://tests/20news-bydate.pkz
                321B	upload/test2.cp --> obs://tests/test2.cp
                742B	upload/test.cp --> obs://tests/test.cp

        
        Copying a file from obs to obs

        The  following  cp  command  copies  a  single obs object to a specified
        bucket and key:
        
            obscmd obs cp obs://mybucket/test.txt obs://mybucket/test2.txt
        
        Output:
        
            copy: 100%||||||||||||||||||| 15.7M/15.7M [00:04<00:00, 84.1B/s]
            15.7M obs://mybucket/test.txt --> obs://mybucket/test2.txt
        
        downloading an obs object to a local file
        
        The following cp command copies a single object  to  a  specified  file
        locally:
        
            obscmd obs cp obs://mybucket/test.txt test2.txt
          
        Output:
        
            download: 100%||||||||||||||||||| 15.7M/15.7M [00:04<00:00, 84.1B/s]
            15.7M obs://mybucket/test.txt --> ./test2.txt
        
        Copying an obs object from one bucket to another
        
        The  following  cp command copies a single object to a specified bucket
        while retaining its original name:
        
            obscmd obs cp obs://mybucket/test.txt obs://mybucket2/
        
        Output:
        
            copy: 100%||||||||||||||||||| 15.7M/15.7M [00:04<00:00, 84.1B/s]
            15.7M obs://mybucket/test.txt --> obs://mybucket2/test.txt
        
        Recursively copying obs objects to a local directory
        
        When passed with the parameter --recursive, the  following  cp  command
        recursively copies all objects under a specified prefix and bucket to a
        specified directory.  In this example,  the  bucket  mybucket  has  the
        objects test1.txt and test2.txt:
        
          obscmd obs cp obs://mybucket . --recursive
        
        Output:
        
          download: obs://mybucket/test1.txt --> test1.txt
          download: obs://mybucket/test2.txt --> test2.txt
        
        Recursively copying local files to obs
        
        When  passed  with  the parameter --recursive, the following cp command
        recursively copies all files under a specified directory to a specified
        bucket  and  prefix  while  excluding  some files by using an --exclude
        parameter.   In  this  example,  the  directory  myDir  has  the  files
        test1.txt and test2.jpg:
            obscmd obs cp myDir obs://obs-sh-test1/doc --recursive --exclude "*.jpg"

       Output:

            upload: 100%||||||||||||||||||| 15.7M/15.7M [00:20<00:00, 606B/s]
            upload complete 3 files:
                321B	myDir/test2.cp --> obs://obs-sh-test1/doc/test2.cp
                742B	myDir/test.cp --> obs://obs-sh-test1/doc/test.cp
               14.9M	myDir/20news-bydate.pkz --> obs://obs-sh-test1/doc/20news-bydate.pkz


       Recursively copying obs objects to another bucket

       When  passed  with  the parameter --recursive, the following cp command
       recursively copies all objects under  a  specified  bucket  to  another
       bucket  while  excluding  some objects by using an --exclude parameter.
       In this example, the bucket mybucket  has  the  objects  test1.txt  and
       another/test1.txt:

          obscmd obs cp obs://mybucket/ obs://mybucket2/ --recursive --exclude "another/*"

       Output:

          copy: obs://mybucket/test1.txt to obs://mybucket2/test1.txt

       You  can  combine  --exclude and --include options to copy only objects
       that match a pattern, excluding all others:

          obscmd obs cp obs://mybucket/logs/ obs://mybucket2/logs/ --recursive --exclude "*" --include "*.log"

       Output:

          copy: obs://mybucket/test/test.log to obs://mybucket2/test/test.log
          copy: obs://mybucket/test3.log to obs://mybucket2/test3.log
          
        If check md5 with obs file and local file, set --md5 option, upload and download operations are valid.
        
            obscmd obs cp obscmd.cmd obs://hellob --md5
        
        Output:
            upload: 100%||||||||||||||||||| 1.40k/1.40k [00:00<00:00, 2.87kB/s]
            upload complete, 1.4K obscmd.cmd --> obs://hellob/obscmd.cmd
            
            md5 check (result, obs_md5, local_md5, obs_path, local_path):
            ok	"bf4b89fc2c705a45057f73611258ba04"	"bf4b89fc2c705a45057f73611258ba04" obs://hellob/obscmd.cmd  obscmd.cmd

    """

    def _run(self, parsed_args, parsed_globals):

        self.srcpath = parsed_args.srcpath
        self.destpath = parsed_args.destpath
        self.md5 = parsed_args.md5
        self.recursive = parsed_args.recursive
        self.update = parsed_args.update
        self.include = parsed_args.include
        self.exclude = parsed_args.exclude

        # read from configure file
        self.tasknum = int(parsed_args.tasknum) if parsed_args.tasknum else int(self.session.config.task.tasknum)
        self.parttasknum = int(parsed_args.parttasknum) if parsed_args.parttasknum else int(self.session.config.task.parttasknum)
        self.partsize = unitstr_to_bytes(parsed_args.partsize) if parsed_args.partsize else unitstr_to_bytes(self.session.config.task.partsize)
        self.partsize = check_value_threshold(self.partsize, PARTSIZE_MINIMUM, PARTSIZE_MAXIMUM)
        if self.session.config.task.flowwidth == '0':
            self.flowwidth = None
        else:
            flowpolicy = self.session.config.task.flowpolicy
            tmpwidth = get_flowwidth_from_flowpolicy(flowpolicy) if flowpolicy else None
            self.flowwidth = tmpwidth if tmpwidth else unitstr_to_bytes(self.session.config.task.flowwidth)
        self.part_threshhold = unitstr_to_bytes(self.session.config.task.part_threshhold)

        self.obs_cmd_util = ObsCmdUtil(self.client)

        self.now = time.strftime('%Y.%m.%d_%H.%M.%S', time.localtime(time.time()))

        # flowtime secondes flow threshold
        self.cmdtype = self._check_path_type([self.srcpath, self.destpath])

        if self.update and self.cmdtype in ['download', 'copy']:
            raise NotSupportError(**{'msg': 'update option in download or copy operations'})

        self.print_cp_params()
        self.cp_dir_or_files()

        method = getattr(self, '_' + self.cmdtype, None)
        method()
        return 0


    def print_cp_params(self):
        self._outprint('some cp command parameters:')
        self._outprint('tasknum: %d\t\tparttasknum: %d' % (self.tasknum, self.parttasknum))
        partsize = bytes_to_unitstr(self.partsize)
        part_threshhold = bytes_to_unitstr(self.part_threshhold)
        flowwidth = bytes_to_unitstr(self.flowwidth) if self.flowwidth else 'None'
        self._outprint('partsize: %s\t\tpart_threshhold: %s' % (partsize, part_threshhold))
        self._outprint('flowwidth: %s' % flowwidth)


    def cp_dir_or_files(self):
        """
        if srcpath ends with / or \(in windows), then just cp files in the directroy, not include the dir file.
        otherwise, cp files in the directory and include the dir file.
        :return: 
        """
        if self.recursive:
            if self.cmdtype == 'upload' and not self.srcpath.endswith(os.path.sep):
                basename = os.path.basename(self.srcpath)
                self.destpath = join_obs_path(self.destpath, basename)
            elif self.cmdtype == 'download' and not self.srcpath.endswith('/'):
                bucket, key = split_bucket_key(self.srcpath)
                basename = key.split('/')[-1]
                if basename:
                    self.destpath = os.path.join(self.destpath, basename)
            elif not self.srcpath.endswith('/'):
                bucket, key = split_bucket_key(self.srcpath)
                basename = key.split('/')[-1]
                if basename:
                    self.destpath = join_obs_path(self.destpath, basename)

    def _upload(self):
        bucket, key = split_bucket_key(self.destpath)
        if os.path.isdir(self.srcpath) and not self.recursive:
            raise CommandParamValidationError(**{'report': 'upload directory need recursive option'})
        if self.recursive:
            self._upload_dir(self.srcpath, bucket, key)
        else:
            self._upload_file(self.srcpath, bucket, key)

    def _download(self):
        bucket, key = split_bucket_key(self.srcpath)

        if self.recursive:
            self._download_dir(bucket, key)
        else:
            self._download_file(bucket, key)

    def _copy(self):
        if self.recursive:
            self._copy_dir(self.srcpath, self.destpath)
        else:
            destb, destk = split_bucket_key(self.destpath)
            # copyObject destkey could not be empty
            srcobj = get_object_name(self.srcpath)
            destk = join_obs_path(destk, srcobj)
            self._copy_file(self.srcpath, join_bucket_key(destb, destk))

    def _upload_file_for_process(self, args, item_args):
        filepath, key = item_args
        bucket, lock, oklist, failedlist = args
        return self._upload_file(filepath, bucket, key, lock, oklist, failedlist)

    def _upload_file(self, filepath, bucket, key, lock=None, oklist=None, failedlist=None):
        filename = os.path.basename(filepath)
        objkey = key.strip('/') + '/' + filename if key else filename
        total = os.path.getsize(filepath)
        partsize = total if total < self.part_threshhold else reset_partsize(total, self.partsize)
        if self.part_threshhold > PUTFILE_MAX_SIZE and total > PUTFILE_MAX_SIZE :
            raise NotSupportError(
                **{'msg' : 'single file (not multipart upload) size is bigger than 5G, %s' % self.srcpath})
        prc = None
        alive = None
        status = True
        if not self.recursive:
            prc, alive = start_progressbar(total, self.cmdtype)

        try:
            metadata = {'x-amz-meta-partsize': partsize}
            if total < self.part_threshhold:
                self.session.logger.debug('total is %s, put file now!' % total)
                self.obs_cmd_util.put_file(bucket, objkey, filepath, metadata)
                pbar_add_size(total)
            else:
                cpfilepath = self._checkpoint_filepath(bucket, objkey, filepath, self.cmdtype)

                upload_operation = UploadOperation(bucket, objkey, filepath, partsize, self.parttasknum,
                                                   True, cpfilepath, None, metadata, self.client)
                resp = upload_operation.upload(self.flowwidth)
                check_resp(resp)
        except Exception as e:
            self.session.logger.error(e)
            self.session.logger.error(safe_encode('%s upload failed' % filepath))
            status = False
        if not self.recursive:
            try:
                end_progressbar(prc, alive, status)
                self._print_result(status, total, filepath, join_bucket_key(bucket, objkey))
                if self.md5 and status:
                    self._outprint('calculating md5...')
                    md5_ret = self.obs_cmd_util.check_etag_with_obs_local(bucket, objkey, filepath, partsize, self.part_threshhold)
                    self._print_md5_result(md5_ret)
                    if 'failed' == md5_ret[0]:
                        self.obs_cmd_util.delete_object(bucket, objkey)
                        self._outprint('delete object: %s...' %md5_ret[3])
            except Exception as e:
                self.session.logger.error(safe_encode('catched exception when check md5. %s' % e))
                self.session.logger.info('delete object now... %s' %objkey)
                self.obs_cmd_util.delete_object(bucket, objkey)
                self._outprint('delete object: %s...' % objkey)

        if self.recursive:
            if status:
                self.session.logger.info(safe_encode('%s upload success' % filepath))
            with lock:
                result = (total, filepath, join_bucket_key(bucket, objkey))
                oklist.append(result) if status else failedlist.append(result)

        return 0

    def _upload_dir(self, filedir, bucket, key):
        localfiles = []

        for root, dirs, files in os.walk(filedir):
            for file in files:
                filepath = os.path.join(root, file)

                # filter exclude include pattern
                if self._filter_patterns(filepath, filedir, self.exclude, self.include):
                    continue
                # for upload operation，obs will add local file name to obs key
                # so, here must romove local file name
                obsprefix = filepath[len(filedir):].rstrip(file).strip('/').strip('\\').replace('\\', '/')
                prefix = join_obs_path(key, obsprefix) if key and obsprefix else key + obsprefix
                localfiles.append((filepath, prefix))

        if self.update:
            obsfiles = self._list_obsfiles(bucket, key)
            localfiles = self.localfiles_for_update(localfiles, obsfiles)

        localfiles = self._upload_part_first(localfiles)
        prefixes = self.make_obs_dirs(filedir, bucket, key)
        if len(localfiles) == 0:
            self._outprint('No files to %s.' % self.cmdtype)
            for prefix in prefixes:
                self._outprint('created obs dir: %s' % prefix)
            return 0

        total = get_files_size([filepath for filepath, _ in localfiles])

        alive = compat.Value('i', 0)
        lock = compat.Lock()
        oklist = compat.List()
        failedlist = compat.List()

        pbar = compat.Process(target=progressbar, args=(total, alive, self.cmdtype))
        pbar.daemon = True
        pbar.start()

        multiprocess_with_sleep(self._upload_file_for_process, (bucket, lock, oklist, failedlist),
                                localfiles, self.tasknum, self.flowwidth)
        alive.value = 1 if not failedlist else 2
        pbar.join()
        self._print_result_list(oklist, failedlist)

        if self.md5:
            try:
                self._outprint('calculating md5...')
                local_obs_paths = [(kv[1], get_object_key(kv[2])) for kv in oklist]
                md5_rets = self.obs_cmd_util.check_dir_etag_with_local_obs(bucket, key, local_obs_paths, self.partsize, self.part_threshhold)
                self._print_dir_md5_result(md5_rets)
                for md5_ret in md5_rets:
                    if 'failed' == md5_ret[0]:
                        self.obs_cmd_util.delete_object(bucket, get_object_key(md5_ret[3]))
                        self._outprint('delete object: %s...' % md5_ret[3])
            except Exception as e:
                self.session.logger.error(safe_encode('catched exception when check md5. %s' % e))
                for kv in oklist:
                    objkey = get_object_key(kv[2])
                    self.session.logger.debug('delete object now... %s' % objkey)
                    self.obs_cmd_util.delete_object(bucket, objkey)
                self._outprint('delete object')

        return 0

    def _download_file_for_process(self, args, item_args):
        key = item_args
        bucket, lock, oklist, failedlist = args
        return self._download_file(bucket, key, lock, oklist, failedlist)

    def _download_file(self, bucket, key, lock=None, oklist=None, failedlist=None):

        filepath = self.make_download_filepath(key)
        status = True
        total = 0
        prc = None
        alive = None
        try:
            total = self.obs_cmd_util.get_object_size(bucket, key)
            partsize = self.partsize if total < self.part_threshhold else reset_partsize(total, self.partsize)
            if not self.recursive :
                prc, alive = start_progressbar(total, self.cmdtype)
            dirpath = filepath if filepath.endswith(os.path.sep) else os.path.dirname(filepath)
            if lock is not None:
                with lock:
                    if not os.path.exists(dirpath):
                        os.makedirs(dirpath)
            if total < self.part_threshhold:
                self.obs_cmd_util.download_or_create_file(bucket, key, total, filepath)
                pbar_add_size(total)
            else:
                cpfilepath = self._checkpoint_filepath(bucket, key, filepath, self.cmdtype)
                down_operation = DownloadOperation(bucket, key, filepath,
                                                   partsize, self.parttasknum, True, cpfilepath,
                                                   GetObjectHeader(), None, self.client)
                resp = down_operation.download(self.flowwidth)
                check_resp(resp)
        except Exception as e:
            self.session.logger.error(e)
            self.session.logger.error(safe_encode('%s download failed' % filepath))
            status = False

        if not self.recursive:
            try:
                end_progressbar(prc, alive, status)
                self._print_result(status, total, join_bucket_key(bucket, key), filepath)
                if self.md5 and status:
                    self._outprint('calculating md5...')
                    md5_ret = self.obs_cmd_util.check_etag_with_obs_local(bucket, key, filepath)
                    self._print_md5_result(md5_ret)
                    if 'failed' == md5_ret[0]:
                        os.remove(md5_ret[4])
                        self._outprint('delete file: %s...' % md5_ret[4])
            except Exception as e:
                self.session.logger.error(safe_encode('catched exception when check md5. %s' % e))
                self._outprint('delete file now: %s...' % filepath)
                os.remove(filepath)

        if self.recursive:
            if status:
                self.session.logger.info(safe_encode('%s download success' % filepath))
            with lock:
                result = (total, join_bucket_key(bucket, key), filepath)
                oklist.append(result) if status else failedlist.append(result)
        return 0

    def _download_dir(self, bucket, key):

        key = key.strip('/') + '/' if key else key
        obsfile_infos = self._list_filter_obsfiles(bucket, key, self.exclude, self.include)
        obskeys = [info[0] for info in obsfile_infos if not info[0].endswith('/')]

        self.make_local_dirs([info[0] for info in obsfile_infos if info[0].endswith('/')])

        if len(obskeys) == 0:
            self._outprint('No files to %s.' % self.cmdtype)
            return 0

        total = sum([info[-1] for info in obsfile_infos])

        alive = compat.Value('i', 0)
        lock = compat.Lock()
        oklist = compat.List()
        failedlist = compat.List()

        pbar = compat.Process(target=progressbar, args=(total, alive,  self.cmdtype))
        pbar.daemon = True
        pbar.start()

        multiprocess_with_sleep(self._download_file_for_process, (bucket, lock, oklist, failedlist),
                                obskeys, self.tasknum, self.flowwidth)
        alive.value = 1 if not failedlist else 2
        pbar.join()
        self._print_result_list(oklist, failedlist)

        if self.md5:
            try:
                self._outprint('calculating md5...')
                local_obs_paths = [(kv[2], get_object_key(kv[1])) for kv in oklist]
                md5_rets = self.obs_cmd_util.check_dir_etag_with_local_obs(bucket, key, local_obs_paths)
                self._print_dir_md5_result(md5_rets)
                for md5_ret in md5_rets:
                    if 'failed' == md5_ret[0]:
                        os.remove(md5_ret[4])
                        self._outprint('delete file: %s...' % md5_ret[4])
            except Exception as e:
                self.session.logger.error(e)
                self.session.logger.error(safe_encode('catched exception when checking md5. %s' % e))
                self._outprint('delete file now...')
                for kv in oklist:
                    self.session.logger.debug('delete file %s now...' % kv[2])
                    os.remove(kv[2])

    def _copy_file_for_process(self, args, item_args):
        srcpath, destpath = item_args
        lock, oklist, failedlist = args
        self._copy_file(srcpath, destpath, lock, oklist, failedlist)

    def _copy_file(self, srcpath, destpath, lock=None, oklist=None, failedlist=None):
        srcb, srck = split_bucket_key(srcpath)
        destb, destk = split_bucket_key(destpath)
        # copyObject destkey could not be empty
        destk = destk if destk else get_object_name(srcpath)

        status = True
        total = 0
        try:
            total = self.obs_cmd_util.get_object_size(srcb, srck)
        except:
            self.session.logger.error('obsfile or obsdir not found, if obs dir, use --recursive option')
            status = False
        partsize = self.partsize if total < self.part_threshhold else reset_partsize(total, self.partsize)

        if not self.recursive:
            prc, alive = start_progressbar(total, self.cmdtype)

        try:
            metadata = {'x-amz-meta-partsize': partsize}
            if total < self.part_threshhold:
                self.obs_cmd_util.copy_object(srcb, srck, destb, destk)
                pbar_add_size(total)
            else:
                cpfilepath = self._checkpoint_filepath(destb, destk, srcpath, self.cmdtype)
                # if not os.path.exists(cpfilepath):
                #     self.obs_cmd_util.remove_object_multipart(destb, destk)

                copy_operation = CopyOperation(destb, destk, srcpath, partsize, self.parttasknum,
                                                   True, cpfilepath, None, metadata, self.client)
                resp = copy_operation.copy(self.flowwidth)
                check_resp(resp)
        except Exception as e:
            self.session.logger.error(e)
            # self.session.logger.info(safe_encode('%s upload failed' % filepath))
            status = False

        if not self.recursive:
            end_progressbar(prc, alive, status)
            self._print_result(status, total, srcpath, join_bucket_key(destb, destk))
        if self.recursive:
            if status:
                pbar_add_size(total)
                self.session.logger.info(safe_encode('%s copy success' % srcpath))
            with lock:
                result = (total, srcpath, join_bucket_key(destb, destk))
                oklist.append(result) if status else failedlist.append(result)
        return 0

    def _copy_dir(self, srcdir, destdir):
        srcb, srck = split_bucket_key(srcdir)
        srck = srck if not srck or srck.endswith('/') else srck + '/'
        destb, destk = split_bucket_key(destdir)
        destk = destk if not destk or destk.endswith('/') else destk + '/'
        obsfile_infos = self._list_filter_obsfiles(srcb, srck, self.exclude, self.include)

        src_dest_obsfiles = []
        for info in obsfile_infos:
            key = info[0]
            tmp_destk = destk + key[len(srck):] if srck else destk + key
            src_dest_obsfiles.append((join_bucket_key(srcb, key), join_bucket_key(destb, tmp_destk)))

        if len(src_dest_obsfiles) == 0:
            self._outprint('No files to %s.' % self.cmdtype)
            return 0

        total = sum([info[-1] for info in obsfile_infos])

        alive = compat.Value('i', 0)
        lock = compat.Lock()
        oklist = compat.List()
        failedlist = compat.List()

        pbar = compat.Process(target=progressbar, args=(total, alive, self.cmdtype))
        pbar.daemon = True
        pbar.start()

        multiprocess_with_sleep(self._copy_file_for_process, (lock, oklist, failedlist),
                                src_dest_obsfiles, self.tasknum, self.flowwidth)

        alive.value = 1 if not failedlist else 2
        pbar.join()
        self._print_result_list(oklist, failedlist)
        return 0

    def _check_path_type(self, paths):
        """
        This initial check ensures that the path types for the specified
        command is correct.
        """
        cmd_dict = {
            'localobs': 'upload',
            'obsobs': 'copy',
            'obslocal': 'download',
            'obs': 'delete'
        }
        paths_type = ''
        for path in paths:
            if path.startswith('obs://'):
                paths_type = paths_type + 'obs'
            else:
                paths_type = paths_type + 'local'

        return cmd_dict[paths_type]

    def _filter_patterns(self, filepath, pattern_prefix, exclude_pattern, include_pattern):
        """
        filter file name pattern like *.jpg, [a-z].jpg, ?pg, doc*
        include pattern priority higher than exclude pattern
        
        :param filepath: local or obs file path
        :param pattern_prefix: pattern prefix
        :param exclude_pattern: exclude pattern
        :param include_pattern: include pattern
        :return: bool
        """
        isfilter = False
        if exclude_pattern:
            full_exclude_pattern = os.path.join(pattern_prefix, exclude_pattern)
            if fnmatch.fnmatch(filepath, full_exclude_pattern):
                isfilter = True
        if include_pattern:
            full_include_pattern = os.path.join(pattern_prefix, include_pattern)
            if fnmatch.fnmatch(filepath, full_include_pattern):
                isfilter = False
        return isfilter

    def _list_filter_obsfiles(self, bucket, key, exclude, include):
        obsfile_infos = self._list_obsfiles(bucket, key)
        return [info for info in obsfile_infos if not self._filter_patterns(info[0], key, exclude, include)]

    def _list_obsfiles(self, bucket, key):
        """
        list obsfiles 
        :param bucket: bucket name 
        :param key: prefix key
        :return: list((key, lastModified, size),)
        """
        return self.obs_cmd_util.get_objects_info(bucket, key, ('key', 'lastModified', 'size'))

    def localfiles_for_update(self, localfiles, obsfiles):
        """
        compare the last modified time of localfile and obsfile which has the same prefix key
        :param localfiles: filepath, key
        :param obsfiles: key, lastModifiedTime, size
        :return: list((filepath, key),)
        """
        upload_local_files = []
        obs_dict = {}
        for key, mtime, size in obsfiles:
            obs_dict[key.strip('/')] = mtime

        for localfile in localfiles:
            filepath, key = localfile
            fullkey = key + '/' + os.path.basename(filepath)
            fullkey = fullkey.strip('/')
            if fullkey in obs_dict.keys():
                localfile_timestamp = os.path.getmtime(filepath)
                obsfile_timestamp = time.mktime(time.strptime(obs_dict[fullkey], "%Y/%m/%d %H:%M:%S"))

                if localfile_timestamp > obsfile_timestamp:
                    upload_local_files.append(localfile)
            else:
                upload_local_files.append(localfile)
        return upload_local_files

    def _print_result_list(self, oklist, failedlist):
        self._outprint('')
        self._outprint('{cmdtype} complete {num} files:'.format(cmdtype=self.cmdtype, num=len(oklist)))
        for info in oklist:
            self._outprint('%8s\t%s --> %s' % (bytes_to_unitstr(info[0]), info[1], info[2]))

        self._outprint('')
        self._outprint('{cmdtype} failed {num} files:'.format(cmdtype=self.cmdtype, num=len(failedlist)))
        for info in failedlist:
            self._outprint('%8s\t%s --> %s' % (bytes_to_unitstr(info[0]), info[1], info[2]))

    def _print_result(self, status, total, srcpath, destpath):
        stat = 'complete' if status else 'failed'
        self._outprint("%s %s, %s %s --> %s\n" % (self.cmdtype, stat, bytes_to_unitstr(total), srcpath, destpath))

    def _print_md5_result(self, md5_ret):
        self._outprint('\nmd5 check (result, obs_md5, local_md5, obs_path, local_path):')
        self._outprint('%s\t%s\t%s %s  %s' % md5_ret)

    def _print_dir_md5_result(self, md5_rets):
        self._outprint('\nmd5 check (result, obs_md5, local_md5, obs_path, local_path):')
        for ret in md5_rets:
            self._outprint('%s\t%s\t%s %s  %s' % ret)

    def _checkpoint_filepath(self, bucket, key, localpath, optype):
        pathstr = self._filepath2str(localpath)
        if optype == 'upload':
            cpfilename = '%s.%s.%s' % (pathstr, optype, string2md5(localpath + bucket + key))
        else:
            cpfilename = '%s.%s.%s' % (pathstr, optype, string2md5(bucket + key + localpath))
        cpfilepath = os.path.join(CP_DIR, cpfilename)
        return cpfilepath

    def _filepath2str(self, filepath):

        reps = [':\\', '\\', '/', '//', ':']
        pathstr = filepath
        pathstr = pathstr.strip('/').strip('\\')
        for rep in reps:
            pathstr = pathstr.replace(rep, '_')
        return pathstr

    def _upload_part_first(self, localfiles):
        cpfilestrs = [file.split('.upload.')[0] for file in os.listdir(CP_DIR) if '.upload.' in file]

        noparts = []
        hasparts = []

        for item in localfiles:
            file, _ = item
            filestr = self._filepath2str(file)
            if filestr in cpfilestrs:
                hasparts.append(item)
            else:
                noparts.append(item)
        return hasparts+noparts

    def _outprint(self, msg):
        filename = self.now + '-' + self.session.full_cmd.replace('://', '/').replace('--', '').replace(' ', '_').replace('/', '.')
        filename = filename.replace('\\', '.').replace(':', '').replace('*', '').replace('"', '').replace("'", '')
        filename = filename if len(filename) < 255 else filename[:255]
        filepath = os.path.join(str(FILE_LIST_DIR), filename)
        super(CpCommand, self)._outprint(msg, filepath)

    def _check_max_partnum(self, total, partsize):
        partnum = int(total/partsize) if total % partsize == 0 else int(total/partsize) + 1
        if partnum > MAX_PART_NUM:
            raise MaxPartNumError(**{'filesize': bytes_to_unitstr(total), 'partsize': bytes_to_unitstr(partsize)})

    def make_upload_info(self, bucket, key, filepath):
        filename = os.path.basename(filepath)
        objkey = key.strip('/') + '/' + filename if key else filename
        total = os.path.getsize(filepath)
        cpfilepath = self._checkpoint_filepath(bucket, objkey, filepath, self.cmdtype)
        return objkey, total, cpfilepath

    def make_download_info(self, bucket, key):
        _, srckey = split_bucket_key(self.srcpath)
        destkey = os.path.basename(key) if srckey == key else key[len(srckey):].strip('/')
        filepath = get_full_path(os.path.join(self.destpath, destkey))

        total = self.obs_cmd_util.get_object_size(bucket, key)
        cpfilepath = self._checkpoint_filepath(bucket, key, filepath, self.cmdtype)

        return filepath, total, cpfilepath

    def make_obs_dirs(self, filedir, bucket, key):
        prefixes = []
        for root, dirs, files in os.walk(filedir):
            if not dirs:
                obsprefix = root[len(filedir):].strip('/').strip('\\')
                prefix = key + '/' + obsprefix if key and obsprefix else key + obsprefix
                prefix = prefix.replace('\\', '/') + '/'
                if prefix == '/':
                    continue
                self.obs_cmd_util.put_content(bucket, prefix)
                prefixes.append(prefix)
        return prefixes

    def make_local_dirs(self, dirkeys):
        for dirkey in dirkeys:
            filepath = self.make_download_filepath(dirkey)
            if not os.path.exists(filepath):
                os.makedirs(filepath)

    def make_download_filepath(self, key):
        _, srckey = split_bucket_key(self.srcpath)
        destkey = os.path.basename(key) if srckey == key else key[len(srckey):].strip('/')
        filepath = get_full_path(os.path.join(self.destpath, destkey))
        return filepath