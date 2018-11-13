#!/usr/bin/env python
# -*- coding: UTF-8 -*-
from obs import DeleteObjectsRequest, Object, ListMultipartUploadsRequest

from obscmd.cmds.obs.obsutil import split_bucket_key, check_resp, join_bucket_key, ObsCmdUtil
from obscmd.cmds.obs.subcmd import SubObsCommand


class RbCommand(SubObsCommand):
    NAME = 'rb'
    DESCRIPTION = """
        Deletes  an  empty  Obs  bucket.  A  bucket  must be completely empty of
        objects and versioned objects before it can be  deleted.  However,  the
        --force  parameter  can  be used to delete the non-versioned objects in
        the bucket before the bucket is deleted
    """
    USAGE = "rb <ObsUri> [--force]"
    ARG_TABLE = [
        {'name': 'paths', 'positional_arg': True, 'synopsis': USAGE},
        {'name': 'force', 'action': 'store_true',
         'help_text': (
             "force to remove all objects and the bucket")},
    ]
    EXAMPLES = """
        The following rb command removes a bucket.  In this example, the user's
       bucket  is  mybucket.   Note  that the bucket must be empty in order to
       remove:

          obscmd obs rb obs://mybucket

       Output:

          remove bucket complete: mybucket

       The following rb command uses the --force parameter to first remove all
       of  the  objects  in  the bucket and then remove the bucket itself.  In
       this example, the user's bucket is mybucket and the objects in mybucket
       are test1.txt and test2.txt:

          obscmd obs rb obs://mybucket --force

       Output:

          delete: obs://mybucket/test1.txt
          delete: obs://mybucket/test2.txt
          remove bucket complete: obs://mybucket
    """

    def _run(self, parsed_args, parsed_globals):

        path = parsed_args.paths
        force = parsed_args.force

        bucket, key = split_bucket_key(path)

        self._warning_prompt('Are you sure to delete bucket %s?' % bucket)

        obs_cmd_util = ObsCmdUtil(self.client)
        if force:
            keys = obs_cmd_util.force_remove_bucket(bucket)
            self.print_results(bucket, keys)
        else:
            obs_cmd_util.remove_bucket(bucket)
        self._outprint('remove bucket complete: %s' % path)
        return 0

    def print_results(self, bucket, keys):
        if len(keys) == 0:
            return 0
        for key in keys:
            self._outprint('delete: %s' % (join_bucket_key(bucket, key)))
        self._outprint('\n')
        self._outprint('delete %d objects' % len(keys))