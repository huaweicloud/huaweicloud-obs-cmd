#!/usr/bin/env python
# -*- coding: UTF-8 -*-

#!/usr/bin/env python
# -*- coding: UTF-8 -*-
from obs import CreateBucketHeader

from obscmd.cmds.obs.obsutil import split_bucket_key, check_resp, join_bucket_key, ObsCmdUtil
from obscmd.cmds.obs.subcmd import SubObsCommand
from obscmd.constant import ACL_CONTROL, STORAGE_CLASS
from obscmd.exceptions import InternalError


class MbCommand(SubObsCommand):
    NAME = 'mb'
    DESCRIPTION = "Create an obs bucket."
    USAGE = "mb <ObsUri> [--acl-control] [--location] [--storage]"
    ARG_TABLE = [{'name': 'paths', 'positional_arg': True, 'synopsis': USAGE},
                 {'name': 'acl-control', 'choices': ACL_CONTROL,
                  'help_text': "set bucket acl control"},
                 {'name': 'location',
                  'help_text': "set bucket location, see more https://developer.huaweicloud.com/endpoint "},
                 {'name': 'storage', 'choices': STORAGE_CLASS,
                  'help_text': "set bucket storage type"},
    ]
    EXAMPLES = """  
        The  following  mb command creates a bucket.  In this example, the user
       makes the bucket mybucket.  The bucket is created in the region  speci-
       fied in the user's configuration file:

          obscmd obs mb obs://mybucket

       Output:

          make bucket complete: obs://mybucket

       The  following mb command creates a bucket in a region specified by the
       --region parameter.   In  this  example,  the  user  makes  the  bucket
       mybucket in the region us-west-1:

          obscmd obs mb obs://mybucket --location cn-east-2 --acl-control public-read

       Output:

          make bucket complete: obs://mybucket
          
        The  following mb command creates a bucket already exists in obs.

          obscmd obs mb obs://mybucket 

       Output:

          bucket already exists: obs://mybucket 
            
    """

    def _run(self, parsed_args, parsed_globals):

        path = parsed_args.paths
        acl_control = parsed_args.acl_control
        storage = parsed_args.storage
        location = parsed_args.location

        bucket, key = split_bucket_key(path)

        obs_cmd_util = ObsCmdUtil(self.client)

        if obs_cmd_util.head_bucket(bucket):
            self._outprint("bucket already exists: %s\n" % path)
            return 0
        obs_cmd_util.create_bucket(bucket, acl_control, storage, location)
        self._outprint("make bucket complete: %s\n" % path)
        return 0

