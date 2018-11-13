#!/usr/bin/env python
# -*- coding: UTF-8 -*-
from obscmd.cmds.obs.obsutil import split_bucket_key, check_resp
from obscmd.cmds.obs.subcmd import SubObsCommand


class MbpCommand(SubObsCommand):
    NAME = 'mbp'
    DESCRIPTION = "Create an obs bucket policy"
    USAGE = "mbp <ObsUri> policy"
    ARG_TABLE = [{'name': 'paths', 'default': 'obs://',
                  'positional_arg': True, 'synopsis': USAGE},
                 {'name': 'policy',
                  'positional_arg': True, 'synopsis': USAGE},

    ]
    EXAMPLES = """
        The  following  mbp command creates a bucket policy.  In this example, the user
       makes the bucket policy from the command line json string. 

          obscmd obs mbp obs://mybucket {"Version":"2008-10-17","Id":"Policy2","Statement":[{"Sid":"Stmt1375240018061",\
          "Effect":"Allow","Principal":{"AWS":["arn:aws:iam::userid:root"]},"Action":["s3:GetBucketPolicy"],\
          "Resource":["arn:aws:s3:::mybucket"]}]}

        
       Output:

          create policy complete: obs://mybucket

       The  following mb command creates a bucket policy from local file. 

          obscmd obs mbp obs://mybucket file://json/policy.json

        
       Output:

          create policy complete: obs://mybucket
          
    """

    def _run(self, parsed_args, parsed_globals):

        path = parsed_args.paths
        policy = parsed_args.policy
        bucket, key = split_bucket_key(path)

        resp = self.client.setBucketPolicy(bucket, policy)
        check_resp(resp)
        self._outprint("create policy complete: %s\n" % path)

