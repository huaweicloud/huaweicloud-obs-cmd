#!/usr/bin/env python
# -*- coding: UTF-8 -*-
from obscmd.cmds.obs.obsutil import split_bucket_key, check_resp
from obscmd.cmds.obs.subcmd import SubObsCommand


class RbpCommand(SubObsCommand):
    NAME = 'rbp'
    DESCRIPTION = "delete an obs bucket policy"
    USAGE = "rbp <Uri> or NONE"
    ARG_TABLE = [{'name': 'paths', 'nargs': '?', 'default': 'obs://',
                  'positional_arg': True, 'synopsis': USAGE},
    ]
    EXAMPLES = """
        The  following mb command delete the bucket policy. 

          obscmd obs mbp obs://mybucket

        
       Output:

          delete policy complete.
    """

    def _run(self, parsed_args, parsed_globals):

        path = parsed_args.paths

        bucket, key = split_bucket_key(path)
        self._warning_prompt('Are you sure to delete bucket policy?')

        resp = self.client.deleteBucketPolicy(bucket)
        check_resp(resp)
        self._outprint("delete policy complete.")

