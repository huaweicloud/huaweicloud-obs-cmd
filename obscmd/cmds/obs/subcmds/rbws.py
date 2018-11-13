#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from obscmd.cmds.obs.obsutil import split_bucket_key, check_resp
from obscmd.cmds.obs.subcmd import SubObsCommand


class RbwsCommand(SubObsCommand):
    NAME = 'rbws'
    DESCRIPTION = "delete obs bucket website."
    USAGE = "rbws <ObsUri>"
    ARG_TABLE = [{'name': 'paths', 'positional_arg': True, 'synopsis': USAGE}]

    EXAMPLES = """  
            The following command deletes all the website of a bucket:

              obscmd obs rbws obs://mybucket

           Output:

              delete bucket website complete.
    """

    def _run(self, parsed_args, parsed_globals):
        path = parsed_args.paths

        bucket, key = split_bucket_key(path)
        self._warning_prompt('Are you sure to delete bucket website?')

        resp = self.client.deleteBucketWebsite(bucket)
        check_resp(resp)
        self._outprint("delete bucket website complete.")
