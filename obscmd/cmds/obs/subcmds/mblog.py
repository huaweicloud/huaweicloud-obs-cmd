#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import json

from obscmd.cmds.obs.obsutil import split_bucket_key, check_resp
from obscmd.cmds.obs.subcmd import SubObsCommand
from obscmd.exceptions import LogJsonAnalyzeError
from obs import Grant
from obs import Grantee
from obs import Group
from obs import HeadPermission
from obs import Logging
from obs import Permission


class MblogCommand(SubObsCommand):
    NAME = 'mblog'
    DESCRIPTION = "set obs bucket log."
    USAGE = "mblog <ObsUri> --log"
    ARG_TABLE = [{'name': 'paths', 'positional_arg': True, 'synopsis': USAGE},
                 {'name': 'log',
                  'help_text': "set bucket log"},
                 ]

    EXAMPLES = """  
            The following command set a bucket log:

              obscmd obs mblog obs://mybucket --log file:///Users/tom/log.json

            Output:

              set bucket log complete.
    """

    def _run(self, parsed_args, parsed_globals):
        path = parsed_args.paths
        log_str = parsed_args.log

        bucket, key = split_bucket_key(path)
        logstatus = None
        if log_str is not None:
            logstatus = self._build_log(log_str=log_str)
        resp = self.client.setBucketLogging(bucket, logstatus=logstatus)
        check_resp(resp)
        self._outprint("set bucket log complete.")

    def _build_log(self, log_str):
        try:
            log_obj = json.loads(log_str)
            target_bucket = log_obj['targetBucket']
            target_prefix = log_obj['targetPrefix']
            # first we set write and read acp permission to the log_delivery group on target bucket
            self.client.setBucketAcl(bucketName=target_bucket, aclControl=HeadPermission.LOG_DELIVERY_WRITE)
            # the we set read permission to all users
            grantee = Grantee(group=Group.ALL_USERS)
            grant = Grant(grantee=grantee, permission=Permission.READ)
            grant_list = [grant]
            logstatus = Logging(targetBucket=target_bucket, targetPrefix=target_prefix, targetGrants=grant_list)
            return logstatus
        except Exception:
            raise LogJsonAnalyzeError(**{'log_str': log_str})
