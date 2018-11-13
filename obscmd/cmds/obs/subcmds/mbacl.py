#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import json

from obscmd.cmds.obs.obsutil import split_bucket_key, check_resp
from obscmd.cmds.obs.subcmd import SubObsCommand
from obscmd.constant import ACL_CONTROL
from obscmd.exceptions import ACLJsonAnalyzeError, UnknownParameterError
from obs import ACL, HeadPermission
from obs import Owner
from obs import Grant, Permission
from obs import Grantee, Group



class MbaclCommand(SubObsCommand):
    NAME = 'mbacl'
    DESCRIPTION = "make an obs bucket ACL."
    USAGE = "mbacl <ObsUri> --acl-control"
    ARG_TABLE = [{'name': 'paths', 'positional_arg': True, 'synopsis': USAGE},
                 {'name': 'acl-control', 'choices': ACL_CONTROL,
                  'help_text': "set bucket acl control"},
                 {'name': 'acl',
                  'help_text': "set bucket acl"},
                 ]

    EXAMPLES = """  
            The  following  command creates a bucket acl from acl-control:

              obscmd obs mbacl obs://mybucket --acl-control public-read

           Output:

              create acl complete.
              
            The following command creates a bucket acl from acl string. 
            The acl string come from a file or command line.
            The acl json string schema must like following:
            {
                "owner": {
                    "owner_id": "8be8301bdfc7469aaf721d0504826420",
                    "owner_name": "name"
                },
                "grants": [
                    {
                        "grantee": {"grantee_id": "8be8301bdfc7469aa21d0504826420", "grantee_name":"other"},
                        "permission": "WRITE"
                    },
                    {
                        "grantee": {"group": "http://acs.amazonaws.com/groups/s3/LogDelivery"},
                        "permission": "WRITE_ACP"
                    }
                ]
            }
            
            obscmd obs mbacl obs://mybucket --acl file://acl.json

           Output:

              create acl complete.
              
            
           If acl-control and acl option exist both, acl is invalid.
    """

    def _run(self, parsed_args, parsed_globals):
        path = parsed_args.paths
        acl_control = parsed_args.acl_control
        acl_str = parsed_args.acl

        bucket, key = split_bucket_key(path)
        acl = None
        if not acl_control and acl_str:
            acl = self._build_acl(acl_str)
        resp = self.client.setBucketAcl(bucket, acl=acl, aclControl=acl_control)
        # resp = self.client.setBucketAcl(bucket, acl=acl, aclControl=HeadPermission.PUBLIC_READ_WRITE_DELIVERED)
        check_resp(resp)
        self._outprint("create acl complete.")

    def _build_acl(self, acl_str):
        try:
            acl_obj = json.loads(acl_str)
            ower = Owner(acl_obj['owner']['owner_id'], acl_obj['owner']['owner_name'])
            acl_grants = []
            for grant in acl_obj['grants']:
                grantee = Grantee(**grant['grantee'])
                acl_grant = Grant(grantee=grantee, permission=grant['permission'])
                acl_grants.append(acl_grant)
            acl = ACL(owner=ower, grants=acl_grants)
            return acl
        except Exception:
            raise ACLJsonAnalyzeError(**{'acl_str': acl_str})



