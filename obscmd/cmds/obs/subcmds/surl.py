#!/usr/bin/env python
# -*- coding: UTF-8 -*-

#!/usr/bin/env python
# -*- coding: UTF-8 -*-
from obscmd.cmds.obs.obsutil import get_bucket,check_resp
from obscmd.cmds.obs.obsutil import get_object_key,str_to_dict
from obscmd.cmds.obs.subcmd import SubObsCommand


class SurlCommand(SubObsCommand):
    NAME = 'surl'
    DESCRIPTION = "Creates an temporary authentication for the bucket and object "
    USAGE = "surl < http method> [--obspath] [--special-param] [--expires] [--headers] [--query-param] "

    ARG_TABLE = [
        {'name': 'method', 'positional_arg': True,
         'choices': ['GET', 'POST', 'PUT', 'DELETE', 'HEAD'],
         'synopsis': 'Http method type'},
        {'name': 'obspath', 'help_text': "obs path"},
        {'name': 'special-param',
         'choices': ['versions', 'uploads', 'location', 'storageinfo', 'quota', 'storagePolicy', 'acl', 'logging',
              'policy', 'lifecycle', 'website', 'versioning', 'cors', 'notification', 'tagging', 'delete', 'restore'],
         'help_text': "special parameter"},
        {'name': 'expires', 'cli_type_name': 'integer', 'help_text': "expires time"},
        {'name':'headers','help_text': "headers"},
        {'name':'query-param','help_text': "query params"}
    ]
    
    def _run(self, parsed_args, parsed_globals):

        method = parsed_args.method
        path = parsed_args.obspath
        specialParam = parsed_args.special_param
        expires = parsed_args.expires if parsed_args.expires else 300

        bucket = get_bucket(path) if path else None
        
        objectKey=get_object_key(path) if path else None

        headers=str_to_dict(parsed_args.headers) if parsed_args.headers else None
        
        queryParam=str_to_dict(parsed_args.query_param) if parsed_args.query_param else None

        resp = self.client.createSignedUrl(method, bucket, objectKey, specialParam, expires,headers,queryParam )
        # this function return value has no status
        self._outprint("signed url: %s\n" % resp.signedUrl)
        self._outprint("request header: %s\n" % resp.actualSignedRequestHeaders)



