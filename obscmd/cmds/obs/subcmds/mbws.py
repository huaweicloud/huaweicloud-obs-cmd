#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import json
import re

from obscmd.cmds.obs.obsutil import split_bucket_key, check_resp
from obscmd.cmds.obs.subcmd import SubObsCommand
from obscmd.exceptions import WebsiteJsonAnalyzeError

from obs import WebsiteConfiguration
from obs import IndexDocument
from obs import ErrorDocument
from obs import RoutingRule
from obs import Condition
from obs import Redirect


class MbwsCommand(SubObsCommand):
    NAME = 'mbws'
    DESCRIPTION = "set obs bucket website."
    USAGE = "mbws <ObsUri> website"
    ARG_TABLE = [{'name': 'paths', 'positional_arg': True, 'synopsis': USAGE},
                 {'name': 'website', 'positional_arg': True, 'synopsis': USAGE,
                  'help_text': "set bucket website"},
                 ]

    EXAMPLES = """  
            The following command creates a bucket website:

              obscmd obs mbws obs://mybucket file:///Users/tom/website.json

           Output:

              set bucket website complete.
    """

    def _run(self, parsed_args, parsed_globals):
        path = parsed_args.paths
        website_str = parsed_args.website
        website_obj = json.loads(website_str)
        bucket, key = split_bucket_key(path)
        index_suff = website_obj['indexDocument']['suffix']
        error_page = None
        if website_obj.get('errorDocument') is not None:
            error_page = website_obj['errorDocument']['key']
        if self._check_index_page(index_suff):
            if self._check_error_page(error_page):
                resp = self.client.setBucketWebsite(bucket, website=website_obj)
                check_resp(resp)
                self._outprint("create bucket website complete.")
            else:
                self._outprint("input error, errorDocument[key] must be html/jpg/png/bmp/webp file in the root directory, and filename can be only consist of English letter, number and '-'.")
        else:
            self._outprint("input error, indexDocument[suffix] must be html file in the root directory, and filename can be only consist of English letter, number and '-'.")

    def _check_index_page(self, pagename):
        if pagename is not None and pagename.endswith('.html'):
            check_str = pagename[0:pagename.rfind('.')]
            if re.search(r'[^\w\-]', check_str, re.I | re.U | re.X) is None:
                return True
        return False

    def _check_error_page(self, pagename):
        limit_suffix = ['html', 'jpg', 'png', 'bmp', 'webp']
        if pagename is not None:
            page_suffix = pagename[pagename.rfind('.') + 1:len(pagename)]
            if page_suffix in limit_suffix:
                check_str = pagename[0:pagename.rfind('.')]
                if re.search(r'[^\w\-]', check_str, re.I | re.U | re.X) is None:
                    return True
        else:
            return True
        return False

