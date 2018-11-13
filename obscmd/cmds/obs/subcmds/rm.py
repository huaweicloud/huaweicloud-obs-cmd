#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import os

import time

from obscmd.compat import safe_encode
from obscmd.config import FILE_LIST_DIR
from obs import Versions

from obscmd.cmds.obs.obsutil import split_bucket_key, check_resp, ObsCmdUtil, join_bucket_key
from obscmd.cmds.obs.subcmd import SubObsCommand

class RmCommand(SubObsCommand):
    NAME = 'rm'
    DESCRIPTION = "Remove an obs object."
    USAGE = "rm <ObsUri> [--version] [--recursive]"
    ARG_TABLE = [{'name': 'paths', 'positional_arg': True, 'synopsis': USAGE},
                 {'name': 'recursive', 'action': 'store_true',
                  'help_text': (
                      "Command is performed on all files or objects "
                      "under the specified directory")},
                 {'name': 'versionid',
                  'help_text': "The object version, when recursive option exists, the version is invalid."},
        ]
    EXAMPLES = """
         The following rm command delete a single object. 
          obscmd obs rm obs://mybucket/test.txt

       Output:

          delete object: obs://mybucket/test.txt
        
        The following rm command delete a single object with special version. 
          obscmd obs rm obs://mybucket/test.txt --version 2322

       Output:

          delete object: obs://mybucket/test.txt
          version: 2332

       The following rm command delete a directory recursively. 
          obscmd obs rm obs://mybucket/doc --recursive

       Output:

            delete: obs://tsdd/doc/test.cp
            delete: obs://tsdd/doc/test2.cp
            
            delete 2 objects
       
    """

    def _run(self, parsed_args, parsed_globals):

        path = parsed_args.paths
        recursive = parsed_args.recursive
        versionid = parsed_args.versionid
        bucket, key = split_bucket_key(path)

        self._warning_prompt('Are you sure to delete bucket objects?')

        self.now = time.strftime('%Y.%m.%d_%H.%M.%S', time.localtime(time.time()))

        self.obs_cmd_util = ObsCmdUtil(self.client)
        self._outprint('start delete objects ...')

        if recursive:
            self.delete_dir(bucket, key)
        else:
            self.delete_object(bucket, key, versionid)

    def delete_dir(self, bucket, key):
        key = key if key.endswith('/') or not key else key + '/'
        keys = self.obs_cmd_util.get_objects_info(bucket, key, 'key')
        self.obs_cmd_util.delete_all_objects(bucket, keys)
        self.print_results(bucket, keys)

    def delete_object(self, bucket, key, version):
        self.obs_cmd_util.delete_object(bucket, key, version)
        self._outprint("delete object: %s" % join_bucket_key(bucket, key))

        if version:
            self._outprint("version: %s" % version)

    def print_results(self, bucket, keys):
        if len(keys) == 0:
            return 0
        for key in keys:
            self._outprint('delete: %s' % (join_bucket_key(bucket, key)))
        self._outprint('\n')
        self._outprint('delete %d objects' % len(keys))

    def _outprint(self, msg):
        filename = self.now + '-' + self.session.full_cmd.replace('://', '/').replace('--', '').replace(' ', '-').replace('/', '.')
        filename = filename.replace('\\', '.').replace(':', '')
        filename = filename if len(filename) < 255 else filename[:255]
        filepath = os.path.join(str(FILE_LIST_DIR), filename)
        super(RmCommand, self)._outprint(msg, filepath)