#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from obscmd.cmds.commands import BasicCommand

from obscmd.cmds.obs.obsutil import create_client
from obscmd.compat import compat_input



class SubObsCommand(BasicCommand):

    client = None

    def _create_client(self, ak=None, sk=None, server=None):
        self.client = create_client(ak, sk, server)

    def _run_main(self, parsed_args, parsed_globals):
        ak = parsed_args.ak if parsed_args.ak else self.session.config.client.access_key_id
        sk = parsed_args.sk if parsed_args.sk else self.session.config.client.secret_access_key
        server = parsed_args.server if parsed_args.server else self.session.config.client.server
        if hasattr(parsed_args, 'help'):
            self._display_help(parsed_args, parsed_globals)
        else:
            self._create_client(ak, sk, server)
            self._run(parsed_args, parsed_globals)
            self._close_client()
        return 0

    def _run(self, parsed_args, parsed_globals):
        # Subclasses should implement this method.
        # parsed_globals are the parsed global args (things like region,
        # profile, output, etc.)
        # parsed_args are any arguments you've defined in your ARG_TABLE
        # that are parsed.  These will come through as whatever you've
        # provided as the 'dest' key.  Otherwise they default to the
        # 'name' key.  For example: ARG_TABLE[0] = {"name": "foo-arg", ...}
        # can be accessed by ``parsed_args.foo_arg``.
        raise NotImplementedError("_run")

    def _close_client(self):
        if self.client is not None:
            self.client.close()

    def _warning_prompt(self, prompt_text):
        response = compat_input("%s(yes/no): " % (prompt_text,))
        while response not in ['yes', 'no']:
            response = compat_input("%s(yes/no): " % (prompt_text,))
        if response == 'no':
            exit(0)



