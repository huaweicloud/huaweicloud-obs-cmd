#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import pkgutil
import os

import obscmd
from obscmd.cmds.commands import BasicCommand

class ObsCommand(BasicCommand):
    NAME = 'obs'
    DESCRIPTION = 'Object Storage Service '
    SYNOPSIS = "obscmd obs <Command> [<Arg> ...]"
    SUBCOMMANDS = []
    EXAMPLES = """
        obs has following subcommands:
        \t\t\t
    """

    def __init__(self, session):
        super(ObsCommand, self).__init__(session)
        self.SUBCOMMANDS = self._load_subcmd()
        self.EXAMPLES += '\n\t\t\t'.join([cmd['name'] for cmd in self.SUBCOMMANDS])

    def _run_main(self, parsed_args, parsed_globals):
        if parsed_args.subcommand is None:
            raise ValueError("usage: obscmd [options] <command> <subcommand> "
                             "[parameters]\nobscmd: error: too few arguments")

    def _load_subcmd(self):
        """
        read subcommand from subcmds directory
        :return: subcommands list
        """
        subcommands = []
        pkgname = 'obscmd.cmds.obs.subcmds'
        pkgpath = os.path.join(os.path.dirname(__file__), 'subcmds')

        for _, file, _ in pkgutil.iter_modules([pkgpath]):
            __import__(pkgname + '.' + file)
            tmp_name = pkgname + '.' + file + '.' + file.capitalize() + 'Command'
            cmd_class = eval(tmp_name)
            subcommands.append({'name': file, 'command_class': cmd_class})
        return subcommands
