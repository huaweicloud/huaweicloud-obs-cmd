#!/usr/bin/env python
# -*- coding: UTF-8 -*-
from obscmd.cmds.configure.cmd import ConfigureCommand
from obscmd.cmds.obs.cmd import ObsCommand
from obscmd.cmds.commands import HelpCommand

cmd_table = {
    'obs': ObsCommand,
    'configure': ConfigureCommand,
    'help': HelpCommand,
}
