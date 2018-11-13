#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import obscmd.clidriver
import logging

LOG = logging.getLogger(__name__)


class Completer(object):

    def __init__(self, driver=None):
        if driver is not None:
            self.driver = driver
        else:
            self.driver = obscmd.clidriver.create_clidriver()
        self.command_table = self.driver.create_command_table()

    def complete(self, cmdline, point=None):
        if point is None:
            point = len(cmdline)

        args = cmdline[0:point].split()
        current_arg = args[-1]
        cmd_args = [w for w in args if not w.startswith('-')]
        opts = [w for w in args if w.startswith('-')]

        cmd_name, cmd = self._get_command(self.command_table, cmd_args)
        subcommand_table = cmd.subcommand_table if cmd else None

        subcmd_name, subcmd = self._get_command(subcommand_table, cmd_args)

        if cmd_name is None:
            # If we didn't find any command names in the cmdline
            # lets try to complete provider options
            return self._complete_provider(current_arg, opts)
        elif subcmd_name is None:
            return self._complete_command(cmd_name, subcommand_table, current_arg, opts)
        return self._complete_subcommand(subcmd_name, subcommand_table, current_arg, opts)

    def _complete_command(self, command_name, command_table, current_arg, opts):
        if current_arg == command_name:
            if command_table:
                return self._get_documented_completions(
                    command_table)
        elif current_arg.startswith('-'):
            try:
                arg_table = command_table[command_name].ARG_TABLE
            except:
                arg_table = []
            return self._find_possible_options(current_arg, opts, arg_table)
        elif command_table is not None:
            # See if they have entered a partial command name
            return self._get_documented_completions(
                command_table, current_arg)
        return []

    def _complete_subcommand(self, subcmd_name, subcmd_table, current_arg, opts):
        if current_arg != subcmd_name and current_arg.startswith('-'):
            try:
                arg_table = subcmd_table[subcmd_name].ARG_TABLE
            except:
                arg_table = []
            return self._find_possible_options(current_arg, opts, arg_table)
        return []

    def _complete_option(self, option_name):
        if option_name == '--endpoint-url':
            return []
        if option_name == '--output':
            cli_data = self.driver.session.get_data('cli')
            return cli_data['options']['output']['choices']
        if option_name == '--profile':
            return self.driver.session.available_profiles
        return []

    def _complete_provider(self, current_arg, opts):
        if current_arg.startswith('-'):
            return self._find_possible_options(current_arg, opts)
        elif current_arg == 'obscmd':
            return self._get_documented_completions(
                self.command_table)
        else:
            # Otherwise, see if they have entered a partial command name
            return self._get_documented_completions(
                self.command_table, current_arg)

    def _get_command(self, command_table, command_args):
        if command_table is not None:
            for command_name in command_args:
                if command_name in command_table:
                    cmd_obj = command_table[command_name]
                    return command_name, cmd_obj
        return None, None

    def _get_documented_completions(self, table, startswith=None):
        names = []
        for key, command in table.items():
            if getattr(command, '_UNDOCUMENTED', False):
                # Don't tab complete undocumented commands/params
                continue
            if startswith is not None and not key.startswith(startswith):
                continue
            if getattr(command, 'positional_arg', False):
                continue
            names.append(key)
        return names

    def _find_possible_options(self, current_arg, opts, arg_table):

        all_options = [arg['name'] for arg in arg_table if 'positional_arg' not in arg]

        for option in opts:
            # Look through list of options on cmdline. If there are
            # options that have already been specified and they are
            # not the current word, remove them from list of possibles.
            if option != current_arg:
                stripped_opt = option.lstrip('-')
                if stripped_opt in all_options:
                    all_options.remove(stripped_opt)
        cw = current_arg.lstrip('-')
        possibilities = ['--' + n for n in all_options if n.startswith(cw)]
        if len(possibilities) == 1 and possibilities[0] == current_arg:
            return self._complete_option(possibilities[0])
        return possibilities


def complete(cmdline, point):
    choices = Completer().complete(cmdline, point)
    print(' \n'.join(choices))

