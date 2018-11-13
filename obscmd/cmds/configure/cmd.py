#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import pkgutil
import os
import six
from obscmd.cmds.commands import BasicCommand
from obscmd.compat import compat_input


class InteractivePrompter(object):

    def get_value(self, current_value, config_name, prompt_text=''):
        if config_name in ('access_key_id', 'secret_access_key', 'server'):
            current_value = self._mask_value(current_value)
        response = compat_input("%s [%s]: " % (prompt_text, current_value))
        if not response:
            # If the user hits enter, we return a value of None
            # instead of an empty string.  That way we can determine
            # whether or not a value has changed.
            response = None
        return response

    def _mask_value(self, current_value):
        if current_value is None:
            return 'None'
        else:
            return ('*' * 16) + current_value[-4:]


class ConfigureCommand(BasicCommand):
    NAME = 'configure'
    DESCRIPTION = 'configure ak and sk'
    SYNOPSIS = "obscmd configure <Command> [<Arg> ...]"
    SUBCOMMANDS = []

    VALUES_TO_PROMPT = [
        ('access_key_id', "OBS Access Key ID"),
        ('secret_access_key', "OBS Secret Access Key"),
        ('server', "OBS Server"),

    ]

    def __init__(self, session):
        super(ConfigureCommand, self).__init__(session)
        self.SUBCOMMANDS = []
        self._prompter = InteractivePrompter()

    def _run_main(self, parsed_args, parsed_globals):
        new_values = {}
        for config_name, prompt_text in self.VALUES_TO_PROMPT:
            current_value = self.session.config.client.get(config_name)
            new_value = self._prompter.get_value(current_value, config_name,
                                                 prompt_text)
            if new_value is not None and new_value != current_value:
                new_values[config_name] = new_value
        config_filename = os.path.expanduser(
            self.session.config_file)
        if new_values:
            self._update_config_file(new_values, config_filename)

    def _update_config_file(self, new_values, profile_name):
        conf = six.moves.configparser.RawConfigParser()
        conf.read(profile_name)
        for key, value in new_values.items():
            conf.set("client", key, value)
        conf.write(open(profile_name, "w"))
