#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
import shlex
import copy
import sys

import six
# from compat import six
import obscmd.exceptions
import configparser
from obscmd.utils import DotDict


def raw_config_parse(config_filename, parse_subsections=True):
    """Returns the parsed INI config contents.

    Each section name is a top level key.

    :param config_filename: The name of the INI file to parse

    :param parse_subsections: If True, parse indented blocks as
       subsections that represent their own configuration dictionary.
       For example, if the config file had the contents::

           obs =
              signature_version = obsv4
              addressing_style = path

        The resulting ``raw_config_parse`` would be::

            {'obs': {'signature_version': 'obsv4', 'addressing_style': 'path'}}

       If False, do not try to parse subsections and return the indented
       block as its literal value::

            {'obs': '\nsignature_version = obsv4\naddressing_style = path'}

    :returns: A dict with keys for each profile found in the config
        file and the value of each key being a dict containing name
        value pairs found in that profile.

    :raises: ConfigNotFound, ConfigParseError
    """

    config = DotDict()
    path = config_filename
    if path is not None:
        path = os.path.expandvars(path)
        path = os.path.expanduser(path)
        if not os.path.isfile(path):
            raise obscmd.exceptions.ConfigNotFound(path=_unicode_path(path))
        # cp = six.moves.configparser.RawConfigParser()
        cp = configparser.RawConfigParser()
        try:
            cp.read([path])
        except six.moves.configparser.Error:
            raise obscmd.exceptions.ConfigParseError(
                path=_unicode_path(path))
        else:
            for section in cp.sections():
                config[section] = DotDict()
                for option in cp.options(section):
                    config_value = cp.get(section, option)
                    if parse_subsections and config_value.startswith('\n'):
                        # Then we need to parse the inner contents as
                        # hierarchical.  We support a single level
                        # of nesting for now.
                        try:
                            config_value = _parse_nested(config_value)
                        except ValueError:
                            raise obscmd.exceptions.ConfigParseError(
                                path=_unicode_path(path))
                    config[section][option] = config_value
    return config


def _unicode_path(path):
    if isinstance(path, six.text_type):
        return path
    return path.decode(sys.getfilesystemencoding(), 'replace')


def _parse_nested(config_value):
    # Given a value like this:
    # \n
    # foo = bar
    # bar = baz
    # We need to parse this into
    # {'foo': 'bar', 'bar': 'baz}
    parsed = {}
    for line in config_value.splitlines():
        line = line.strip()
        if not line:
            continue
        # The caller will catch ValueError
        # and raise an appropriate error
        # if this fails.
        key, value = line.split('=', 1)
        parsed[key.strip()] = value.strip()
    return parsed


