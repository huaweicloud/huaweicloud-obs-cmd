#!/usr/bin/env python
# -*- coding: UTF-8 -*-
from obscmd.cmds.obs.subcmd import SubObsCommand
from obscmd.testutils import unittest, BaseHcmdCommandParamsTest
import mock


class TestSubObsCommand(unittest.TestCase):

    def setUp(self):
        self.session = mock.Mock()
        self.command = SubObsCommand(self.session)

    def test_load_arg_table_property(self):
        # Ensure ``_build_arg_table()`` is called if it has not been
        # built via the ``arg_table`` property.  It should be an empty
        # dictionary.
        orig_arg_table = self.command.arg_table
        self.assertEqual(orig_arg_table, {})
        # Ensure the ``arg_table`` is not built again if
        # ``arg_table`` property is called again.
        self.assertIs(orig_arg_table, self.command.arg_table)

    def test_load_subcommand_table_property(self):
        # Ensure ``_build_subcommand_table()`` is called if it has not
        # been built via the ``subcommand_table`` property. It should be
        # an empty dictionary.
        orig_subcommand_table = self.command.subcommand_table
        self.assertEqual(orig_subcommand_table, {})
        # Ensure the ``subcommand_table`` is not built again if
        # ``subcommand_table`` property is called again.
        self.assertIs(orig_subcommand_table, self.command.subcommand_table)


class TestOBS(BaseHcmdCommandParamsTest):
    def test_too_few_args(self):
        stderr = self.run_cmd('obs', expected_rc=255)[1]


if __name__ == "__main__":
    unittest.main()