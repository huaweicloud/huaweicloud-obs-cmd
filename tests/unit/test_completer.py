#!/usr/bin/env python
# -*- coding: UTF-8 -*-
from obscmd.completer import complete, Completer
from obscmd.testutils import mock, unittest


class TestCompleter(unittest.TestCase):
    def test_completer_hcmd(self):
        cmdline = 'obscmd'
        point = None
        choices = Completer().complete(cmdline, point)
        self.assertIn('obs', choices)
        self.assertIn('help', choices)

    def test_completer_hcmd_o(self):
        cmdline = 'obscmd o'
        point = None
        choices = Completer().complete(cmdline, point)
        self.assertIn('obs', choices)
        self.assertNotIn('help', choices)

    def test_completer_hcmd_obs(self):
        cmdline = 'obscmd obs'
        point = None
        choices = Completer().complete(cmdline, point)
        self.assertIn('cp', choices)
        self.assertIn('ls', choices)

    def test_completer_hcmd_obs_subcmds(self):
        cmdline = 'obscmd obs c'
        point = None
        choices = Completer().complete(cmdline, point)
        self.assertIn('cp', choices)
        self.assertNotIn('ls', choices)

    def test_completer_hcmd_obs_subcmds_option(self):
        cmdline = 'obscmd obs ls --'
        point = None
        choices = Completer().complete(cmdline, point)
        self.assertIn('--recursive', choices)
        self.assertIn('--limit', choices)

        cmdline = 'obscmd obs ls --re'
        point = None
        choices = Completer().complete(cmdline, point)
        self.assertIn('--recursive', choices)
        self.assertNotIn('--limit', choices)

        cmdline = 'obscmd obs ls --limit --'
        point = None
        choices = Completer().complete(cmdline, point)
        self.assertIn('--recursive', choices)
        self.assertNotIn('--limit', choices)

    def test_completer_hcmd_obs_subcmds_no_aksk(self):
        cmdline = 'obscmd obs ls --'
        point = None
        choices = Completer().complete(cmdline, point)
        self.assertNotIn('--ak', choices)
        self.assertNotIn('--sk', choices)

