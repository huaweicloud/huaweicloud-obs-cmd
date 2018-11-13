#!/usr/bin/env python
# -*- coding: UTF-8 -*-
from obscmd.testutils import unittest, BaseHcmdCommandParamsTest


class TestObsCommand(BaseHcmdCommandParamsTest):
    def test_too_few_args(self):
        stderr = self.run_cmd('obs', expected_rc=255)[1]
