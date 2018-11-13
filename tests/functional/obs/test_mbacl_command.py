#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#!/usr/bin/env python
# -*- coding: UTF-8 -*-
from obscmd.cmds.obs.obsutil import join_bucket_key
from obscmd.testutils import FileCreator
from tests import BaseObsCommandTest


class TestMbaclCommand(BaseObsCommandTest):

    cmd = 'obs mbacl '

    def test_help(self):
        cmdline = '%s help' % self.cmd
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]
        self.assertIn('mbacl', stdout)
        self.assertIn('DESCRIPTION', stdout)

    def test_make_acl_from_acl_control(self):

        cmdline = '%s %s --acl-control public-read-write' % (self.cmd, join_bucket_key(self.bucket))
        stdout = self.run_cmd(cmdline, expected_rc=0)[0]

        self.assertIn('complete', stdout)
