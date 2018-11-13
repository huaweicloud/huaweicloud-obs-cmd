#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import json

from obscmd.cmds.obs.obsutil import split_bucket_key, check_resp
from obscmd.cmds.obs.subcmd import SubObsCommand


class MblcCommand(SubObsCommand):
    NAME = 'mblc'
    DESCRIPTION = "set obs bucket lifecycle."
    USAGE = "mblc <ObsUri> --lifecycle"
    ARG_TABLE = [{'name': 'paths', 'positional_arg': True, 'synopsis': USAGE},
                 {'name': 'lifecycle', 'positional_arg': True, 'synopsis': USAGE,
                  'help_text': "set bucket lifecycle"},
                 ]

    EXAMPLES = """  
            The following command creates a bucket lifecycle:

              obscmd obs mblc obs://mybucket file:///Users/tom/lifecycle.json

           Output:

              set bucket lifecycle complete.
    """

    def _run(self, parsed_args, parsed_globals):
        path = parsed_args.paths
        rule_str = parsed_args.lifecycle

        bucket, key = split_bucket_key(path)
        rule = self._check_lifecycle(bucket, rule_str)
        lifecycle = json.loads(rule)
        resp = self.client.setBucketLifecycle(bucket, lifecycle)
        check_resp(resp)
        self._outprint("create bucket lifecycle complete.")

    def _check_lifecycle(self, bucket, rule_str):
        json_str = '{"rule":'
        rule = []
        rule_ids = []
        rules_old = None
        resp = self.client.getBucketLifecycle(bucket)
        if resp.status == 200:
            rules_old = resp.body.lifecycleConfig.rule
        rules_new = json.loads(rule_str).get('rule')
        if rules_new is not None and len(rules_new) > 0:
            for item_new in rules_new:
                rule.append(item_new)
                rule_ids.append(item_new['id'])
        if rules_old is not None and len(rules_old) > 0:
            for item_old in rules_old:
                if rule is not None and len(rule) > 0 and item_old['id'] not in rule_ids:
                    rule.append(item_old)
            return json_str + json.dumps(rule) + '}'
        else:
            return rule_str

