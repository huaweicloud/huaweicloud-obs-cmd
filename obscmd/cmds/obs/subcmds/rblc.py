#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import json

from obscmd.cmds.obs.obsutil import split_bucket_key, check_resp
from obscmd.cmds.obs.subcmd import SubObsCommand


class RblcCommand(SubObsCommand):
    NAME = 'rblc'
    DESCRIPTION = "delete obs bucket lifecycle."
    USAGE = "rblc <ObsUri>"
    ARG_TABLE = [{'name': 'paths', 'positional_arg': True, 'synopsis': USAGE},
                 {'name': 'id', 'synopsis': USAGE,
                  'help_text': "set bucket lifecycle"}]

    EXAMPLES = """  
            The following command deletes all the lifecycle of a bucket:

              obscmd obs rblc obs://mybucket --id rule1

           Output:

              delete lifecycle: rule1 complete.
    """

    def _run(self, parsed_args, parsed_globals):
        path = parsed_args.paths
        rule_id = parsed_args.id

        bucket, key = split_bucket_key(path)
        self._warning_prompt('Are you sure to delete bucket life circle?')

        if rule_id is None:
            # delete all lifecycles
            resp = self.client.deleteBucketLifecycle(bucket)
            check_resp(resp)
            self._outprint("delete all lifecycle complete.")
        else:
            # delete one lifecycle
            rule_str = self._check_lifecycle(bucket, rule_id)
            if rule_str is not None and rule_str != 'NOTIN':
                resp = self.client.deleteBucketLifecycle(bucket)
                check_resp(resp)
                lifecycle = json.loads(rule_str)
                resp = self.client.setBucketLifecycle(bucket, lifecycle)
                check_resp(resp)
                self._outprint("delete lifecycle " + rule_id + " complete.")
            elif rule_str is None:
                resp = self.client.deleteBucketLifecycle(bucket)
                check_resp(resp)
                self._outprint("delete lifecycle " + rule_id + " complete.")
            elif rule_str == 'NOTIN':
                self._outprint("there is no lifecycle named: " + rule_id)

    def _check_lifecycle(self, bucket, rule_id):
        json_str = None
        rule = []
        rule_ids = []
        rules_old = None
        resp = self.client.getBucketLifecycle(bucket)
        if resp.status == 200:
            rules_old = resp.body.lifecycleConfig.rule
        if rules_old is not None and len(rules_old) > 0:
            for item_old in rules_old:
                rule_ids.append(item_old['id'])
                if item_old['id'] != rule_id:
                    rule.append(item_old)
            if rule is not None and len(rule) > 0:
                json_str = '{"rule":' + json.dumps(rule) + '}'

        if rule_id not in rule_ids:
            json_str = 'NOTIN'

        return json_str
