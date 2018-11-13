#!/usr/bin/env python
# -*- coding: UTF-8 -*-
from obscmd.utils import bytes_to_unitstr

from obscmd.cmds.obs.obsutil import get_bucket, check_resp
from obscmd.cmds.obs.subcmd import SubObsCommand


class InfobCommand(SubObsCommand):
    NAME = 'infob'
    DESCRIPTION = "show bucket information"
    USAGE = ("infob <ObsUri> [--acl] [--lifecycle] [--location] [--metadata] [--policy] "
             "[--quota] [--storage] [--tagging] [--logging] [--website]")
    ARG_TABLE = [
        {'name': 'path', 'nargs': '?', 'default': 'obs://',
         'positional_arg': True, 'synopsis': USAGE},
        {'name': 'acl', 'action': 'store_true',
         'help_text': "get bucket acl info"},
        {'name': 'lifecycle', 'action': 'store_true',
         'help_text': "get bucket lifecycle info"},
        {'name': 'location', 'action': 'store_true',
         'help_text': "get bucket location info"},
        {'name': 'metadata', 'action': 'store_true',
         'help_text': "get bucket metadata info"},
        {'name': 'policy', 'action': 'store_true',
         'help_text': "get bucket policy info"},
        {'name': 'quota', 'action': 'store_true',
         'help_text': "get bucket quota info"},
        {'name': 'storage', 'action': 'store_true',
         'help_text': "get bucket storage info"},
        {'name': 'tagging', 'action': 'store_true',
         'help_text': "get bucket tagging info"},
        {'name': 'logging', 'action': 'store_true',
         'help_text': "get bucket tagging info"},
        {'name': 'website', 'action': 'store_true',
         'help_text': "get bucket website configuration info"},
    ]

    EXAMPLES = """
        
        In this example, the user look for all information about the bucket.

          obscmd obs infob obs://mybucket

        Output:

            #######################     acl    #######################
            owner_id: 8be8301bdfc7469aaf721d0504826420
            owner_name: test
            
            ############## grant [1]
            grant_id: 8be8301bdfc7469aaf721d0504826420
            grant_name: jiaowoxiaoming
            group: None
            permission: FULL_CONTROL
            
            
            #######################     lifecycle    #######################
            
            
            #######################     location    #######################
            location: cn-north-1
            
            
            #######################     metadata    #######################
            storageClass: STANDARD
            storageClass: STANDARD
            accessContorlAllowOrigin: None
            accessContorlMaxAge: None
            accessContorlExposeHeaders: None
            accessContorlAllowMethods: None
            accessContorlAllowHeaders: None
            
            
            #######################     policy    #######################
            
            
            #######################     quota    #######################
            quota: None
            
            
            #######################     storage    #######################
            size: 10485760
            objectNumber: 0
            
            
            #######################     tagging    #######################
            
            
        In this example, the user look for some information about the bucket.

            obscmd obs infob obs://mybucket --acl --location

        Output:
            #######################     acl    #######################
            owner_id: 8be8301bdfc7469aaf721d0504826420
            owner_name: jiaowoxiaoming
            
            ############## grant [1]
            grant_id: 8be8301bdfc7469aaf721d0504826420
            grant_name: jiaowoxiaoming
            group: None
            permission: FULL_CONTROL
            
            
            #######################     location    #######################
            location: cn-north-1

    """

    def _run(self, parsed_args, parsed_globals):
        path = parsed_args.path

        acl = parsed_args.acl
        lifecycle = parsed_args.lifecycle
        location = parsed_args.location
        metadata = parsed_args.metadata
        policy = parsed_args.policy
        quota = parsed_args.quota
        storage = parsed_args.storage
        tagging = parsed_args.tagging
        logging = parsed_args.logging
        website = parsed_args.website

        bucket = get_bucket(path)

        switches = [acl, lifecycle, location, metadata, policy, quota, storage, tagging, logging, website]
        topics = ['acl', 'lifecycle', 'location', 'metadata', 'policy', 'quota', 'storage', 'tagging', 'logging', 'website']
        show_all = True not in switches

        for i in range(len(switches)):
            if show_all or switches[i]:
                self._print_topic_split_line(topics[i])
                method = getattr(self, '_get_bucket_' + topics[i], None)
                method(bucket)

    def _print_topic_split_line(self, topic):
        self._outprint('\n\n')
        self._outprint("#######################     %s    #######################\n" % topic)

    def _get_bucket_metadata(self, bucket):
        """
        get bucket metadata info
        :param bucket: 
        :return: 
        """
        resp = self.client.getBucketMetadata(bucket)
        check_resp(resp)
        self._outprint('obs version: %s' % resp.body.obsVersion)
        self._outprint("storage type: %s" % resp.body.storageClass)
        self._outprint('location: %s' % resp.body.location)

    def _get_bucket_location(self, bucket):
        """
        get bucket location info
        :param bucket: 
        :return: 
        """
        resp = self.client.getBucketLocation(bucket)
        check_resp(resp)
        self._outprint("location: %s\n" % resp.body.location)

    def _get_bucket_storage(self, bucket):
        """
        get bucket storage info, include size and object number
        :param bucket: 
        :return: 
        """
        resp = self.client.getBucketStorageInfo(bucket)
        check_resp(resp)
        self._outprint("size: %s\n" % bytes_to_unitstr(resp.body.size))
        self._outprint("objectNumber: %s\n" % resp.body.objectNumber)

    def _get_bucket_quota(self, bucket):
        resp = self.client.getBucketStorageInfo(bucket)
        check_resp(resp)
        self._outprint("quota: %s\n" % resp.body.quota)

    def _get_bucket_acl(self, bucket):
        resp = self.client.getBucketAcl(bucket)
        check_resp(resp)
        self._outprint('owner_id: %s\n' % resp.body.owner.owner_id)
        self._outprint('owner_name: %s\n' % resp.body.owner.owner_name)

        acls = {}
        for grant in resp.body.grants:
            group = grant.grantee.group
            if group not in acls :
                acls[group] = {}
            grantee_id = grant.grantee.grantee_id
            if grantee_id not in acls[group]:
                acls[group][grantee_id] = {}
            acls[group][grantee_id]['name'] = grant.grantee.grantee_name
            if 'premission' not in acls[group][grantee_id] :
                acls[group][grantee_id]['premission'] = []
            acls[group][grantee_id]['premission'].append(grant.permission)
        for group, grantees in acls.items() :
            self._outprint('\n############## group: %s' % group)
            for grantee_id, item in grantees.items() :
                self._outprint('grant_id: %s\n' % grantee_id)
                self._outprint('grant_name: %s\n' % item['name'])
                self._outprint('permission: %s\n\n' % ', '.join(item['premission']))

        # index = 1
        # for grant in resp.body.grants:
        #     self._outprint('\n############## grant [' + str(index) + ']')
        #     self._outprint('grant_id: %s\n' % grant.grantee.grantee_id)
        #     self._outprint('grant_name: %s\n' % grant.grantee.grantee_name)
        #     self._outprint('group: %s\n' % grant.grantee.group)
        #     self._outprint('permission: %s\n' % grant.permission)
        #     index += 1

    def _get_bucket_policy(self, bucket):
        try:
            resp = self.client.getBucketPolicy(bucket)
            check_resp(resp)
        except Exception as e:
            if 'NoSuchBucketPolicy' not in str(e):
                raise e
            else:
                self._outprint("no bucket policy.")
                return
        self._outprint("policyJSON: %s\n" % resp.body.policyJSON)

    def _get_bucket_lifecycle(self, bucket):
        try:
            resp = self.client.getBucketLifecycleConfiguration(bucket)
            check_resp(resp)
        except Exception as e:
            if 'NoSuchLifecycleConfiguration' not in str(e):
                raise e
            else:
                self._outprint("no bucket lifecycle.")
                return

        index = 1
        for rule in resp.body.lifecycleConfig.rule:

            self._outprint('\n############## rule [' + str(index) + ']\n')
            self._outprint('id: %s\n' % rule.id)
            self._outprint('rule.prefix: %s\n' % rule.prefix)
            self._outprint('rule.status: %s\n' % rule.status)
            if rule.noncurrentVersionExpiration:
                self._outprint('rule.noncurrentVersionExpiration: %s\n' % rule.noncurrentVersionExpiration.noncurrentDays)
            if rule.expiration:
                self._outprint('rule.expiration.days: %s\n' % rule.expiration.days)
            self._outprint('')
            if rule.noncurrentVersionTransition:
                self._outprint('### rule.noncurrentVersionExpiration')
                for item in rule.noncurrentVersionTransition:
                    self._outprint('storage: %s\tdays: %d' % (item['storageClass'], item['noncurrentDays']))
            self._outprint('')
            if rule.transition:
                self._outprint('### rule.transition')
                for item in rule.transition:
                    self._outprint('storage: %s\tdays: %d' % (item['storageClass'], item['days']))
            index += 1

    def _get_bucket_tagging(self, bucket):

        try:
            resp = self.client.getBucketTagging(bucket)
            check_resp(resp)
        except Exception as e:
            if 'NoSuchTagSet' not in str(e):
                raise e
            else:
                self._outprint("no bucket tag.")
                return
        for tag in resp.body.tagSet:
            self._outprint('tag {key}: {value}\n'.format(key=tag.key, value=tag.value))

    def _get_bucket_logging(self, bucket):
        resp = self.client.getBucketLoggingConfiguration(bucket)
        check_resp(resp)
        self._outprint('targetBucket: %s\n' % resp.body.targetBucket)
        self._outprint('targetPrefix: %s\n' % resp.body.targetPrefix)

        index = 1
        for grant in resp.body.targetGrants:
            self._outprint('\n############## grant [' + str(index) + ']')
            self._outprint('grant_id: %s\n' % grant.grantee.grantee_id)
            self._outprint('grant_name: %s\n' % grant.grantee.grantee_name)
            self._outprint('group: %s\n' % grant.grantee.group)
            self._outprint('permission: %s\n' % grant.permission)
            index += 1

    def _get_bucket_website(self, bucket):

        try:
            resp = self.client.getBucketWebsiteConfiguration(bucket)
            check_resp(resp)
        except Exception as e:
            if 'NoSuchWebsiteConfiguration' not in str(e):
                raise e
            else:
                self._outprint("no bucket website configuration.")
                return

        if resp.body.redirectAllRequestTo:
            self._outprint('redirectAllRequestTo.hostName: %s' % resp.body.redirectAllRequestTo.hostName)
            self._outprint('redirectAllRequestTo.protocol: %s' % resp.body.redirectAllRequestTo.protocol)
        if resp.body.indexDocument:
            self._outprint('indexDocument.suffix: %s' % resp.body.indexDocument.suffix)
        if resp.body.errorDocument:
            self._outprint('errorDocument.key: %s' % resp.body.errorDocument.key)
        self._outprint('')
        if resp.body.routingRules:
            index = 1
            for rout in resp.body.routingRules:
                self._outprint('##### routingRule[%d]:' % index)
                index += 1
                self._outprint('condition.keyPrefixEquals: %s ' % rout.condition.keyPrefixEquals)
                self._outprint('condition.httpErrorCodeReturnedEquals: %s' % rout.condition.httpErrorCodeReturnedEquals)
                self._outprint('redirect.protocol: %s ' % rout.redirect.protocol)
                self._outprint('redirect.hostName: %s' % rout.redirect.hostName)
                self._outprint('redirect.replaceKeyPrefixWith: %s' % rout.redirect.replaceKeyPrefixWith)
                self._outprint('redirect.replaceKeyWith: %s' % rout.redirect.replaceKeyWith)
                self._outprint('redirect.httpRedirectCode: %s' % rout.redirect.httpRedirectCode)
                self._outprint('')


