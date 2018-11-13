#!/usr/bin/env python
# -*- coding: UTF-8 -*-
from obscmd.cmds.obs.obsutil import check_resp, split_bucket_key, join_bucket_key, ObsCmdUtil
from obscmd.cmds.obs.subcmd import SubObsCommand
from obscmd.compat import safe_decode
from obscmd.utils import bytes_to_unitstr


class LsCommand(SubObsCommand):
    NAME = 'ls'
    DESCRIPTION = ("List Obs objects and common prefixes under a prefix or "
                   "all Obs buckets.")
    USAGE = "ls <ObsUri> or NONE [--outfile] [--recursive] [--limit]"
    ARG_TABLE = [
        {'name': 'paths', 'nargs': '?', 'default': 'obs://',
            'positional_arg': True, 'synopsis': USAGE},
        {
             'name': 'outfile',
             'help_text': 'save objects into a file'
        },
        {'name': 'recursive', 'action': 'store_true',
         'help_text': "list all objects"},
        {'name': 'limit', 'cli_type_name': 'integer',
         'help_text': "limit number of objects for display"},
    ]

    EXAMPLES = """
        The following ls command lists all of the bucket owned by the user.  In
       this example, the user owns the buckets mybucket  and  mybucket2.   The
       timestamp  is  the date the bucket was created, shown in your machine's
       time zone.  Note if obs:// is used for the  path  argument  <obsUri>,  it
       will list all of the buckets as well:

          obscmd obs ls

       Output:

            2018/08/08 15:24:47	cn-north-1	STANDARD	obs://tsdd
            2018/08/13 15:43:49	cn-east-2	STANDARD	obs://uvmtzfnh
            2018/08/08 17:16:31	cn-east-2	STANDARD	obs://vbmcovbm
            2018/08/04 12:34:35	cn-north-1	STANDARD	obs://vjisr1zu

       The  following  ls  command  lists  objects and common prefixes under a
       specified bucket and prefix.  In this example, the user owns the bucket
       mybucket  with the objects and directories.  The Last-
       WriteTime and Length are arbitrary. 

          obscmd obs ls obs://mybucket/doc/

       Output:

                             --	      --	obs://mybucket/doc/doc/
                             --	      --	obs://mybucket/doc/lfw_home/
                             --	      --	obs://mybucket/doc/news/
                             --	      --	obs://mybucket/doc/pic/
                             --	      --	obs://mybucket/doc/portia_projects/
            2018/08/02 22:43:21	   14.9M	obs://mybucket/doc/20news-bydate.pkz
            2018/08/02 22:43:21	     16B	obs://mybucket/doc/dd.t
            2018/08/02 22:43:21	     14B	obs://mybucket/doc/ee.txt
            2018/08/02 22:43:21	  191.4K	obs://mybucket/doc/lfw_home.tar.gz
            2018/08/02 22:43:21	     16B	obs://mybucket/doc/tt.data
            2018/08/02 22:43:21	     21B	obs://mybucket/doc/up.dat
       

       The following ls command will recursively list  objects  in  a  bucket.
       Rather  than  showing  PRE dirname/ in the output, all the content in a
       bucket will be listed in order:

          obscmd obs ls obs://mybucket --recursive -limit 10

       Output:

          2013-09-02 21:37:53         10 obs://mybucket/a.txt
          2013-09-02 21:37:53       286M obs://mybucket/foo.zip
          2013-09-02 21:32:57         23 obs://mybucket/foo/bar/.baz/a
          2013-09-02 21:32:58         41 obs://mybucket/foo/bar/.baz/b
          2013-09-02 21:32:57        281 obs://mybucket/foo/bar/.baz/c
          2013-09-02 21:32:57         73 obs://mybucket/foo/bar/.baz/d
          2013-09-02 21:32:57        452 obs://mybucket/foo/bar/.baz/e
          2013-09-02 21:32:57        896 obs://mybucket/foo/bar/.baz/hooks/bar
          2013-09-02 21:32:57        189 obs://mybucket/foo/bar/.baz/hooks/foo
          2013-09-02 21:32:57        398 obs://mybucket/z.txt
          
        list object to a file.
        
          obscmd obs ls obs://mybucket --recursive -limit 10 --outfile result.txt
          
        Output:
          complete.
    """

    def _run(self, parsed_args, parsed_globals):

        path = parsed_args.paths
        outfile = parsed_args.outfile
        if outfile:
            with open(outfile, 'w'):
                pass
        recursive = parsed_args.recursive
        limit = int(parsed_args.limit) if parsed_args.limit else parsed_args.limit
        bucket, key = split_bucket_key(path)

        self._outprint('start to find list ...')
        if not bucket:
            self._list_all_buckets()
        elif recursive:
            self._list_all_objects(bucket, key, limit, outfile)
        else:
            self._list_dirs_objects(bucket, key, limit, outfile)

    def _list_all_buckets(self):
        resp = self.client.listBuckets()
        check_resp(resp)

        if not resp.body.buckets:
            self._outprint("no bucket found.")
        bucket_infos = []

        for bucket in resp.body.buckets:
            storage = ObsCmdUtil(self.client).get_bucket_storage(bucket.name)
            bucket_infos.append((bucket, storage))

        for bucket, storage in bucket_infos:
            self._outprint("%-19s\t%-8s\t%-8s\t%-8s\n" % (bucket.create_date, bucket.location, storage, join_bucket_key(bucket.name)))

    def _list_dirs_objects(self, bucket, prefix, limit, outfile=None):

        dirs, objects = ObsCmdUtil(self.client).list_dirs_objects(bucket, prefix)
        cur = 0
        for item in dirs:
            if limit and cur >= limit:
                break
            self._outprint("%19s\t%8s\t%s\n" % ('--', '--', join_bucket_key(bucket, item)), outfile)
            cur += 1
        for item in objects:
            if limit and cur >= limit:
                break
            self._outprint("%19s\t%8s\t%s\n" % (item['lastModified'], bytes_to_unitstr(item['size']), (join_bucket_key(bucket, item['key']))), outfile)
            cur += 1

        self._outprint('\ntotal: %d dirs, %d objects' % (len(dirs), len(objects)), outfile)

    def _list_all_objects(self, bucket, prefix, limit,  outfile=None):
        items = ObsCmdUtil(self.client).get_objects_info(bucket, prefix, ('lastModified', 'size', 'key'), limit)

        for item in items:
            self._outprint("%19s\t%8s\t%s\n" % (item[0], bytes_to_unitstr(item[1]), join_bucket_key(bucket, item[2])), outfile)

        self._outprint('\ndisplay total: %d objects' % len(items), outfile)
        if outfile:
            self._outprint('ls to file %s complete.' % outfile)