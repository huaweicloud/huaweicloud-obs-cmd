#!/usr/bin/env python
# -*- coding: UTF-8 -*-

__version__ = '5.0.0_beta'

ACL_CONTROL = ['private', 'public-read', 'public-read-write',
               # 'public-read-delivered', 'public-read-write-delivered',
               ]

STORAGE_CLASS = ['STANDARD', 'WARM', 'COLD']
STORAGE_CLASS_TR = {'STANDARD': 'STANDARD',
                    'STANDARD_IA': 'WARM',
                    'GLACIER': 'COLD'}
#k:param in header; v:property of object
HEADER_PARAMS = {'content-length' : 'size', 'id-2' : 'id', 'last-modified' : 'lastModified',
                 'connection' : 'connection', 'request-id' : 'requestId', 'etag' : 'etag', 'x-reserved' : 'reserved',
                 'date' : 'date', 'version-id' : 'versionId', 'partsize' : 'partSize'}

PUTFILE_MAX_SIZE = 5 * 1024 ** 3
