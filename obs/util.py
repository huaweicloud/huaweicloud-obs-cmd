#!/usr/bin/python
# -*- coding:utf-8 -*-

import re
import base64
import hashlib
from obs.const import LONG, IS_PYTHON2, UNICODE, IPv4_REGEX
if IS_PYTHON2:
    import urllib
else:
    import urllib.parse as urllib
from obs.ilog import INFO, ERROR

def to_bool(item):
    try:
        return True if item is not None and str(item).lower() == 'true' else False
    except Exception:
        return None

def to_int(item):
    try:
        return int(item)
    except Exception:
        return None

def to_long(item):
    try:
        return LONG(item)
    except Exception:
        return None

def to_float(item):
    try:
        return float(item)
    except Exception:
        return None

def to_string(item):
    try:
        return str(item) if item is not None else ''
    except Exception:
        return ''

def is_valid(item):
    return item is not None and item.strip() != ''

class RequestFormat(object):

    @staticmethod
    def get_pathformat():
        return PathFormat()

    @staticmethod
    def get_subdomainformat():
        return SubdomainFormat()

    @classmethod
    def convert_path_string(cls, path_args, allowdNames=None):
        e = ''
        if isinstance(path_args, dict):
            e1 = '?'
            e2 = '&'
            for path_key, path_value in path_args.items():
                flag = True
                if allowdNames is not None and path_key not in allowdNames:
                    flag = False
                if flag:
                    path_key = encode_item(path_key, '/')
                    if path_value is None:
                        e1 += path_key + '&'
                        continue
                    e2 += path_key + '=' + encode_item(path_value, '/') + '&'
            e = (e1 + e2).replace('&&', '&').replace('?&', '?')[:-1]
        return e

    def get_endpoint(self, server, port, bucket):
        return

    def get_pathbase(self, bucket, key):
        return

    def get_url(self, bucket, key, path_args):
        return

class PathFormat(RequestFormat):

    def get_server(self, server, bucket):
        return server

    def get_pathbase(self, bucket, key):
        if not bucket:
            return '/'
        if key is None:
            return '/' + bucket
        return '/' + bucket + '/' + encode_object_key(key)

    def get_endpoint(self, server, port, bucket):
        return server + ':' + str(port)

    def get_url(self, bucket, key, path_args):
        path_base = self.get_pathbase(bucket, key)
        path_arguments = self.convert_path_string(path_args)
        return path_base + path_arguments

    def get_full_url(self, is_secure, server, port, bucket, key, path_args):
        url = 'https://' if is_secure else 'http://'
        url += self.get_endpoint(server, port, bucket)
        url += self.get_url(bucket, key, path_args)
        return url

class SubdomainFormat(RequestFormat):

    def get_server(self, server, bucket):
        return bucket + '.' + server if bucket else server

    def get_pathbase(self, bucket, key):
        if key is None:
            return '/'
        return '/' + encode_object_key(key)

    def get_endpoint(self, server, port, bucket):
        return self.get_server(server, bucket) + ':' + str(port)

    def get_url(self, bucket, key, path_args):
        url = self.convert_path_string(path_args)
        return self.get_pathbase(bucket, key) + url if bucket else url

    def get_full_url(self, is_secure, server, port, bucket, key, path_args):
        url = 'https://' if is_secure else 'http://'
        url += self.get_endpoint(server, port, bucket)
        url += self.get_url(bucket, key, path_args)
        return url


def get_readable_entity(readable, chunk_size=65536):
    def entity(conn):
        try:
            while True:
                chunk = readable.read(chunk_size)
                if not chunk:
                    conn.send('0\r\n\r\n' if IS_PYTHON2 else '0\r\n\r\n'.encode('UTF-8'))
                    break
                hex_chunk = hex(len(chunk))[2:]
                conn.send(hex_chunk if IS_PYTHON2 else hex_chunk.encode('UTF-8'))
                conn.send('\r\n' if IS_PYTHON2 else '\r\n'.encode('UTF-8'))
                conn.send(chunk)
        finally:
            if hasattr(readable, 'close') and callable(readable.close):
                readable.close()
    return entity

def get_readable_entity_by_totalcount(readable, totalCount, chunk_size=65536):
    def entity(conn):
        try:
            readCount = 0
            while True:
                if readCount >= totalCount:
                    break
                readCountOnce = chunk_size if totalCount - readCount >= chunk_size else totalCount - readCount
                chunk = readable.read(readCountOnce)
                if not chunk:
                    break
                conn.send(chunk)
                readCount = readCount + readCountOnce
        finally:
            if hasattr(readable, 'close') and callable(readable.close):
                readable.close()
    return entity

def get_file_entity(file_path, chunk_size=65536):
    def entity(conn):
        with open(file_path, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                conn.send(chunk)
    return entity

def get_file_entity_by_totalcount(file_path, totalCount, chunk_size=65536):
    def entity(conn):
        readCount = 0
        with open(file_path, 'rb') as f:
            while True:
                if readCount >= totalCount:
                    break
                readCountOnce = chunk_size if totalCount - readCount >= chunk_size else totalCount - readCount
                chunk = f.read(readCountOnce)
                if not chunk:
                    break
                conn.send(chunk)
                readCount = readCount + readCountOnce
    return entity

def get_file_entity_by_offset_partsize(file_path, offset, partSize, chunk_size=65536):
    def entity(conn):
        readCount = 0
        with open(file_path, 'rb') as f:
            f.seek(offset)
            while readCount < partSize:
                read_size = chunk_size if partSize - readCount >= chunk_size else partSize - readCount
                chunk = f.read(read_size)
                readCountOnce = len(chunk)
                if readCountOnce <= 0:
                    break
                conn.send(chunk)
                readCount += readCountOnce
    return entity


def is_ipaddress(item):
    return re.match(IPv4_REGEX, item)

def md5_encode(unencoded):
    m = hashlib.md5()
    unencoded = unencoded if IS_PYTHON2 else (unencoded.encode('UTF-8') if not isinstance(unencoded, bytes) else unencoded)
    m.update(unencoded)
    return m.digest()

def base64_encode(unencoded):
    unencoded = unencoded if IS_PYTHON2 else (unencoded.encode('UTF-8') if not isinstance(unencoded, bytes) else unencoded)
    encodeestr = base64.b64encode(unencoded, altchars=None)
    return encodeestr if IS_PYTHON2 else encodeestr.decode('UTF-8')

def encode_object_key(key):
    return encode_item(key, '/')


def encode_item(item, safe='/'):
    return urllib.quote(to_string(item), safe)

def decode_item(item):
    return urllib.unquote(item)


def safe_trans_to_utf8(item):
    if not IS_PYTHON2:
        return item
    if item is not None:
        item = safe_encode(item)
        try:
            return item.decode('GB2312').encode('UTF-8')
        except Exception:
            return item
    return None

def safe_trans_to_gb2312(item):
    if not IS_PYTHON2:
        return item
    if item is not None:
        item = safe_encode(item)
        try:
            return item.decode('UTF-8').encode('GB2312')
        except Exception:
            return item
    return None

def safe_decode(item):
    if not IS_PYTHON2:
        return item
    if isinstance(item, str):
        try:
            item = item.decode('UTF-8')
        except:
            try:
                item = item.decode('GB2312')
            except Exception:
                item = None
    return item

def safe_encode(item):
    if not IS_PYTHON2:
        return item
    if isinstance(item, UNICODE):
        try:
            item = item.encode('UTF-8')
        except UnicodeDecodeError:
            try:
                item = item.encode('GB2312')
            except Exception:
                item = None
    return item


def md5_file_encode_by_size_offset(filepath=None, size=None, offset=None, chucksize=None):
    if filepath is not None and size is not None and offset is not None:
        m = hashlib.md5()
        with open(filepath, 'rb') as fp:
            CHUNKSIZE = 65536 if chucksize is None else chucksize
            fp.seek(offset)
            read_count = 0
            while read_count < size:
                read_size = CHUNKSIZE if size - read_count >= CHUNKSIZE else size - read_count
                data = fp.read(read_size)
                read_count_once = len(data)
                if read_count_once <= 0:
                    break
                m.update(data)
                read_count += read_count_once
        return m.digest()


def do_close(result, conn, connHolder, log_client=None):
    if not result:
        close_conn(conn, log_client)
    elif result.getheader('connection', '').lower() == 'close' or result.getheader('Connection', '').lower() == 'close':
        if log_client:
            log_client.log(INFO, 'server inform to close connection')
        close_conn(conn, log_client)
    elif to_int(result.status) >= 500 or connHolder is None:
        close_conn(conn, log_client)
    elif hasattr(conn, '_redirect') and conn._redirect:
        close_conn(conn, log_client)
    else:
        if connHolder is not None:
            try:
                connHolder['connSet'].put_nowait(conn)
            except:
                close_conn(conn, log_client)

def close_conn(conn, log_client=None):
    try:
        if conn:
            conn.close()
    except Exception as ex:
        if log_client:
            log_client.log(ERROR, ex)

SKIP_VERIFY_ATTR_TYPE = False
def verify_attr_type(value, allowedAttrType):
    if SKIP_VERIFY_ATTR_TYPE:
        return True
    if isinstance(allowedAttrType, list):
        for t in allowedAttrType:
            if isinstance(value, t):
                return True
        return False
    return isinstance(value, allowedAttrType)
