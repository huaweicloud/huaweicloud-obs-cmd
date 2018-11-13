#!/usr/bin/python
# -*- coding:utf-8 -*-

import socket
import time
import functools
import threading
import os
import sys
import re
import traceback
import math

from obs import const
from obs import convertor, util, auth
from obs.ilog import NoneLogClient, INFO, WARNING, ERROR, DEBUG, LogClient
from obs.transfer import _resumer_upload, _resumer_download
from obs.model import Logging
from obs.model import AppendObjectHeader
from obs.model import AppendObjectContent
from obs.model import Notification
from obs.model import ListMultipartUploadsRequest
from obs.model import PutObjectHeader
from obs.model import BaseModel
from obs.model import GetResult
from obs.model import ObjectStream
from obs.model import ResponseWrapper
from obs.model import CreateBucketHeader
from obs.model import ACL
from obs.model import Versions
from obs.model import GetObjectRequest
from obs.model import GetObjectHeader
from obs.model import CopyObjectHeader
from obs.bucket import BucketClient

if const.IS_PYTHON2:
    from urlparse import urlparse
    import httplib
else:
    import http.client as httplib
    from urllib.parse import urlparse

class _RedirectException(Exception):
    def __init__(self, msg, location):
        self.msg = msg
        self.location = location
    
    def __str__(self):
        return self.msg

class _SecurityProvider(object):
    def __init__(self, access_key_id, secret_access_key, security_token=None):
        access_key_id = util.to_string(util.safe_encode(access_key_id)).strip()
        secret_access_key = util.to_string(util.safe_encode(secret_access_key)).strip()
        security_token = util.to_string(util.safe_encode(security_token)).strip() if security_token is not None else None
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.security_token = security_token

def count_time(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        ret = None
        obsClient = args[0] if isinstance(args[0], ObsClient) else None
        
        try:
            if obsClient:    
                obsClient.log_client.log(INFO, 'enter %s ...' % func.__name__)
                if func.__name__ == 'copyObject':
                    if 'destBucketName' in kwargs:
                        obsClient._assert_not_null(kwargs['destBucketName'], 'destBucketName is empty')
                    elif len(args) >= 4:
                        obsClient._assert_not_null(args[3], 'destBucketName is empty')
                else:
                    if len(args) > 1:
                        obsClient._assert_not_null(args[1], 'bucketName is empty')
                    elif 'bucketName' in kwargs:
                        obsClient._assert_not_null(kwargs['bucketName'], 'bucketName is empty')
            ret = func(*args, **kwargs)
        finally:
            if obsClient:
                obsClient.log_client.log(INFO, '%s cost %s ms' % (func.__name__, int((time.time() - start) * 1000)))
        return ret
    return wrapper

class _BasicClient(object):
    def __init__(self, access_key_id, secret_access_key, is_secure=True, server=None, 
                 signature='v2', region='region', path_style=False, ssl_verify=False,
                 port=None, max_retry_count=3, timeout=60, chunk_size=65536, 
                 long_conn_mode=False, proxy_host=None, proxy_port=None, 
                 proxy_username=None, proxy_password=None, security_token=None, custom_ciphers=None):
        self.securityProvider = _SecurityProvider(access_key_id, secret_access_key, security_token)
        
        server = server if server is not None else ''
        server = util.to_string(util.safe_encode(server))
        
        _server = urlparse(server)
        
        hostname = _server.netloc if util.is_valid(_server.netloc) else _server.path
        
        if not util.is_valid(hostname):
            raise Exception('server is not set correctly')
        
        if util.is_valid(_server.scheme):
            if _server.scheme == 'https':
                is_secure = True
            elif _server.scheme == 'http':
                is_secure = False
        
        host_port = hostname.split(':')
        if len(host_port) == 2:
            port = util.to_int(host_port[1])
            
        self.is_secure = is_secure
        self.server = host_port[0]
        
        path_style = True if util.is_ipaddress(self.server) else path_style
        
        self.signature = util.to_string(util.safe_encode(signature))
        self.region = region
        self.path_style = path_style
        self.ssl_verify = ssl_verify
        self.calling_format = util.RequestFormat.get_pathformat() if self.path_style else util.RequestFormat.get_subdomainformat()
        self.port = port if port is not None else const.DEFAULT_SECURE_PORT if is_secure else const.DEFAULT_INSECURE_PORT
        self.max_retry_count = max_retry_count
        self.timeout = timeout
        self.chunk_size = chunk_size
        self.log_client = NoneLogClient()
        self.context = None
        if self.is_secure:
            self._init_ssl_context(custom_ciphers)
            
        self.long_conn_mode = long_conn_mode
        self.connHolder = None
        if self.long_conn_mode:
            self._init_connHolder()
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        self.proxy_username = proxy_username
        self.proxy_password = proxy_password
        self.pattern = re.compile('xmlns="http.*?"')
        self.ha = convertor.Adapter(self.signature)
        self.convertor = convertor.Convertor(self.signature, self.ha)
    
    def _init_connHolder(self):
        if const.IS_PYTHON2:
            from Queue import Queue
        else:
            from queue import Queue
        self.connHolder = {'connSet' : Queue(), 'lock' : threading.Lock()}
    
    def _init_ssl_context(self, custom_ciphers):
        try:
            import ssl
            if hasattr(ssl, 'SSLContext'):
                context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
                context.options |= ssl.OP_NO_SSLv2
                context.options |= ssl.OP_NO_SSLv3
                if custom_ciphers is not None:
                    custom_ciphers = util.to_string(custom_ciphers).strip()
                    if custom_ciphers != '' and hasattr(context, 'set_ciphers') and callable(context.set_ciphers):
                        context.set_ciphers(custom_ciphers)
                if self.ssl_verify:
                    import _ssl
                    cafile = util.to_string(self.ssl_verify)
                    context.options |= getattr(_ssl, "OP_NO_COMPRESSION", 0)
                    context.verify_mode = ssl.CERT_REQUIRED
                    if os.path.isfile(cafile):
                        context.load_verify_locations(cafile)
                else:
                    context.verify_mode = ssl.CERT_NONE
                self.context = context
                if hasattr(self.context, 'check_hostname'):
                    self.context.check_hostname = False
            else:
                self.context = None
        except Exception:
            print(traceback.format_exc())
    
    def close(self):
        if self.connHolder is not None:
            with self.connHolder['lock']:
                while not self.connHolder['connSet'].empty():
                    conn = self.connHolder['connSet'].get()
                    if conn and hasattr(conn, 'close'):
                        try:
                            conn.close()
                        except Exception as ex:
                            self.log_client.log(WARNING, ex)

            del self.connHolder['lock']
            del self.connHolder['connSet']
        self.connHolder = None

    def refresh(self, access_key_id, secret_access_key, security_token=None):
        self.securityProvider = _SecurityProvider(access_key_id, secret_access_key, security_token)
    
    def initLog(self, log_config=None, log_name='OBS_LOGGER'):
        if log_config:
            self.log_client = LogClient(log_config, 'OBS_LOGGER' if const.IS_WINDOWS else log_name, log_name)
            msg = ['[OBS SDK Version=' + const.OBS_SDK_VERSION]
            msg.append('Endpoint=' + ('%s://%s:%d' % ('https' if self.is_secure else 'http', self.server, self.port)))
            msg.append('Access Mode=' + ('Path' if self.path_style else 'Virtual Hosting') + ']')
            self.log_client.log(WARNING, '];['.join(msg))

    def _assert_not_null(self, param, msg):
        param = util.safe_encode(param)
        if param is None or util.to_string(param).strip() == '':
            raise Exception(msg)
    
    def _generate_object_url(self, ret, bucketName, objectKey):
        if ret and ret.status < 300 and ret.body:
            ret.body.objectUrl = self.calling_format.get_full_url(self.is_secure, self.server, self.port, bucketName, objectKey, {})
    
    def _make_options_request(self, bucketName, objectKey=None, pathArgs=None, headers=None, methodName=None):
        return self._make_request(const.HTTP_METHOD_OPTIONS, bucketName, objectKey, pathArgs, headers, methodName=methodName)

    def _make_head_request(self, bucketName, objectKey=None, pathArgs=None, headers=None, methodName=None):
        return self._make_request(const.HTTP_METHOD_HEAD, bucketName, objectKey, pathArgs, headers, methodName=methodName)

    def _make_get_request(self, bucketName='', objectKey=None, pathArgs=None, headers=None, methodName=None, parseMethod=None):
        return self._make_request(const.HTTP_METHOD_GET, bucketName, objectKey, pathArgs, headers, methodName=methodName, parseMethod=parseMethod)

    def _make_delete_request(self, bucketName, objectKey=None, pathArgs=None, headers=None, entity=None, methodName=None):
        return self._make_request(const.HTTP_METHOD_DELETE, bucketName, objectKey, pathArgs, headers, entity, methodName=methodName)

    def _make_post_request(self, bucketName, objectKey=None, pathArgs=None, headers=None, entity=None, chunkedMode=False, methodName=None, readable=False):
        return self._make_request(const.HTTP_METHOD_POST, bucketName, objectKey, pathArgs, headers, entity, chunkedMode, methodName=methodName, readable=readable)
    
    def _make_put_request(self, bucketName, objectKey=None, pathArgs=None, headers=None, entity=None, chunkedMode=False, methodName=None, readable=False):
        return self._make_request(const.HTTP_METHOD_PUT, bucketName, objectKey, pathArgs, headers, entity, chunkedMode, methodName=methodName, readable=readable)

    def _make_request(self, methodType, bucketName, objectKey=None, pathArgs=None, headers=None, 
                       entity=None, chunkedMode=False, methodName=None, readable=False, parseMethod=None):
        try:
            conn = self._make_request_internal(methodType, bucketName, objectKey, pathArgs, headers, entity, chunkedMode)
            result = self._parse_xml(conn, methodName, readable) if not parseMethod else parseMethod(conn)
        except _RedirectException as e:
            conn = self._make_request_internal(methodType, bucketName, objectKey, pathArgs, headers, entity, chunkedMode, e.location)
            conn._redirect = True
            result = self._parse_xml(conn, methodName, readable) if not parseMethod else parseMethod(conn)
        except Exception as e:
            flag = False
            if self.long_conn_mode:
                if const.IS_PYTHON35_UP:
                    if isinstance(e, httplib.RemoteDisconnected):
                        flag = True
                elif isinstance(e, httplib.BadStatusLine):
                    flag = True
                        
            if not flag:
                raise e
            conn = self._make_request_internal(methodType, bucketName, objectKey, pathArgs, headers, entity, chunkedMode)
            result = self._parse_xml(conn, methodName, readable) if not parseMethod else parseMethod(conn)
        
        return result

    def _make_request_internal(self, method, bucketName='', objectKey=None, pathArgs=None, headers=None, entity=None, 
                               chunkedMode=False, redirectLocation=None):
        objectKey = util.safe_encode(objectKey)
        if objectKey is None:
            objectKey = ''
        path = self.calling_format.get_url(bucketName, objectKey, pathArgs)
        port = None
        scheme = None
        if redirectLocation:
            redirectLocation = urlparse(redirectLocation)
            connect_server = redirectLocation.hostname
            scheme = redirectLocation.scheme
            port = redirectLocation.port if redirectLocation.port is not None else const.DEFAULT_INSECURE_PORT if scheme.lower() == 'http' else const.DEFAULT_SECURE_PORT
            redirect = True
        else:
            connect_server = self.calling_format.get_server(self.server, bucketName)
            redirect = False
            port = self.port

        headers = self._rename_request_headers(headers, method)

        if entity is not None and not callable(entity):
            entity = util.safe_encode(entity)
            if not isinstance(entity, str) and not isinstance(entity, bytes):
                entity = util.to_string(entity)
            if not const.IS_PYTHON2:
                entity = entity.encode('UTF-8') if not isinstance(entity, bytes) else entity
            headers[const.CONTENT_LENGTH_HEADER] = util.to_string(len(entity))

        headers[const.HOST_HEADER] = connect_server if port != 443 and port != 80 else '%s:%s' % (connect_server, port)
        header_config = self._add_auth_headers(headers, method, bucketName, objectKey, pathArgs)

        header_log = header_config.copy()
        header_log[const.HOST_HEADER] = '******'
        header_log[const.AUTHORIZATION_HEADER] = '******'
        self.log_client.log(DEBUG, 'method:%s, path:%s, header:%s', method, path, header_log)
        conn = self._send_request(connect_server, method, path, header_config, entity, port, scheme, redirect, chunkedMode)
        return conn

    def _add_auth_headers(self, headers, method, bucketName, objectKey, pathArgs):
        from datetime import datetime
        
        now_date = None
        if self.ha.date_header() not in headers:
            now_date = datetime.utcnow()
            headers[const.DATE_HEADER] = now_date.strftime(const.GMT_DATE_FORMAT)

        securityProvider = self.securityProvider
        ak = securityProvider.access_key_id
        sk = securityProvider.secret_access_key
        
        if util.is_valid(ak) and util.is_valid(sk):
            if securityProvider.security_token is not None:
                headers[self.ha.security_token_header()] = securityProvider.security_token
                
            if self.signature.lower() == 'v4':
                if now_date is None:
                    now_date = datetime.strptime(headers[self.ha.date_header()], const.LONG_DATE_FORMAT)
                shortDate = now_date.strftime(const.SHORT_DATE_FORMAT)
                longDate = now_date.strftime(const.LONG_DATE_FORMAT)
                v4Auth = auth.V4Authentication(ak, sk, str(self.region) if self.region is not None else '', shortDate, longDate, self.path_style, self.ha)
                ret = v4Auth.doAuth(method, bucketName, objectKey, pathArgs, headers)
                self.log_client.log(DEBUG, '%s: %s' % (const.CANONICAL_REQUEST, ret[const.CANONICAL_REQUEST]))
            else:
                obsAuth = auth.Authentication(ak, sk, self.path_style, self.ha)
                ret = obsAuth.doAuth(method, bucketName, objectKey, pathArgs, headers)
                self.log_client.log(DEBUG, '%s: %s' % (const.CANONICAL_STRING, ret[const.CANONICAL_STRING]))
            headers[const.AUTHORIZATION_HEADER] = ret[const.AUTHORIZATION_HEADER]
        return headers
    
    def _rename_request_headers(self, headers, method):
        new_headers = {}
        if isinstance(headers, dict):
            for k, v in headers.items():
                if k is not None and v is not None:
                    k = util.to_string(k).strip()
                    if k.lower() not in const.ALLOWED_REQUEST_HTTP_HEADER_METADATA_NAMES and not k.lower().startswith(const.V2_HEADER_PREFIX) and not k.lower().startswith(const.OBS_HEADER_PREFIX):
                        if method not in (const.HTTP_METHOD_PUT, const.HTTP_METHOD_POST):
                            continue
                        k = self.ha._get_meta_header_prefix() + k
                    
                    if(k.lower().startswith(self.ha._get_meta_header_prefix())):
                        k = util.encode_item(k, ' ;/?:@&=+$,')
                    
                    new_headers[k] = v if (isinstance(v, list)) else util.encode_item(v, ' ;/?:@&=+$,')
        return new_headers

    def _get_server_connection(self, server, port=None, scheme=None, redirect=False):
        if self.connHolder is not None and not self.connHolder['connSet'].empty() and not redirect:
            try:
                return self.connHolder['connSet'].get_nowait()
            except:
                self.log_client.log(DEBUG, 'can not get conn, will create a new one')

        is_secure = self.is_secure if scheme is None else True if scheme == 'https' else False

        if is_secure:
            self.log_client.log(DEBUG, 'is ssl_verify: %s', self.ssl_verify)
            if const.IS_PYTHON2:
                try:
                    conn = httplib.HTTPSConnection(server, port=port, timeout=self.timeout, context=self.context)
                except Exception:
                    conn = httplib.HTTPSConnection(server, port=port, timeout=self.timeout)
            else:
                conn = httplib.HTTPSConnection(server, port=port, timeout=self.timeout, context=self.context, check_hostname=False)
        else:
            conn = httplib.HTTPConnection(server, port=port, timeout=self.timeout)

        return conn

    def _send_request(self, server, method, path, header, entity=None, port=None, scheme=None, redirect=False, chunkedMode=False):

        flag = 0
        conn = None
        while True:
            try:
                connection_key = const.CONNECTION_HEADER
                if self.proxy_host is not None and self.proxy_port is not None:
                    conn = self._get_server_connection(util.to_string(self.proxy_host), util.to_int(self.proxy_port), scheme, redirect)
                    _header = {}
                    if self.proxy_username is not None and self.proxy_password is not None:
                        _header[const.PROXY_AUTHORIZATION_HEADER] = 'Basic %s' % (util.base64_encode(util.to_string(self.proxy_username) + ':' + util.to_string(self.proxy_password)))
                    conn.set_tunnel(server, port, _header)
                    connection_key = const.PROXY_CONNECTION_HEADER
                else:
                    conn = self._get_server_connection(server, port, scheme, redirect)

                if header is None:
                    header = {}

                if self.long_conn_mode:
                    header[connection_key] = const.CONNECTION_KEEP_ALIVE_VALUE
                else:
                    header[const.CONNECTION_HEADER] = const.CONNECTION_CLOSE_VALUE

                header[const.USER_AGENT_HEADER] = 'obs-sdk-python/' + const.OBS_SDK_VERSION

                if method == const.HTTP_METHOD_OPTIONS:
                    del header[const.HOST_HEADER]
                    conn.putrequest(method, path)
                    for k, v in header.items():
                        if k == const.ACCESS_CONTROL_REQUEST_METHOD_HEADER and isinstance(v, list):
                            for item in v:
                                conn.putheader(const.ACCESS_CONTROL_REQUEST_METHOD_HEADER, item)
                        elif k == const.ACCESS_CONTROL_REQUEST_HEADERS_HEADER and isinstance(v, list):
                            for item in v:
                                conn.putheader(const.ACCESS_CONTROL_REQUEST_HEADERS_HEADER, item)
                        else:
                            conn.putheader(k, v)
                    conn.endheaders()
                elif chunkedMode:
                    del header[const.HOST_HEADER]
                    header[const.TRANSFER_ENCODING_HEADER] = const.TRANSFER_ENCODING_VALUE
                    conn.putrequest(method, path)
                    for k, v in header.items():
                        conn.putheader(k, v)
                    conn.endheaders()
                else:
                    conn.request(method, path, headers=header)
            except socket.error as e:
                util.close_conn(conn, self.log_client)
                errno, _ = sys.exc_info()[:2]
                if errno != socket.timeout or flag >= self.max_retry_count:
                    self.log_client.log(ERROR, 'connect service error, %s' % e)
                    self.log_client.log(ERROR, traceback.format_exc())
                    raise e
                flag += 1
                time.sleep(math.pow(2, flag) * 0.05)
                self.log_client.log(WARNING, 'connect service time out, connect again, connect time:%d', int(flag))
                continue
            break

        if entity is not None:
            if callable(entity):
                entity(conn)
            else:
                conn.send(entity)
                self.log_client.log(DEBUG, 'request content:%s', util.to_string(entity))
        return conn
    
    def _getNoneResult(self, message='None Result'):
        return GetResult(code=-1, message=message, status=-1)
    
    def _parse_xml(self, conn, methodName=None, readable=False):
        if not conn:
            return self._getNoneResult('connection is none')
        result = None
        try:
            result = conn.getresponse()
            if not result:
                return self._getNoneResult('response is none')
            return self._parse_xml_internal(result, methodName, readable=readable)
        except _RedirectException as ex:
            raise ex
        except Exception as e:
            self.log_client.log(ERROR, traceback.format_exc())
            raise e
        finally:
            util.do_close(result, conn, self.connHolder, self.log_client)
            
    def _parse_content(self, conn, objectKey, downloadPath=None, chuckSize=65536, loadStreamInMemory=False):
        if not conn:
            return self._getNoneResult('connection is none')
        close_conn_flag = True
        result = None
        try:
            result = conn.getresponse()
            if not result:
                return self._getNoneResult('response is none')
 
            if not util.to_int(result.status) < 300:
                return self._parse_xml_internal(result)
 
            if loadStreamInMemory:
                self.log_client.log(DEBUG, 'loadStreamInMemory is True, read stream into memory')
                buf = None
                while True:
                    chunk = result.read(chuckSize)
                    if not chunk:
                        break
                    if buf is None:
                        buf = chunk
                    else:
                        buf += chunk
                body = ObjectStream(buffer=buf, size=util.to_long(len(buf)))
            elif downloadPath is None:
                self.log_client.log(DEBUG, 'DownloadPath is none, return conn directly')
                close_conn_flag = False
                body = ObjectStream(response=ResponseWrapper(conn, result, self.connHolder))
            else:
                objectKey = util.safe_encode(objectKey)
                downloadPath = util.safe_encode(downloadPath)
                file_path = self._get_data(result, downloadPath, chuckSize)
                body = ObjectStream(url=util.to_string(file_path))
                self.log_client.log(DEBUG, 'DownloadPath is ' + util.to_string(file_path))
 
            status = util.to_int(result.status)
            reason = result.reason
            headers = {}
            for k, v in result.getheaders():
                headers[k.lower()] = v
            self.convertor.parseGetObject(headers, body)
            header = self._rename_response_headers(headers)
            requestId = headers.get(self.ha.request_id_header())
            return GetResult(status=status, reason=reason, header=header, body=body, requestId=requestId)
        except _RedirectException as ex:
            raise ex
        except Exception as e:
            self.log_client.log(ERROR, traceback.format_exc())
            raise e
        finally:
            if close_conn_flag:
                util.do_close(result, conn, self.connHolder, self.log_client)
    
    def _get_data(self, result, downloadPath, chuckSize):
        origin_file_path = downloadPath
        if const.IS_WINDOWS:
            downloadPath = util.safe_trans_to_gb2312(downloadPath)
        pathDir = os.path.dirname(downloadPath)
        if not os.path.exists(pathDir):
            os.makedirs(pathDir, 0o755)
        with open(downloadPath, 'wb') as f:
            while True:
                chunk = result.read(chuckSize)
                if not chunk:
                    break
                f.write(chunk)
        return origin_file_path
    
    def _rename_key(self, k, v, header_prefix, meta_header_prefix):
        flag = 0
        if k.startswith(meta_header_prefix):
            k = k[k.index(meta_header_prefix) + len(meta_header_prefix):]
            k = util.decode_item(k)
            v = util.decode_item(v)
            flag = 1
        elif k.startswith(header_prefix):
            k = k[k.index(header_prefix) + len(header_prefix):]
            v = util.decode_item(v)
            flag = 1
        elif k.startswith(const.OBS_META_HEADER_PREFIX):
            k = k[k.index(const.OBS_META_HEADER_PREFIX) + len(const.OBS_META_HEADER_PREFIX):]
            k = util.decode_item(k)
            v = util.decode_item(v)
            flag = 1
        elif k.startswith(const.OBS_HEADER_PREFIX):
            k = k[k.index(const.OBS_HEADER_PREFIX) + len(const.OBS_HEADER_PREFIX):]
            v = util.decode_item(v)
            flag = 1
        return flag, k, v
    
    def _rename_response_headers(self, headers):
        header = []
        header_prefix = self.ha._get_header_prefix()
        meta_header_prefix = self.ha._get_meta_header_prefix()
        for k, v in headers.items():
            flag = 0
            if k in const.ALLOWED_RESPONSE_HTTP_HEADER_METADATA_NAMES:
                flag = 1
            else:
                flag, k, v = self._rename_key(k, v, header_prefix, meta_header_prefix)
            if flag:
                header.append((k, v))
        return header
    
    def _parse_xml_internal(self, result, methodName=None, chuckSize=65536, readable=False):
        status = util.to_int(result.status)
        reason = result.reason
        code = None
        message = None
        body = None
        requestId = None
        hostId = None
        resource = None
        headers = {}
        for k, v in result.getheaders():
            headers[k.lower()] = v
        
        xml = None
        while True:
            chunk = result.read(chuckSize)
            if not chunk:
                break
            xml = chunk if xml is None else xml + chunk
         
        if status >= 300 and status < 400 and status != 304 and not readable and const.LOCATION_HEADER.lower() in headers:
            location = headers.get(const.LOCATION_HEADER.lower())
            self.log_client.log(WARNING, 'http code is %d, need to redirect to %s', status, location)
            raise _RedirectException('http code is {0}, need to redirect to {1}'.format(status, location), location)
        else:
            if status < 300:
                if methodName is not None:
                    methodName = 'parse' + methodName[:1].upper() + methodName[1:]
                    parseMethod = getattr(self.convertor, methodName)
                    if parseMethod is not None:
                        if xml:
                            xml = xml if const.IS_PYTHON2 else xml.decode('UTF-8')
                            self.log_client.log(DEBUG, 'recv Msg:%s', xml)
                            try:
                                search = self.pattern.search(xml)
                                xml = xml if search is None else xml.replace(search.group(), '')
                                body = parseMethod(xml, headers)
                            except Exception as e:
                                self.log_client.log(ERROR, e)
                        else:
                            body = parseMethod(headers)
                requestId = headers.get(self.ha.request_id_header())
            elif xml:
                xml = xml if const.IS_PYTHON2 else xml.decode('UTF-8')
                try:
                    search = self.pattern.search(xml)
                    xml = xml if search is None else xml.replace(search.group(), '')
                    code, message, requestId, hostId, resource = self.convertor.parseErrorResult(xml)
                except Exception as ee:
                    self.log_client.log(ERROR, util.to_string(ee))
                    self.log_client.log(ERROR, traceback.format_exc())
                    
        header = self._rename_response_headers(headers)
        self.log_client.log(DEBUG, 'http response result:status:%d,reason:%s,code:%s,message:%s,headers:%s', status, reason, code,
                            message, header)
 
        return GetResult(code=code, message=message, status=status, reason=reason, body=body, 
                         requestId=requestId, hostId=hostId, resource=resource, header=header)

class _CreateSignedUrlResponse(BaseModel):
    allowedAttr = {'signedUrl': const.BASESTRING, 'actualSignedRequestHeaders': dict}
    

class _CreatePostSignatureResponse(BaseModel):
    allowedAttr = {'originPolicy': const.BASESTRING, 'policy': const.BASESTRING, 
                   'credential': const.BASESTRING, 'date': const.BASESTRING, 'signature': const.BASESTRING, 'accessKeyId': const.BASESTRING}
    
class ObsClient(_BasicClient):
    
    def __init__(self, *args, **kwargs):
        super(ObsClient, self).__init__(*args, **kwargs)
    
    def _prepareParameterForSignedUrl(self, specialParam, expires, headers, queryParams):
        
        headers = {} if headers is None or not isinstance(headers, dict) else headers.copy()
        queryParams = {} if queryParams is None or not isinstance(queryParams, dict) else queryParams.copy()

        if specialParam is not None:
            specialParam = 'storageClass' if self.signature.lower() == 'obs' and specialParam == 'storagePolicy' else 'storagePolicy' if self.signature.lower() != 'obs' and specialParam == 'storageClass' else specialParam
            queryParams[specialParam] = None

        expires = 300 if expires is None else util.to_int(expires)

        return headers, queryParams, expires, self.calling_format
    
    def createSignedUrl(self, method, bucketName=None, objectKey=None, specialParam=None, expires=300, headers=None, queryParams=None):
        delegate = self._createV4SignedUrl if self.signature.lower() == 'v4' else self._createV2SignedUrl
        return delegate(method, bucketName, objectKey, specialParam, expires, headers, queryParams)
    
    def createV2SignedUrl(self, method, bucketName=None, objectKey=None, specialParam=None, expires=300, headers=None, queryParams=None):
        return self._createV2SignedUrl(method, bucketName, objectKey, specialParam, expires, headers, queryParams)
    
    def createV4SignedUrl(self, method, bucketName=None, objectKey=None, specialParam=None, expires=300, headers=None, queryParams=None):
        return self._createV4SignedUrl(method, bucketName, objectKey, specialParam, expires, headers, queryParams)
    
    def _createV2SignedUrl(self, method, bucketName=None, objectKey=None, specialParam=None, expires=300, headers=None, queryParams=None):

        headers, queryParams, expires, calling_format = self._prepareParameterForSignedUrl(specialParam, expires, headers, queryParams)
        
        headers[const.HOST_HEADER] = calling_format.get_server(self.server, bucketName)
        
        if self.port != 443 and self.port != 80:
            headers[const.HOST_HEADER] = headers[const.HOST_HEADER] + util.to_string(self.port)
        
        expires += util.to_int(time.time())

        securityProvider = self.securityProvider
        if securityProvider.security_token is not None and self.ha.security_token_header() not in headers:
            headers[self.ha.security_token_header()] = securityProvider.security_token

        v2Auth = auth.Authentication(securityProvider.access_key_id, securityProvider.secret_access_key, self.path_style, self.ha)

        signature = v2Auth.getSignature(method, bucketName, objectKey, queryParams, headers, util.to_string(expires))['Signature']

        queryParams['Expires'] = expires
        queryParams['AccessKeyId' if self.signature == 'obs' else 'AWSAccessKeyId'] = securityProvider.access_key_id
        queryParams['Signature'] = signature
        
        result = {
            'signedUrl': calling_format.get_full_url(self.is_secure, self.server, self.port, bucketName, objectKey, queryParams),
            'actualSignedRequestHeaders': headers
        }

        return _CreateSignedUrlResponse(**result)
    
    def _createV4SignedUrl(self, method, bucketName=None, objectKey=None, specialParam=None, expires=300, headers=None, queryParams=None):
        from datetime import datetime

        headers, queryParams, expires, calling_format = self._prepareParameterForSignedUrl(specialParam, expires, headers, queryParams)

        headers[const.HOST_HEADER] = calling_format.get_server(self.server, bucketName)
        
        if self.port != 443 and self.port != 80:
            headers[const.HOST_HEADER] = headers[const.HOST_HEADER] + util.to_string(self.port)

        date = headers[const.DATE_HEADER] if const.DATE_HEADER in headers else headers.get(const.DATE_HEADER.lower())
        date = datetime.strptime(date, const.GMT_DATE_FORMAT) if date else datetime.utcnow()
        shortDate = date.strftime(const.SHORT_DATE_FORMAT)
        longDate = date.strftime(const.LONG_DATE_FORMAT)

        securityProvider = self.securityProvider

        if securityProvider.security_token is not None and self.ha.security_token_header() not in headers:
            headers[self.ha.security_token_header()] = securityProvider.security_token

        v4Auth = auth.V4Authentication(securityProvider.access_key_id, securityProvider.secret_access_key, self.region, shortDate, longDate, self.path_style, self.ha)

        queryParams['X-Amz-Algorithm'] = 'AWS4-HMAC-SHA256'
        queryParams['X-Amz-Credential'] = v4Auth.getCredenttial()
        queryParams['X-Amz-Date'] = longDate
        queryParams['X-Amz-Expires'] = expires
        
        headMap = v4Auth.setMapKeyLower(headers)
        signedHeaders = v4Auth.getSignedHeaders(headMap)
        
        queryParams['X-Amz-SignedHeaders'] = signedHeaders
        
        signature = v4Auth.getSignature(method, bucketName, objectKey, queryParams, headMap, signedHeaders, 'UNSIGNED-PAYLOAD')['Signature']

        queryParams['X-Amz-Signature'] = signature

        result = {
            'signedUrl': calling_format.get_full_url(self.is_secure, self.server, self.port, bucketName, objectKey, queryParams),
            'actualSignedRequestHeaders': headers
        }

        return _CreateSignedUrlResponse(**result)
    
    def createV4PostSignature(self, bucketName=None, objectKey=None, expires=300, formParams=None):
        return self._createPostSignature(bucketName, objectKey, expires, formParams, True)
    
    def createPostSignature(self, bucketName=None, objectKey=None, expires=300, formParams=None):
        return self._createPostSignature(bucketName, objectKey, expires, formParams, self.signature.lower() == 'v4')
    
    def _createPostSignature(self, bucketName=None, objectKey=None, expires=300, formParams=None, is_v4=False):
        from datetime import datetime, timedelta

        date = datetime.utcnow()
        shortDate = date.strftime(const.SHORT_DATE_FORMAT)
        longDate = date.strftime(const.LONG_DATE_FORMAT)
        securityProvider = self.securityProvider

        expires = 300 if expires is None else util.to_int(expires)
        expires = date + timedelta(seconds=expires)

        expires = expires.strftime(const.EXPIRATION_DATE_FORMAT)

        formParams = {} if formParams is None or not isinstance(formParams, dict) else formParams.copy()

        if securityProvider.security_token is not None and self.ha.security_token_header() not in formParams:
            formParams[self.ha.security_token_header()] = securityProvider.security_token
        
        if is_v4:
            formParams['X-Amz-Algorithm'] = 'AWS4-HMAC-SHA256'
            formParams['X-Amz-Date'] = longDate
            formParams['X-Amz-Credential'] = '%s/%s/%s/s3/aws4_request' % (securityProvider.access_key_id, shortDate, self.region)

        if bucketName:
            formParams['bucket'] = bucketName

        if objectKey:
            formParams['key'] = objectKey

        policy = ['{"expiration":"']
        policy.append(expires)
        policy.append('", "conditions":[')

        matchAnyBucket = True
        matchAnyKey = True

        conditionAllowKeys = ['acl', 'bucket', 'key', 'success_action_redirect', 'redirect', 'success_action_status']

        for key, value in formParams.items():
            if key:
                key = util.to_string(key).lower()

                if key == 'bucket':
                    matchAnyBucket = False
                elif key == 'key':
                    matchAnyKey = False

                if key not in const.ALLOWED_REQUEST_HTTP_HEADER_METADATA_NAMES and not key.startswith(self.ha._get_header_prefix()) and not key.startswith(const.OBS_HEADER_PREFIX) and key not in conditionAllowKeys:
                    continue

                policy.append('{"')
                policy.append(key)
                policy.append('":"')
                policy.append(util.to_string(value) if value is not None else '')
                policy.append('"},')

        if matchAnyBucket:
            policy.append('["starts-with", "$bucket", ""],')

        if matchAnyKey:
            policy.append('["starts-with", "$key", ""],')

        policy.append(']}')

        originPolicy = ''.join(policy)

        policy = util.base64_encode(originPolicy)
        
        if is_v4:
            v4Auth = auth.V4Authentication(securityProvider.access_key_id, securityProvider.secret_access_key, self.region, shortDate, longDate,
                                           self.path_style, self.ha)
            signingKey = v4Auth.getSigningKey_python2() if const.IS_PYTHON2 else v4Auth.getSigningKey_python3()
            signature = v4Auth.hmacSha256(signingKey, policy if const.IS_PYTHON2 else policy.encode('UTF-8'))
            result = {'originPolicy': originPolicy, 'policy': policy, 'algorithm': formParams['X-Amz-Algorithm'], 'credential': formParams['X-Amz-Credential'], 'date': formParams['X-Amz-Date'], 'signature': signature}
        else:
            v2Auth = auth.Authentication(securityProvider.access_key_id, securityProvider.secret_access_key, self.path_style, self.ha)
            signature = v2Auth.hmacSha128(policy)
            result = {'originPolicy': originPolicy, 'policy': policy, 'signature': signature, 'accessKeyId': securityProvider.access_key_id}
        return _CreatePostSignatureResponse(**result)

    def bucketClient(self, bucketName):
        return BucketClient(self, bucketName)
    
    @count_time
    def listBuckets(self, isQueryLocation=True):
        return self._make_get_request(methodName='listBuckets', **self.convertor.trans_list_buckets(isQueryLocation=isQueryLocation))
    
    @count_time
    def createBucket(self, bucketName, header=CreateBucketHeader(), location=None):
        return self._make_put_request(bucketName, **self.convertor.trans_create_bucket(header=header, location=location))

    @count_time
    def listObjects(self, bucketName, prefix=None, marker=None, max_keys=None, delimiter=None):
        return self._make_get_request(bucketName, methodName='listObjects', **self.convertor.trans_list_objects(prefix=prefix, marker=marker, max_keys=max_keys, delimiter=delimiter))

    @count_time
    def headBucket(self, bucketName):
        return self._make_head_request(bucketName)

    @count_time
    def getBucketMetadata(self, bucketName, origin=None, requestHeaders=None):
        return self._make_head_request(bucketName, methodName='getBucketMetadata', **self.convertor.trans_get_bucket_metadata(origin=origin, requestHeaders=requestHeaders))

    @count_time
    def getBucketLocation(self, bucketName):
        return self._make_get_request(bucketName, pathArgs={'location':None}, methodName='getBucketLocation')

    @count_time
    def deleteBucket(self, bucketName):
        return self._make_delete_request(bucketName)

    @count_time
    def setBucketQuota(self, bucketName, quota):
        self._assert_not_null(quota, 'quota is empty')
        return self._make_put_request(bucketName, pathArgs={'quota': None}, entity=self.convertor.trans_quota(quota))

    @count_time
    def getBucketQuota(self, bucketName):
        return self._make_get_request(bucketName, pathArgs={'quota': None}, methodName='getBucketQuota')

    @count_time
    def getBucketStorageInfo(self, bucketName):
        return self._make_get_request(bucketName, pathArgs={'storageinfo': None}, methodName='getBucketStorageInfo')

    @count_time
    def setBucketAcl(self, bucketName, acl=ACL(), aclControl=None):
        if acl is not None and len(acl) > 0 and aclControl is not None:
            raise Exception('Both acl and aclControl are set')
        return self._make_put_request(bucketName, **self.convertor.trans_set_bucket_acl(acl=acl, aclControl=aclControl))

    @count_time
    def getBucketAcl(self, bucketName):
        return self._make_get_request(bucketName, pathArgs={'acl': None}, methodName='getBucketAcl')

    @count_time
    def setBucketPolicy(self, bucketName, policyJSON):
        self._assert_not_null(policyJSON, 'policyJSON is empty')
        return self._make_put_request(bucketName, pathArgs={'policy' : None}, entity=policyJSON)

    @count_time
    def getBucketPolicy(self, bucketName):
        return self._make_get_request(bucketName, pathArgs={'policy' : None}, methodName='getBucketPolicy')

    @count_time
    def deleteBucketPolicy(self, bucketName):
        return self._make_delete_request(bucketName, pathArgs={'policy' : None})

    @count_time
    def setBucketVersioning(self, bucketName, status):
        self._assert_not_null(status, 'status is empty')
        return self._make_put_request(bucketName, pathArgs={'versioning' : None}, entity=self.convertor.trans_version_status(status))

    @count_time
    def getBucketVersioning(self, bucketName):
        return self._make_get_request(bucketName, pathArgs={'versioning' : None}, methodName='getBucketVersioning')

    @count_time
    def listVersions(self, bucketName, version=Versions()):
        return self._make_get_request(bucketName, methodName='listVersions', **self.convertor.trans_list_versions(version=version))

    @count_time
    def listMultipartUploads(self, bucketName, multipart=ListMultipartUploadsRequest()):
        return self._make_get_request(bucketName, methodName='listMultipartUploads', **self.convertor.trans_list_multipart_uploads(multipart=multipart))

    @count_time
    def deleteBucketLifecycle(self, bucketName):
        return self._make_delete_request(bucketName, pathArgs={'lifecycle':None})

    @count_time
    def setBucketLifecycle(self, bucketName, lifecycle):
        self._assert_not_null(lifecycle, 'lifecycle is empty')
        return self._make_put_request(bucketName, **self.convertor.trans_set_bucket_lifecycle(lifecycle=lifecycle))

    @count_time
    def getBucketLifecycle(self, bucketName):
        return self._make_get_request(bucketName, pathArgs={'lifecycle':None}, methodName='getBucketLifecycle')

    @count_time
    def deleteBucketWebsite(self, bucketName):
        return self._make_delete_request(bucketName, pathArgs={'website':None})

    @count_time
    def setBucketWebsite(self, bucketName, website):
        self._assert_not_null(website, 'website is empty')
        return self._make_put_request(bucketName, pathArgs={'website':None}, entity=self.convertor.trans_website(website))

    @count_time
    def getBucketWebsite(self, bucketName):
        return self._make_get_request(bucketName, pathArgs={'website':None}, methodName='getBucketWebsite')

    @count_time
    def setBucketLogging(self, bucketName, logstatus=Logging()):
        if logstatus is None:
            logstatus = Logging()
        return self._make_put_request(bucketName, pathArgs={'logging':None}, entity=self.convertor.trans_logging(logstatus))

    @count_time
    def getBucketLogging(self, bucketName):
        return self._make_get_request(bucketName, pathArgs={'logging':None}, methodName='getBucketLogging')

    @count_time
    def getBucketTagging(self, bucketName):
        return self._make_get_request(bucketName, pathArgs={'tagging' : None}, methodName='getBucketTagging')

    @count_time
    def setBucketTagging(self, bucketName, tagInfo):
        self._assert_not_null(tagInfo, 'tagInfo is empty')
        return self._make_put_request(bucketName, **self.convertor.trans_set_bucket_tagging(tagInfo=tagInfo))

    @count_time
    def deleteBucketTagging(self, bucketName):
        return self._make_delete_request(bucketName, pathArgs={'tagging' : None})

    @count_time
    def setBucketCors(self, bucketName, corsRuleList):
        self._assert_not_null(corsRuleList, 'corsRuleList is empty')
        return self._make_put_request(bucketName, **self.convertor.trans_set_bucket_cors(corsRuleList=corsRuleList))

    @count_time
    def deleteBucketCors(self, bucketName):
        return self._make_delete_request(bucketName, pathArgs={'cors' : None})

    @count_time
    def getBucketCors(self, bucketName):
        return self._make_get_request(bucketName, pathArgs={'cors': None}, methodName='getBucketCors')

    @count_time
    def optionsBucket(self, bucketName, option):
        return self.optionsObject(bucketName, None, option=option)

    @count_time
    def setBucketNotification(self, bucketName, notification=Notification()):
        if notification is None:
            notification = Notification()
        return self._make_put_request(bucketName, pathArgs={'notification': None}, entity=self.convertor.trans_notification(notification))

    @count_time
    def getBucketNotification(self, bucketName):
        return self._make_get_request(bucketName, pathArgs={'notification': None}, methodName='getBucketNotification')
    
    
    @count_time
    def optionsObject(self, bucketName, objectKey, option):
        headers = {}
        if option is not None:
            if option.get('origin') is not None:
                headers[const.ORIGIN_HEADER] = util.to_string(option['origin'])
            if option.get('accessControlRequestMethods') is not None:
                headers[const.ACCESS_CONTROL_REQUEST_METHOD_HEADER] = option['accessControlRequestMethods']
            if option.get('accessControlRequestHeaders') is not None:
                headers[const.ACCESS_CONTROL_REQUEST_HEADERS_HEADER] = option['accessControlRequestHeaders']
        return self._make_options_request(bucketName, objectKey, headers=headers, methodName='optionsBucket')

    @count_time
    def getObjectMetadata(self, bucketName, objectKey, versionId=None, sseHeader=None, origin=None, requestHeaders=None):
        pathArgs = {}
        if versionId:
            pathArgs[const.VERSION_ID_PARAM] = util.to_string(versionId)
        headers = {}
        if origin:
            headers[const.ORIGIN_HEADER] = util.to_string(origin)
        _requestHeaders = requestHeaders[0] if isinstance(requestHeaders, list) and len(requestHeaders) == 1 else requestHeaders
        if _requestHeaders:
            headers[const.ACCESS_CONTROL_REQUEST_HEADERS_HEADER] = util.to_string(_requestHeaders)
        return self._make_head_request(bucketName, objectKey, pathArgs=pathArgs, 
                                       headers=self.convertor._set_sse_header(sseHeader, headers=headers, onlySseCHeader=True), methodName='getObjectMetadata')

    @count_time
    def getObject(self, bucketName, objectKey, downloadPath=None, getObjectRequest=GetObjectRequest(), headers=GetObjectHeader(), loadStreamInMemory=False):
        _parse_content = self._parse_content
        CHUNKSIZE = self.chunk_size
        def parseMethod(conn):
            return _parse_content(conn, objectKey, downloadPath, CHUNKSIZE, loadStreamInMemory)
        
        return self._make_get_request(bucketName, objectKey, parseMethod=parseMethod, **self.convertor.trans_get_object(getObjectRequest=getObjectRequest, headers=headers))
    
    @count_time
    def appendObject(self, bucketName, objectKey, content=None, metadata=None, headers=None):
        objectKey = util.safe_encode(objectKey)
        if objectKey is None:
            objectKey = ''
            
        if headers is None:
            headers = AppendObjectHeader()
            
        if content is None:
            content = AppendObjectContent()
            
        if headers.get('contentType') is None:
            headers['contentType'] = const.MIME_TYPES.get(objectKey[objectKey.rfind('.') + 1:])
        
        chunkedMode = False
        readable = False
        if content.get('isFile'):
            file_path = util.safe_encode(content.get('content'))
            if not os.path.exists(file_path):
                file_path = util.safe_trans_to_gb2312(file_path)
                if not os.path.exists(file_path):
                    raise Exception('file [%s] does not exist' % file_path)
            
            if headers.get('contentType') is None:
                headers['contentType'] = const.MIME_TYPES.get(file_path[file_path.rfind('.') + 1:])
            
            file_size = util.to_long(os.path.getsize(file_path))
            headers['contentLength'] = util.to_long(headers.get('contentLength'))
            headers['contentLength'] = headers['contentLength'] if headers.get('contentLength') is not None and headers['contentLength'] <= file_size else file_size
            offset = util.to_long(content.get('offset'))
            if offset is not None and 0 < offset < file_size:
                headers['contentLength'] = headers['contentLength'] if 0 < headers['contentLength'] <= (file_size - offset) else file_size - offset
                entity = util.get_file_entity_by_offset_partsize(file_path, offset, headers['contentLength'], self.chunk_size)
            else:
                entity = util.get_file_entity_by_totalcount(file_path, headers['contentLength'], self.chunk_size)
            headers = self.convertor.trans_put_object(metadata=metadata, headers=headers)
            self.log_client.log(DEBUG, 'send Path:%s' % file_path)
        else:
            entity = content.get('content')
            if entity is None:
                entity = ''
            elif hasattr(entity, 'read') and callable(entity.read):
                readable = True
                if headers.get('contentLength') is None:
                    chunkedMode = True
                    self.log_client.log(DEBUG, 'missing content-length when uploading a readable stream')
                entity = util.get_readable_entity(entity, self.chunk_size) if chunkedMode else util.get_readable_entity_by_totalcount(entity, util.to_long(headers['contentLength']), self.chunk_size)
            headers = self.convertor.trans_put_object(metadata=metadata, headers=headers)
        ret = self._make_post_request(bucketName, objectKey, pathArgs={'append': None, 'position': util.to_string(content['position']) if content.get('position') is not None else 0}, 
                                       headers=headers, entity=entity, chunkedMode=chunkedMode, methodName='appendObject', readable=readable)
        self._generate_object_url(ret, bucketName, objectKey)
        return ret
    
    @count_time
    def putContent(self, bucketName, objectKey, content=None, metadata=None, headers=None):
        objectKey = util.safe_encode(objectKey)
        if objectKey is None:
            objectKey = ''
        if headers is None:
            headers = PutObjectHeader()
        if headers.get('contentType') is None:
            headers['contentType'] = const.MIME_TYPES.get(objectKey[objectKey.rfind('.') + 1:])
        _headers = self.convertor.trans_put_object(metadata=metadata, headers=headers)
        
        readable = False
        chunkedMode = False
        
        entity = content
        if entity is None:
            entity = ''
        elif hasattr(entity, 'read') and callable(entity.read):
            readable = True
            if headers.get('contentLength') is None:
                chunkedMode = True
                self.log_client.log(DEBUG, 'missing content-length when uploading a readable stream')
            entity = util.get_readable_entity(entity, self.chunk_size) if chunkedMode else util.get_readable_entity_by_totalcount(entity, util.to_long(headers.get('contentLength')), self.chunk_size)
                        
        ret = self._make_put_request(bucketName, objectKey, headers=_headers, entity=entity, chunkedMode=chunkedMode, methodName='putContent', readable=readable)
        self._generate_object_url(ret, bucketName, objectKey)
        return ret

    def putObject(self, bucketName, objectKey, content, metadata=None, headers=None):
        return self.putContent(bucketName, objectKey, content, metadata, headers)

    @count_time
    def putFile(self, bucketName, objectKey, file_path, metadata=None, headers=None):
        file_path = util.safe_encode(file_path)
        if not os.path.exists(file_path):
            file_path = util.safe_trans_to_gb2312(file_path)
            if not os.path.exists(file_path):
                raise Exception('file [{0}] doesnot exist'.format(file_path))

        _flag = os.path.isdir(file_path)

        if headers is None:
            headers = PutObjectHeader()

        if _flag:
            headers['contentLength'] = None
            headers['md5'] = None
            headers['contentType'] = None

            results = []
            for f in os.listdir(file_path):  # windows中文文件路径
                f = util.safe_encode(f)
                __file_path = os.path.join(file_path, f)
                if not objectKey:
                    key = util.safe_trans_to_gb2312('{0}/'.format(os.path.split(file_path)[1]) + f)
                else:
                    key = '{0}/'.format(objectKey) + util.safe_trans_to_gb2312(f)
                result = self.putFile(bucketName, key, __file_path, metadata, headers)
                results.append((key, result))
            return results

        if not objectKey:
            objectKey = os.path.split(file_path)[1]

        size = util.to_long(os.path.getsize(file_path))
        headers['contentLength'] = util.to_long(headers.get('contentLength'))
        if headers.get('contentLength') is not None:
            headers['contentLength'] = size if headers['contentLength'] > size else headers['contentLength']

        if headers.get('contentType') is None:
            headers['contentType'] = const.MIME_TYPES.get(objectKey[objectKey.rfind('.') + 1:])

        if headers.get('contentType') is None:
            headers['contentType'] = const.MIME_TYPES.get(file_path[file_path.rfind('.') + 1:])

        _headers = self.convertor.trans_put_object(metadata=metadata, headers=headers)
        if const.CONTENT_LENGTH_HEADER not in _headers:
            _headers[const.CONTENT_LENGTH_HEADER] = util.to_string(size)
        self.log_client.log(DEBUG, 'send Path:%s' % file_path)

        entity = util.get_file_entity_by_totalcount(file_path, util.to_long(headers['contentLength']), self.chunk_size) if headers.get('contentLength') is not None else util.get_file_entity(file_path, self.chunk_size)
        
        ret = self._make_put_request(bucketName, objectKey, headers=_headers, entity=entity, methodName='putContent')
        self._generate_object_url(ret, bucketName, objectKey)
        return ret
    
    @count_time
    def uploadPart(self, bucketName, objectKey, partNumber, uploadId, object=None, isFile=False, partSize=None, 
                   offset=0, sseHeader=None, isAttachMd5=False, md5=None, content=None):
        self._assert_not_null(partNumber, 'partNumber is empty')
        self._assert_not_null(uploadId, 'uploadId is empty')
        
        if content is None:
            content = object
        
        chunkedMode = False
        readable = False
        if isFile:
            file_path = util.safe_encode(content)
            if not os.path.exists(file_path):
                file_path = util.safe_trans_to_gb2312(file_path)
                if not os.path.exists(file_path):
                    raise Exception('file [%s] does not exist' % file_path)
            file_size = util.to_long(os.path.getsize(file_path))
            offset = util.to_long(offset)
            offset = offset if offset is not None and 0 <= offset < file_size else 0
            partSize = util.to_long(partSize)
            partSize = partSize if partSize is not None and 0 < partSize <= (file_size - offset) else file_size - offset

            headers = {const.CONTENT_LENGTH_HEADER : util.to_string(partSize)}

            if md5:
                headers[const.CONTENT_MD5_HEADER] = md5
            elif isAttachMd5:
                headers[const.CONTENT_MD5_HEADER] = util.base64_encode(util.md5_file_encode_by_size_offset(file_path, partSize, offset, self.chunk_size))

            if sseHeader is not None:
                self.convertor._set_sse_header(sseHeader, headers, True)
            
            entity = util.get_file_entity_by_offset_partsize(file_path, offset, partSize, self.chunk_size)    
        else:
            headers = {}
            if content is not None and hasattr(content, 'read') and callable(content.read):
                readable = True
                if md5:
                    headers[const.CONTENT_MD5_HEADER] = md5
                if sseHeader is not None:
                    self.convertor._set_sse_header(sseHeader, headers, True)

                if partSize is None:
                    self.log_client.log(DEBUG, 'missing partSize when uploading a readable stream')
                    chunkedMode = True
                    entity = util.get_readable_entity(content, self.chunk_size)
                else:
                    headers[const.CONTENT_LENGTH_HEADER] = util.to_string(partSize)
                    entity = util.get_readable_entity_by_totalcount(content, util.to_long(partSize), self.chunk_size)
            else:
                entity = content
                if entity is None:
                    entity = ''
                if md5:
                    headers[const.CONTENT_MD5_HEADER] = md5
                if sseHeader:
                    self.convertor._set_sse_header(sseHeader, headers, True)
        
        return self._make_put_request(bucketName, objectKey, pathArgs={'partNumber': partNumber, 'uploadId': uploadId}, 
                                      headers=headers, entity=entity, chunkedMode=chunkedMode, methodName='uploadPart', readable=readable)

    @count_time
    def copyObject(self, sourceBucketName, sourceObjectKey, destBucketName, destObjectKey, metadata=None, headers=CopyObjectHeader(), versionId=None):
        self._assert_not_null(sourceBucketName, 'sourceBucketName is empty')
        sourceObjectKey = util.safe_encode(sourceObjectKey)
        if sourceObjectKey is None:
            sourceObjectKey = ''
        destObjectKey = util.safe_encode(destObjectKey)
        if destObjectKey is None:
            destObjectKey = ''        
        return self._make_put_request(destBucketName, destObjectKey, 
                                      methodName='copyObject', **self.convertor.trans_copy_object(metadata=metadata, headers=headers, versionId=versionId,
                                                                                                  sourceBucketName=sourceBucketName, sourceObjectKey=sourceObjectKey))

    @count_time
    def setObjectAcl(self, bucketName, objectKey, acl=ACL(), versionId=None, aclControl=None):
        if acl is not None and len(acl) > 0 and aclControl is not None:
            raise Exception('Both acl and aclControl are set')
        return self._make_put_request(bucketName, objectKey, **self.convertor.trans_set_object_acl(acl=acl, versionId=versionId, aclControl=aclControl))


    @count_time
    def getObjectAcl(self, bucketName, objectKey, versionId=None):
        pathArgs = {'acl': None}
        if versionId:
            pathArgs[const.VERSION_ID_PARAM] = util.to_string(versionId)

        return self._make_get_request(bucketName, objectKey, pathArgs=pathArgs, methodName='getObjectAcl')

    @count_time
    def deleteObject(self, bucketName, objectKey, versionId=None):
        path_args = {}
        if versionId:
            path_args[const.VERSION_ID_PARAM] = util.to_string(versionId)
        return self._make_delete_request(bucketName, objectKey, pathArgs=path_args, methodName='deleteObject')

    @count_time
    def deleteObjects(self, bucketName, deleteObjectsRequest):
        self._assert_not_null(deleteObjectsRequest, 'deleteObjectsRequest is empty')
        return self._make_post_request(bucketName, methodName='deleteObjects', **self.convertor.trans_delete_objects(deleteObjectsRequest=deleteObjectsRequest))

    @count_time
    def restoreObject(self, bucketName, objectKey, days, tier=None, versionId=None):
        self._assert_not_null(days, 'days is empty')
        return self._make_post_request(bucketName, objectKey, **self.convertor.trans_restore_object(days=days, tier=tier, versionId=versionId))

    @count_time
    def initiateMultipartUpload(self, bucketName, objectKey, acl=None, storageClass=None, 
                                metadata=None, websiteRedirectLocation=None, contentType=None, sseHeader=None, expires=None, extensionGrants=None):
        objectKey = util.safe_encode(objectKey)
        if objectKey is None:
            objectKey = ''
        
        if contentType is None:
            contentType = const.MIME_TYPES.get(objectKey[objectKey.rfind('.') + 1:])
        
        return self._make_post_request(bucketName, objectKey, methodName='initiateMultipartUpload', 
                                       **self.convertor.trans_initiate_multipart_upload(acl=acl, storageClass=storageClass, 
                                                                                        metadata=metadata, websiteRedirectLocation=websiteRedirectLocation,
                                                                                        contentType=contentType, sseHeader=sseHeader, expires=expires, extensionGrants=extensionGrants))

    @count_time
    def copyPart(self, bucketName, objectKey, partNumber, uploadId, copySource, copySourceRange=None, destSseHeader=None, sourceSseHeader=None):
        self._assert_not_null(partNumber, 'partNumber is empty')
        self._assert_not_null(uploadId, 'uploadId is empty')
        self._assert_not_null(copySource, 'copySource is empty')
        
        return self._make_put_request(bucketName, objectKey, methodName='copyPart', **self.convertor.trans_copy_part(partNumber=partNumber, uploadId=uploadId, copySource=copySource, 
                                                                                                                     copySourceRange=copySourceRange, destSseHeader=destSseHeader, sourceSseHeader=sourceSseHeader))

    @count_time
    def completeMultipartUpload(self, bucketName, objectKey, uploadId, completeMultipartUploadRequest):
        self._assert_not_null(uploadId, 'uploadId is empty')
        self._assert_not_null(completeMultipartUploadRequest, 'completeMultipartUploadRequest is empty')

        ret = self._make_post_request(bucketName, objectKey, pathArgs={'uploadId':uploadId},
                                       entity=self.convertor.trans_complete_multipart_upload_request(completeMultipartUploadRequest), methodName='completeMultipartUpload')
        self._generate_object_url(ret, bucketName, objectKey)
        return ret

    @count_time
    def abortMultipartUpload(self, bucketName, objectKey, uploadId):
        self._assert_not_null(uploadId, 'uploadId is empty')
        return self._make_delete_request(bucketName, objectKey, pathArgs={'uploadId' : uploadId})

    @count_time
    def listParts(self, bucketName, objectKey, uploadId, maxParts=None, partNumberMarker=None):
        self._assert_not_null(uploadId, 'uploadId is empty')
        pathArgs = {'uploadId': uploadId}
        if maxParts is not None:
            pathArgs['max-parts'] = maxParts
        if partNumberMarker is not None:
            pathArgs['part-number-marker'] = partNumberMarker
        return self._make_get_request(bucketName, objectKey, pathArgs=pathArgs, methodName='listParts')

    @count_time
    def getBucketStoragePolicy(self, bucketName):
        return self._make_get_request(bucketName, methodName='getBucketStoragePolicy', **self.convertor.trans_get_bucket_storage_policy())

    @count_time
    def setBucketStoragePolicy(self, bucketName, storageClass):
        self._assert_not_null(storageClass, 'storageClass is empty')
        return self._make_put_request(bucketName, **self.convertor.trans_set_bucket_storage_policy(storageClass=storageClass))

    @count_time
    def setBucketReplication(self, bucketName, replication):
        self._assert_not_null(replication, 'replication is empty')
        return self._make_put_request(bucketName, **self.convertor.trans_set_bucket_replication(replication=replication))

    @count_time
    def getBucketReplication(self, bucketName):
        return self._make_get_request(bucketName, pathArgs={'replication':None}, methodName='getBucketReplication')
    
    @count_time
    def deleteBucketReplication(self, bucketName):
        return self._make_delete_request(bucketName, pathArgs={'replication':None})

    @count_time
    def uploadFile(self, bucketName, objectKey, uploadFile, partSize=5*1024*1024, taskNum=1, enableCheckpoint=False,
                   checkpointFile=None, checkSum=False, metadata=None):
        self.log_client.log(INFO, 'enter resume upload...')
        self._assert_not_null(bucketName, 'bucketName is empty')
        self._assert_not_null(objectKey, 'objectKey is empty')
        self._assert_not_null(uploadFile, 'uploadFile is empty')
        if enableCheckpoint and checkpointFile is None:
            checkpointFile = uploadFile + '.upload_record'

        if partSize < const.DEFAULT_MINIMUM_SIZE:
            partSize = const.DEFAULT_MINIMUM_SIZE
        elif partSize > const.DEFAULT_MAXIMUM_SIZE:
            partSize = const.DEFAULT_MAXIMUM_SIZE
        else:
            partSize = util.to_int(partSize)
        if taskNum <= 0:
            taskNum = 1
        else:
            import math
            taskNum = int(math.ceil(taskNum))
        return _resumer_upload(bucketName, objectKey, uploadFile, partSize, taskNum, enableCheckpoint, checkpointFile, checkSum, metadata, self)

    @count_time
    def downloadFile(self, bucketName, objectKey, downloadFile=None, partSize=5*1024*1024, taskNum=1, enableCheckpoint=False,
                     checkpointFile=None, header=GetObjectHeader(), versionId=None):
        self.log_client.log(INFO, 'enter resume download...')
        self._assert_not_null(bucketName, 'bucketName is empty')
        self._assert_not_null(objectKey, 'objectKey is empty')
        if downloadFile is None:
            downloadFile = objectKey
        if enableCheckpoint and checkpointFile is None:
            checkpointFile = downloadFile+'.download_record'

        if partSize < const.DEFAULT_MINIMUM_SIZE:
            partSize = const.DEFAULT_MINIMUM_SIZE
        elif partSize > const.DEFAULT_MAXIMUM_SIZE:
            partSize = const.DEFAULT_MAXIMUM_SIZE
        else:
            partSize = util.to_int(partSize)
        if taskNum <= 0:
            taskNum = 1
        else:
            taskNum = int(math.ceil(taskNum))
        return _resumer_download(bucketName, objectKey, downloadFile, partSize, taskNum, enableCheckpoint, checkpointFile, header, versionId, self)
    

ObsClient.setBucketVersioningConfiguration = ObsClient.setBucketVersioning
ObsClient.getBucketVersioningConfiguration = ObsClient.getBucketVersioning
ObsClient.deleteBucketLifecycleConfiguration = ObsClient.deleteBucketLifecycle
ObsClient.setBucketLifecycleConfiguration = ObsClient.setBucketLifecycle
ObsClient.getBucketLifecycleConfiguration = ObsClient.getBucketLifecycle
ObsClient.getBucketWebsiteConfiguration = ObsClient.getBucketWebsite
ObsClient.setBucketWebsiteConfiguration = ObsClient.setBucketWebsite
ObsClient.deleteBucketWebsiteConfiguration = ObsClient.deleteBucketWebsite
ObsClient.setBucketLoggingConfiguration = ObsClient.setBucketLogging
ObsClient.getBucketLoggingConfiguration = ObsClient.getBucketLogging


    
 
