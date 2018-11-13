#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import base64
import hashlib
import json
import os
import sys
import time
import random
import string

import functools

import shutil

from obscmd import globl
from obscmd.compat import six, is_windows, safe_encode
from obscmd.exceptions import ParamValidationError
import logging.handlers
from concurrent_log_handler import ConcurrentRotatingFileHandler

"""
obscmd common util function 
"""

def get_files_size(filepaths):
    total = 0
    for filepath in filepaths:
        total += os.path.getsize(filepath)
    return total


class DotDict(dict):
    """dot.notation access to dictionary attributes"""
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


def get_full_path(path):
    if is_windows:
        path.replace('/', '\\')
    fullpath = os.path.expandvars(path)
    fullpath = os.path.expanduser(fullpath)
    fullpath = os.path.abspath(fullpath)
    return fullpath


def write_exception(ex, outfile):
    outfile.write("\n")
    outfile.write(six.text_type(ex))
    outfile.write("\n")


def uni_print(statement, out_file=None):
    """
    This function is used to properly write unicode to a file, usually
    stdout or stdderr.  It ensures that the proper encoding is used if the
    statement is not a string type.
    """
    if out_file is None:
        out_file = sys.stdout
    try:
        # Otherwise we assume that out_file is a
        # text writer type that accepts str/unicode instead
        # of bytes.
        out_file.write(safe_encode(statement))
    except UnicodeEncodeError:
        # Some file like objects like cStringIO will
        # try to decode as ascii on python2.
        #
        # This can also fail if our encoding associated
        # with the text writer cannot encode the unicode
        # ``statement`` we've been given.  This commonly
        # happens on windows where we have some OBS key
        # previously encoded with utf-8 that can't be
        # encoded using whatever codepage the user has
        # configured in their console.
        #
        # At this point we've already failed to do what's
        # been requested.  We now try to make a best effort
        # attempt at printing the statement to the outfile.
        # We're using 'ascii' as the default because if the
        # stream doesn't give us any encoding information
        # we want to pick an encoding that has the highest
        # chance of printing successfully.
        new_encoding = getattr(out_file, 'encoding', 'ascii')
        # When the output of the obscmd command is being piped,
        # ``sys.stdout.encoding`` is ``None``.
        if new_encoding is None:
            new_encoding = 'ascii'
        new_statement = statement.encode(
            new_encoding, 'replace').decode(new_encoding)
        out_file.write(new_statement)
    out_file.flush()


def get_dir_total_size(path):
    """
    get directory size 
    :param path: 
    :return: 
    """
    size = 0
    for root, dirs, files in os.walk(path):
        for file in files:
            size += os.path.getsize(os.path.join(root, file))
    return size


def get_dir_file_num(path):
    # return sum([len(x) for _, _, x in os.walk(os.path.dirname(path))])
    num = 0
    for root, dirs, files in os.walk(path):
        num += len(files)
    return num


def string2md5(content):
    """
    transform string to md5
    :param content: string
    :return: md5
    """
    md5 = hashlib.md5(content.encode('utf-8')).hexdigest()
    return md5


def str_md5_base64(content):
    md5_base64 = base64.b64encode(bytes(content, encoding="utf8"))
    md5 = string2md5(md5_base64)
    # md5_base64 = base64.b64encode(bytes(md5, encoding="utf8"))
    return md5


def file2md5(filepath):
    with open(filepath, 'rb') as fp:
        content = hashlib.md5(fp.read()).hexdigest()
    return string2md5(content)



def random_string(size):
    return ''.join(random.sample(string.ascii_letters + string.digits, size))

POSTFIXES = ('B', 'K', 'M', 'G', 'T', 'P', 'E')


def bytes_to_unitstr(bytes):
    """
    transform 1024 to 1.0k
    2.3*1024*1024 to 2.3M
    :param bytes: 
    :return: 
    """
    base = 1024
    format = '%.1f%s'
    for i, postfix in enumerate(POSTFIXES):
        unit = base ** (i+1)
        if round((1.0 * bytes / unit) * base) < base:
            value = 1.0*bytes / (base**i)
            if i == 0:
                format = '%d%s'

            return format % (value, postfix)


def unitstr_to_bytes(unitstr):
    """
    transform 1M to 1024*1024
    1.2K to int(1.2*1024)
    :param unitstr: 
    :return: 
    """
    digit = unitstr[:-1]
    unit = unitstr[-1].upper()
    if unit.isdigit():
        return int(float(unitstr)) if '.' in unitstr else int(unitstr)
    if unit not in POSTFIXES:
        raise ParamValidationError(**{'report': '%s unit must be in %s' % (unitstr, POSTFIXES)})

    base = 1024
    return int(float(digit) * base ** POSTFIXES.index(unit))


def hour_time_to_int(hour_time):
    """
    translate time 08:23 to 823
    23:01 to 2301    
    :param hour_time: 
    :return: 
    """
    hour, minute = hour_time.split(':')
    time_int = int(hour) * 100 + int(minute)
    return time_int


def get_flowwidth_from_flowpolicy(jsonstr):
    """
    jsonstr like '{"7:30-12:00": "1.1M", "12:00-15:30": "2.5M", "16:00-20:30": "110K", "21:00-06:30": "110M"}'
    current time is 08:00, return int(1.1*1024*1024)
    :param jsonstr: 
    :return: 
    """

    obj = json.loads(jsonstr)
    now_time_int = hour_time_to_int(time.strftime("%H:%M"))

    for key, item in obj.items():
        start_time, end_time = key.split('-')

        start_time_int = hour_time_to_int(start_time)
        end_time_int = hour_time_to_int(end_time)

        if end_time_int < start_time_int:
            end_time_int += 24*100

        if (now_time_int >= start_time_int and now_time_int < end_time_int)\
                or (now_time_int+24*100 >= start_time_int and now_time_int+24*100 < end_time_int):
            return unitstr_to_bytes(item)


def check_value_threshold(value, low, high):
    if value < low:
        value = low
    elif value > high:
        value = high
    return value


def calculate_etag(file_path, part_size=None, chunk_size=None):
    md5s = []
    file_size = os.path.getsize(file_path)
    if file_size == 0:
        return '"{}"'.format(hashlib.md5(b'').hexdigest())
    if not part_size:
        part_size = file_size
    with open(file_path, 'rb') as fp:
        CHUNKSIZE = 65536 if chunk_size is None else chunk_size
        offset = 0
        while offset < file_size:
            fp.seek(offset)
            read_count = 0
            m = hashlib.md5()
            while read_count < part_size:
                read_size = CHUNKSIZE if part_size - read_count >= CHUNKSIZE else part_size - read_count
                data = fp.read(read_size)
                read_count_once = len(data)
                if read_count_once <= 0:
                    break
                m.update(data)
                read_count += read_count_once
            md5s.append(m)
            offset += read_count
    if len(md5s) == 1 :
        return '"{}"'.format(md5s[0].hexdigest())
    digests = b''.join(m.digest() for m in md5s)
    digests_md5 = hashlib.md5(digests)
    return '"{}-{}"'.format(digests_md5.hexdigest(), len(md5s))


# def calculate_etag(file_path, chunk_size=1024**3):
#     md5s = []
#     if os.path.getsize(file_path) == 0:
#         return '"{}"'.format(hashlib.md5(b'').hexdigest())
#
#     with open(file_path, 'rb') as fp:
#         while True:
#             data = fp.read(chunk_size)
#             if not data:
#                 break
#             md5s.append(hashlib.md5(data))
#
#     if len(md5s) == 1:
#         return '"{}"'.format(md5s[0].hexdigest())
#
#     digests = b''.join(m.digest() for m in md5s)
#     digests_md5 = hashlib.md5(digests)
#     return '"{}-{}"'.format(digests_md5.hexdigest(), len(md5s))


def move_file(srcfile, dstfile):
    fpath, fname = os.path.split(dstfile)
    if not os.path.exists(fpath):
        os.makedirs(fpath)
    shutil.move(srcfile, dstfile)


def rename_file(srcfile, dstfile):
    os.rename(srcfile, dstfile)


def count_time(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        ret = func(*args, **kwargs)
        now = time.time()
        timeused = int((now - start) * 1000)
        lock = globl.get_value('lock')
        with lock:
            value = globl.get_value('value')
            value.append((now, timeused))
        return ret
    return wrapper


