#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import json
import signal
import sys
import logging
import logging.handlers
import zipfile
import shutil
import time
import platform
from obscmd import globl, compat
from obscmd.compat import ZIP_COMPRESSION_MODE, Lock, is_windows
from concurrent_log_handler import ConcurrentRotatingFileHandler

from obscmd.argparser import MainArgParser, USAGE
from obscmd.arguments import CustomArgument, UnknownArgumentError
from obscmd.config import *
from obscmd.exceptions import NoCredentialsError, NoRegionError
from obscmd.utils import get_dir_total_size, unitstr_to_bytes
from obs.const import OBS_SDK_VERSION
from obscmd.constant import __version__


LOG_LEVEL = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
}


def main(args=None):

    clidriver = create_clidriver()
    clidriver.main()
    process_logs(clidriver.session.config)
    process_file_list(clidriver.session.config)
    logger = logging.getLogger("obscmd.file")
    logger.info('execute over!')


def quit_hcmd(signum, frame):
    """
    for ctr+c stop multiprocessing or threading and exit
    :param signum: 
    :param frame: 
    :return: 
    """
    globl.set_value_lock('force_exit', True)
    exit(1)


def create_clidriver():

    init_env()
    logger, logfile = init_logger(config)
    session = Session(config, logger)
    session.logfile = logfile
    if not is_windows:
        signal.signal(signal.SIGINT, quit_hcmd)
        signal.signal(signal.SIGTERM, quit_hcmd)
    return CliDriver(session)


def init_env():
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    if not os.path.exists(CP_DIR):
        os.makedirs(CP_DIR)
    if not os.path.exists(FILE_LIST_DIR):
        os.makedirs(FILE_LIST_DIR)
    init_globl()


def init_globl():
    """
    初始化多进程多线程可用的全局变量
    :return: 
    """
    globl.init()
    # td tps
    globl.set_value('lock', compat.Lock())
    globl.set_value('value', compat.List())
    # progressbar
    globl.set_value('pbar_lock', compat.Lock())
    globl.set_value('pbar_value', compat.Value('L', 0))
    # flow control
    globl.set_value('isflow_sleep', compat.Value('b', False))
    globl.set_value('part_task_failed', compat.List())
    globl.set_value('force_exit', compat.Value('b', False))


def process_logs(config):
    """
    扫描日志目录下所有日志文件，将非今日产生的文件非压缩文件进行压缩
    计算日志目录中所有日志文件大小，所超过设置的最大日志量，依次删除老日志文件
    直到总日志量小于等于设置的最大日志量
    :param config: 配置参数
    :return: 
    """
    for filename in os.listdir(LOG_DIR):
        filepath = os.path.join(LOG_DIR, filename)
        if os.path.isfile(filepath) and not filename.endswith('.zip') and not filename.endswith('.lock') and filepath != make_log_filename(config):
            zipfilepath = filepath + '.zip'
            azip = zipfile.ZipFile(zipfilepath, 'w')
            azip.write(filepath, compress_type=ZIP_COMPRESSION_MODE)
            azip.close()

            os.remove(filepath)

    total_size = get_dir_total_size(LOG_DIR)
    max_size = unitstr_to_bytes(config.log.maxsize)
    if total_size > max_size:

        files = sorted(os.listdir(LOG_DIR), key=lambda x: os.path.getmtime(os.path.join(LOG_DIR, x)))
        for file in files:
            filepath = os.path.join(LOG_DIR, file)
            size = os.path.getsize(filepath)
            total_size -= size
            os.remove(filepath)
            if total_size <= max_size:
                break


def process_file_list(config):
    max_size = int(config.task.filelist_max)

    files = os.listdir(FILE_LIST_DIR)
    total_size = len(files)
    if total_size <= max_size:
        return
    for file in files:
        filepath = os.path.join(FILE_LIST_DIR, file)
        os.remove(filepath)
        total_size -= 1
        if total_size <= max_size:
            break


def make_log_filename(config):
    logfile = LOG_FILE + '.' + time.strftime('%Y%m%d', time.localtime())
    return logfile


def init_logger(config):
    """
    初始化日志句柄(写文件)
    需要多进程安全的ConcurrentRotatingFileHandler
    当日志级别大于等于Warning时会同时打印到屏幕和写文件
    :param config: 配置参数
    :return: 
    """
    # console_handler = logging.StreamHandler(sys.__stdout__)
    console_handler = logging.StreamHandler(sys.__stderr__)
    console_handler.level = logging.ERROR
    # console_handler.level = logging.DEBUG
    console_logger = logging.getLogger('obscmd')
    console_logger.addHandler(console_handler)

    format = '%(asctime)s - %(levelname)s - %(process)d - %(thread)d - %(filename)s[line:%(lineno)d] - %(message)s'
    logfile = make_log_filename(config)
    handler = ConcurrentRotatingFileHandler(logfile, mode='a',
                                                   maxBytes=unitstr_to_bytes(config.log.maxbytes),
                                                   backupCount=int(config.log.backupcount),
                                                   encoding=None, delay=0)
    handler.setFormatter(logging.Formatter(format))
    logger = logging.getLogger("obscmd.file")
    logger.propagate = False
    logger.addHandler(handler)
    # logger.setLevel(logging.DEBUG)

    format = '%(message)s'
    handler1 = ConcurrentRotatingFileHandler(logfile, mode='a',
                                            maxBytes=unitstr_to_bytes(config.log.maxbytes),
                                            backupCount=int(config.log.backupcount),
                                            encoding=None, delay=0)
    handler1.setFormatter(logging.Formatter(format))
    logger1 = logging.getLogger("print")
    logger1.propagate = True
    logger1.addHandler(handler1)

    logger.setLevel(LOG_LEVEL[config.log.level])
    return logger, logfile


class Session(object):

    config = {}
    logger = None
    config_file = CONFIG_FILE

    def __init__(self, config, logger):
        self.config = config
        self.logger = logger


class CliDriver(object):

    def __init__(self, session=None):
        self._cli_data = None
        self.session = session

    def main(self, args=None):
        """

        :param args: List of arguments, with the 'obscmd' removed.  For example,
            the command "obscmd obs ls --bucket foo" will have an
            args list of ``['obs', 'ls', '--bucket', 'foo']``.

        """
        if args is None:
            args = sys.argv[1:]
        full_cmd = ' '.join(sys.argv)
        self.session.logger.info('############ {cmd} #############'.format(cmd=full_cmd))
        self.session.full_cmd = full_cmd

        try:
            # it's possible
            # that exceptions can be raised, which should have the same
            # general exception handling logic as calling into the
            # command table.  This is why it's in the try/except clause.

            command_table = self.create_command_table()
            parser = self._create_parser(command_table)
            parsed_args, remaining = parser.parse_known_args(args)
            if parsed_args.command != 'configure':
                self._check_credential()

            return command_table[parsed_args.command](remaining, parsed_args)
        except UnknownArgumentError as e:
            sys.stderr.write("usage: %s\n" % USAGE)
            sys.stderr.write(str(e))
            sys.stderr.write("\n")
            return 255
        except NoRegionError as e:
            msg = ('%s You can also configure your region by running '
                   '"obscmd configure".' % e)
            self._show_error(msg)
            return 255
        except NoCredentialsError as e:

            msg = ("{errmsg}. No access_key_id or secret_access_key found, "
                   "Please set them in file {configfile} or run 'obscmd configure'").format(errmsg=e,
                                                                  configfile=get_full_path(CONFIG_FILE))
            self._show_error(msg)
            return 255
        except KeyboardInterrupt:
            # Shell standard for signals that terminate
            # the process is to return 128 + signum, in this case
            # SIGINT=2, so we'll have an RC of 130.
            sys.stdout.write("\n")
            return 128 + signal.SIGINT
        except Exception as e:
            # self.session.logger.warning("Exception caught in main()", exc_info=True)
            self.session.logger.debug("Exiting with rc 255")
            if e.message:
                self._show_error(str(e.message))
            else:
                msg = 'Exception caught in this task, see *.log please.'
                self._show_error(msg)
            return 255

    def _check_credential(self):
        if not self.session.config.client.access_key_id or not self.session.config.client.secret_access_key:
            raise NoCredentialsError()

    def _show_error(self, msg):
        # self.session.logger.debug(msg, exc_info=True)
        self.session.logger.error(msg, exc_info=True)
        # for functional test
        sys.stderr.write('%s\n' % msg)
        self.session.logger.error(msg)

    def _create_parser(self, command_table):
        version = 'obscmd/%s obs_sdk/%s python/%s' % (__version__, OBS_SDK_VERSION, platform.python_version())
        parser = MainArgParser(
            command_table, version,
            None,
            [],
            prog="obscmd")
        return parser

    def create_command_table(self):
        from obscmd.cmds import cmd_table
        command_table = {}
        for key, cmd in cmd_table.items():
            command_table[key] = cmd(self.session)
        return command_table
