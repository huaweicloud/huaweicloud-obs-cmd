#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import os

from obscmd.configloader import raw_config_parse
from obscmd.utils import get_full_path


HCMD_DIR = get_full_path(os.path.join('~', '.obscmd'))
CONFIG_FILE = os.path.join(HCMD_DIR, 'obscmd.ini')
DEFAULT_CONFIG_FILE = os.path.join(HCMD_DIR, 'default.ini')


ROOT_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(ROOT_DIR, 'data')
HCMD_INI = os.path.join(DATA_DIR, 'obscmd.ini')

user_config = raw_config_parse(CONFIG_FILE) if os.path.exists(CONFIG_FILE) else raw_config_parse(HCMD_INI)
default_config = raw_config_parse(DEFAULT_CONFIG_FILE) if os.path.exists(DEFAULT_CONFIG_FILE) else raw_config_parse(HCMD_INI)

config = default_config
config.task.update(user_config.task)
config.log.update(user_config.log)
config.client.update(user_config.client)

# DOWNLOAD_TMP_DIR = get_full_path(config.task.download_tmp_path)
CP_DIR = get_full_path(config.task.checkpoint_path)
LOG_DIR = get_full_path(config.log.log_path)
LOG_FILE = os.path.join(LOG_DIR, 'obscmd.log')

FILE_LIST_DIR = get_full_path(config.task.filelist_path)

TRY_MULTIPART_TIMES = 2


# some constant variables

# PARTSIZE_MINIMUM = 5 * 1024 * 1024
PARTSIZE_MINIMUM = 1
PARTSIZE_MAXIMUM = 5 * 1024 * 1024 * 1024

MAX_PART_NUM = 10000


BAR_SLEEP_FOR_UPDATE = 1
BAR_MS2S = 1000
BAR_NCOLS = 80
BAR_MININTERVAL = 1
BAR_MINITERS = 10
