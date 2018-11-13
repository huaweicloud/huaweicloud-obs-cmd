#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import os

_hcmdcli_data_path = []
if 'HCMD_DATA_PATH' in os.environ:
    for path in os.environ['HCMD_DATA_PATH'].split(os.pathsep):
        path = os.path.expandvars(path)
        path = os.path.expanduser(path)
        _hcmdcli_data_path.append(path)
_hcmdcli_data_path.append(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
)
os.environ['HCMD_DATA_PATH'] = os.pathsep.join(_hcmdcli_data_path)
