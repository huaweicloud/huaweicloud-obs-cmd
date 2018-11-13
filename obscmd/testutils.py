#!/usr/bin/env python
# -*- coding: UTF-8 -*-


"""Test utilities for the OBS CLI.

This module includes various classes/functions that help in writing
CLI unit/integration tests.  This module should not be imported by
any module **except** for test code.  This is included in the CLI
package so that code that is not part of the CLI can still take
advantage of all the testing utilities we provide.

"""
import os
import sys
import copy
import shutil
import logging
import tempfile
import platform
import contextlib
from pprint import pformat
from obscmd.cmds.obs.obsutil import check_resp
from obscmd.utils import unitstr_to_bytes, random_string

try:
    import mock
except ImportError as e:
    # In the off chance something imports this module
    # that's not suppose to, we should not stop the CLI
    # by raising an ImportError.  Now if anything actually
    # *uses* this module that isn't suppose to, that's a
    # different story.
    mock = None
from obscmd.compat import six


# The unittest module got a significant overhaul
# in 2.7, so if we're in 2.6 we can use the backported
# version unittest2.
if sys.version_info[:2] == (2, 6):
    import unittest2 as unittest
else:
    import unittest


# In python 3, order matters when calling assertEqual to
# compare lists and dictionaries with lists. Therefore,
# assertItemsEqual needs to be used but it is renamed to
# assertCountEqual in python 3.
if six.PY2:
    unittest.TestCase.assertCountEqual = unittest.TestCase.assertItemsEqual


# _LOADER = obscmd.loaders.Loader()
INTEG_LOG = logging.getLogger('obscmd.tests.integration')
HCMD_CMD = None


def skip_if_windows(reason):
    """Decorator to skip tests that should not be run on windows.

    Example usage:

        @skip_if_windows("Not valid")
        def test_some_non_windows_stuff(self):
            self.assertEqual(...)

    """
    def decorator(func):
        return unittest.skipIf(
            platform.system() not in ['Darwin', 'Linux'], reason)(func)
    return decorator


def set_invalid_utime(path):
    """Helper function to set an invalid last modified time"""
    try:
        os.utime(path, (-1, -100000000000))
    except (OSError, OverflowError):
        # Some OS's such as Windows throws an error for trying to set a
        # last modified time of that size. So if an error is thrown, set it
        # to just a negative time which will trigger the warning as well for
        # Windows.
        os.utime(path, (-1, -1))


def create_clidriver():
    from obscmd.clidriver import create_clidriver
    driver = create_clidriver()
    return driver


def create_bucket(client, name=None, region=None):
    """
    Creates a bucket
    :returns: the name of the bucket created
    """
    if name:
        bucket_name = name
    else:
        bucket_name = random_string()
    resp = client.createBucket(name)
    check_resp(resp)
    return bucket_name


class BaseCLIDriverTest(unittest.TestCase):
    """Base unittest that use clidriver.

    This will load all the default plugins as well so it
    will simulate the behavior the user will see.
    """
    def setUp(self):
        self.environ = {
            'HCMD_DATA_PATH': os.environ['HCMD_DATA_PATH'],
            'HCMD_DEFAULT_REGION': 'us-east-1',
            'HCMD_ACCESS_KEY_ID': 'access_key',
            'HCMD_SECRET_ACCESS_KEY': 'secret_key',
            'HCMD_CONFIG_FILE': '',
        }
        self.environ_patch = mock.patch('os.environ', self.environ)
        self.environ_patch.start()
        # self.driver = create_clidriver()
        # self.session = self.driver.session

    def tearDown(self):
        self.environ_patch.stop()


class BaseHCMDHelpOutputTest(BaseCLIDriverTest):
    def setUp(self):
        super(BaseHCMDHelpOutputTest, self).setUp()
        self.renderer_patch = mock.patch('obscmd.help.get_renderer')
        self.renderer_mock = self.renderer_patch.start()
        self.renderer = CapturedRenderer()
        self.renderer_mock.return_value = self.renderer

    def tearDown(self):
        super(BaseHCMDHelpOutputTest, self).tearDown()
        self.renderer_patch.stop()

    def assert_contains(self, contains):
        if contains not in self.renderer.rendered_contents:
            self.fail("The expected contents:\n%s\nwere not in the "
                      "actual rendered contents:\n%s" % (
                          contains, self.renderer.rendered_contents))

    def assert_contains_with_count(self, contains, count):
        r_count = self.renderer.rendered_contents.count(contains)
        if r_count != count:
            self.fail("The expected contents:\n%s\n, with the "
                      "count:\n%d\nwere not in the actual rendered "
                      " contents:\n%s\nwith count:\n%d" % (
                          contains, count, self.renderer.rendered_contents, r_count))

    def assert_not_contains(self, contents):
        if contents in self.renderer.rendered_contents:
            self.fail("The contents:\n%s\nwere not suppose to be in the "
                      "actual rendered contents:\n%s" % (
                          contents, self.renderer.rendered_contents))

    def assert_text_order(self, *args, **kwargs):
        # First we need to find where the SYNOPSIS section starts.
        starting_from = kwargs.pop('starting_from')
        args = list(args)
        contents = self.renderer.rendered_contents
        self.assertIn(starting_from, contents)
        start_index = contents.find(starting_from)
        arg_indices = [contents.find(arg, start_index) for arg in args]
        previous = arg_indices[0]
        for i, index in enumerate(arg_indices[1:], 1):
            if index == -1:
                self.fail('The string %r was not found in the contents: %s'
                          % (args[index], contents))
            if index < previous:
                self.fail('The string %r came before %r, but was suppose to come '
                          'after it.\n%s' % (args[i], args[i - 1], contents))
            previous = index


class CapturedRenderer(object):
    def __init__(self):
        self.rendered_contents = ''

    def render(self, contents):
        self.rendered_contents = contents.decode('utf-8')


class CapturedOutput(object):
    def __init__(self, stdout, stderr):
        self.stdout = stdout
        self.stderr = stderr


@contextlib.contextmanager
def capture_output():
    stderr = six.StringIO()
    stdout = six.StringIO()
    with mock.patch('sys.stderr', stderr):
        with mock.patch('sys.stdout', stdout):
            yield CapturedOutput(stdout, stderr)


@contextlib.contextmanager
def capture_input(input_bytes=b''):
    input_data = six.BytesIO(input_bytes)
    if six.PY3:
        mock_object = mock.Mock()
        mock_object.buffer = input_data
    else:
        mock_object = input_data

    with mock.patch('sys.stdin', mock_object):
        yield input_data


class BaseHcmdCommandParamsTest(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def assert_params_for_cmd(self, cmd, params=None, expected_rc=0,
                              stderr_contains=None, ignore_params=None):
        stdout, stderr, rc = self.run_cmd(cmd, expected_rc)
        if stderr_contains is not None:
            self.assertIn(stderr_contains, stderr)
        if params is not None:
            # The last kwargs of Operation.call() in obscmd.
            last_kwargs = copy.copy(self.last_kwargs)
            if ignore_params is not None:
                for key in ignore_params:
                    try:
                        del last_kwargs[key]
                    except KeyError:
                        pass
            if params != last_kwargs:
                self.fail("Actual params did not match expected params.\n"
                          "Expected:\n\n"
                          "%s\n"
                          "Actual:\n\n%s\n" % (
                              pformat(params), pformat(last_kwargs)))
        return stdout, stderr, rc

    def run_cmd(self, cmd, expected_rc=0):
        logging.debug("Calling cmd: %s", cmd)
        self.driver = create_clidriver()

        if not isinstance(cmd, list):
            cmdlist = cmd.split()
        else:
            cmdlist = cmd

        with capture_output() as captured:
            try:
                rc = self.driver.main(cmdlist)
            except SystemExit as e:
                # We need to catch SystemExit so that we
                # can get a proper rc and still present the
                # stdout/stderr to the test runner so we can
                # figure out what went wrong.
                rc = e.code
        stderr = captured.stderr.getvalue()
        stdout = captured.stdout.getvalue()

        self.assertEqual(
            rc, expected_rc,
            "Unexpected rc (expected: %s, actual: %s) for command: %s\n"
            "stdout:\n%sstderr:\n%s" % (
                expected_rc, rc, cmd, stdout, stderr))
        return stdout, stderr, rc


class FileCreator(object):
    def __init__(self):
        self.rootdir = tempfile.mkdtemp()

    def remove_all(self, path=None):
        if path:
            shutil.rmtree(path)
        shutil.rmtree(self.rootdir)

    def create_full_dir(self, rootdir, dirname, filenum=0, names=None, sizelist=None):
        tmpdir = self.rootdir
        self.rootdir = rootdir
        fullpath = self.create_dir(dirname, filenum, names, sizelist)
        self.rootdir = tmpdir
        return fullpath

    def create_dir(self, dirname, filenum=0, names=None, sizelist=None):
        tmpdir = self.rootdir
        self.rootdir = os.path.join(self.rootdir, dirname)

        os.mkdir(self.rootdir)
        for i in range(filenum):
            name = names[i] if names and i < len(names) else random_string(8)
            size = sizelist[i] if sizelist and i < len(sizelist) else '8B'
            self.create_size_file(name, size)
        fulldir = self.rootdir
        self.rootdir = tmpdir
        return fulldir

    def create_size_file(self, filename, size):
        size = unitstr_to_bytes(size)
        full_path = os.path.join(self.rootdir, filename)
        with open(full_path, 'w') as fp:
            fp.seek(size-1)
            fp.write('a')
            fp.close()
        return full_path

    def create_file(self, filename, contents=None, mtime=None, mode='w'):
        """Creates a file in a tmpdir

        ``filename`` should be a relative path, e.g. "foo/bar/baz.txt"
        It will be translated into a full path in a tmp dir.

        If the ``mtime`` argument is provided, then the file's
        mtime will be set to the provided value (must be an epoch time).
        Otherwise the mtime is left untouched.

        ``mode`` is the mode the file should be opened either as ``w`` or
        `wb``.

        Returns the full path to the file.

        """
        contents = contents if contents else random_string(8)
        full_path = os.path.join(self.rootdir, filename)
        if not os.path.isdir(os.path.dirname(full_path)):
            os.makedirs(os.path.dirname(full_path))
        with open(full_path, mode) as f:
            f.write(contents)
        current_time = os.path.getmtime(full_path)
        # Subtract a few years off the last modification date.
        os.utime(full_path, (current_time, current_time - 100000000))
        if mtime is not None:
            os.utime(full_path, (mtime, mtime))
        return full_path

    def append_file(self, filename, contents):
        """Append contents to a file

        ``filename`` should be a relative path, e.g. "foo/bar/baz.txt"
        It will be translated into a full path in a tmp dir.

        Returns the full path to the file.
        """
        full_path = os.path.join(self.rootdir, filename)
        if not os.path.isdir(os.path.dirname(full_path)):
            os.makedirs(os.path.dirname(full_path))
        with open(full_path, 'a') as f:
            f.write(contents)
        return full_path

    def full_path(self, filename):
        """Translate relative path to full path in temp dir.

        f.full_path('foo/bar.txt') -> /tmp/asdfasd/foo/bar.txt
        """
        return os.path.join(self.rootdir, filename)



