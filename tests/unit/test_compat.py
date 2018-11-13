#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import six
from nose.tools import assert_equal

from obscmd.compat import ensure_text_type
from obscmd.compat import compat_shell_quote
from obscmd.compat import get_popen_kwargs_for_pager_cmd
from obscmd.testutils import mock, unittest


class TestEnsureText(unittest.TestCase):
    def test_string(self):
        value = 'foo'
        response = ensure_text_type(value)
        self.assertIsInstance(response, six.text_type)
        self.assertEqual(response, 'foo')

    def test_binary(self):
        value = b'bar'
        response = ensure_text_type(value)
        self.assertIsInstance(response, six.text_type)
        self.assertEqual(response, 'bar')

    def test_unicode(self):
        value = u'baz'
        response = ensure_text_type(value)
        self.assertIsInstance(response, six.text_type)
        self.assertEqual(response, 'baz')

    def test_non_ascii(self):
        value = b'\xe2\x9c\x93'
        response = ensure_text_type(value)
        self.assertIsInstance(response, six.text_type)
        self.assertEqual(response, u'\u2713')

    def test_non_string_or_bytes_raises_error(self):
        value = 500
        with self.assertRaises(ValueError):
            ensure_text_type(value)


def test_compat_shell_quote_windows():
    windows_cases = {
        '': '""',
        '"': '\\"',
        '\\': '\\',
        '\\a': '\\a',
        '\\\\': '\\\\',
        '\\"': '\\\\\\"',
        '\\\\"': '\\\\\\\\\\"',
        'foo bar': '"foo bar"',
        'foo\tbar': '"foo\tbar"',
    }
    for input_string, expected_output in windows_cases.items():
        yield ShellQuoteTestCase().run, input_string, expected_output, "win32"


def test_comat_shell_quote_unix():
    unix_cases = {
        "": "''",
        "*": "'*'",
        "foo": "foo",
        "foo bar": "'foo bar'",
        "foo\tbar": "'foo\tbar'",
        "foo\nbar": "'foo\nbar'",
        "foo'bar": "'foo'\"'\"'bar'",
    }
    for input_string, expected_output in unix_cases.items():
        yield ShellQuoteTestCase().run, input_string, expected_output, "linux2"
        yield ShellQuoteTestCase().run, input_string, expected_output, "darwin"


class ShellQuoteTestCase(object):
    def run(self, s, expected, platform=None):
        assert_equal(compat_shell_quote(s, platform), expected)


class TestGetPopenPagerCmd(unittest.TestCase):
    @mock.patch('obscmd.compat.is_windows', True)
    @mock.patch('obscmd.compat.default_pager', 'more')
    def test_windows(self):
        kwargs = get_popen_kwargs_for_pager_cmd()
        self.assertEqual({'args': 'more', 'shell': True}, kwargs)

    @mock.patch('obscmd.compat.is_windows', True)
    @mock.patch('obscmd.compat.default_pager', 'more')
    def test_windows_with_specific_pager(self):
        kwargs = get_popen_kwargs_for_pager_cmd('less -R')
        self.assertEqual({'args': 'less -R', 'shell': True}, kwargs)

    @mock.patch('obscmd.compat.is_windows', False)
    @mock.patch('obscmd.compat.default_pager', 'less -R')
    def test_non_windows(self):
        kwargs = get_popen_kwargs_for_pager_cmd()
        self.assertEqual({'args': ['less', '-R']}, kwargs)

    @mock.patch('obscmd.compat.is_windows', False)
    @mock.patch('obscmd.compat.default_pager', 'less -R')
    def test_non_windows_specific_pager(self):
        kwargs = get_popen_kwargs_for_pager_cmd('more')
        self.assertEqual({'args': ['more']}, kwargs)
