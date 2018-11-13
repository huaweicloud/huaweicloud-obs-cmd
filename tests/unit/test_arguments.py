#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import mock

from obscmd.testutils import unittest
from obscmd import arguments
from obscmd.model import StringShape, OperationModel, ServiceModel


class DemoArgument(arguments.CustomArgument):
    pass


class TestArgumentClasses(unittest.TestCase):
    def test_can_set_required(self):
        arg = DemoArgument('test-arg')
        self.assertFalse(arg.required)
        arg.required = True
        self.assertTrue(arg.required)


class TestCLIArgument(unittest.TestCase):
    def setUp(self):
        self.service_name = 'baz'
        self.service_model = ServiceModel({
            'metadata': {
                'endpointPrefix': 'bad',
            },
            'operations': {
                'SampleOperation': {
                    'name': 'SampleOperation',
                    'input': {'shape': 'Input'}
                }
            },
            'shapes': {
                'StringShape': {'type': 'string'},
                'Input': {
                    'type': 'structure',
                    'members': {
                        'Foo': {'shape': 'StringShape'}
                    }
                }
            }
        }, self.service_name)
        self.operation_model = self.service_model.operation_model(
            'SampleOperation')
        self.argument_model = self.operation_model
        self.event_emitter = mock.Mock()

    def create_argument(self):
        return arguments.CLIArgument(
            self.argument_model.name, self.argument_model,
            self.operation_model, self.event_emitter)

