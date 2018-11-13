#!/usr/bin/env python
# -*- coding: UTF-8 -*-

class CLICommand(object):

    """Interface for a CLI command.

    This class represents a top level CLI command
    (``obscmd ec2``, ``obscmd obs``, ``obscmd config``).

    """

    @property
    def name(self):
        # Subclasses must implement a name.
        raise NotImplementedError("name")

    @name.setter
    def name(self, value):
        # Subclasses must implement setting/changing the cmd name.
        raise NotImplementedError("name")

    def __call__(self, args, parsed_globals):
        """Invoke CLI operation.

        :type args: str
        :param args: The remaining command line args.

        :type parsed_globals: ``argparse.Namespace``
        :param parsed_globals: The parsed arguments so far.

        :rtype: int
        :return: The return code of the operation.  This will be used
            as the RC code for the ``obscmd`` process.

        """
        # Subclasses are expected to implement this method.
        pass

    def create_help_command(self):
        # Subclasses are expected to implement this method if they want
        # help docs.
        return None

    @property
    def arg_table(self):
        return {}


