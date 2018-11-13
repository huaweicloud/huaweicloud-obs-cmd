import logging

from collections import OrderedDict

import six
from obscmd.argparser import ArgTableArgParser
from obscmd.arguments import CustomArgument, create_argument_model_from_schema
from obscmd.commands import CLICommand
from obscmd.compat import safe_encode, safe_decode, safe_encode_file
from obscmd.utils import uni_print

LOG = logging.getLogger(__name__)


class BasicCommand(CLICommand):
    """Basic top level command with no subcommands.

    If you want to create a new command, subclass this and
    provide the values documented below.

    """

    # This is the name of your command, so if you want to
    # create an 'obscmd mycommand ...' command, the NAME would be
    # 'mycommand'
    NAME = 'commandname'
    # This is the description that will be used for the 'help'
    # command.
    DESCRIPTION = 'describe the command'
    # This is optional, if you are fine with the default synopsis
    # (the way all the built in operations are documented) then you
    # can leave this empty.
    SYNOPSIS = ''
    # If you want to provide some hand written examples, you can do
    # so here.  This is written in RST format.  This is optional,
    # you don't have to provide any examples, though highly encouraged!
    EXAMPLES = ''
    # If your command has arguments, you can specify them here.  This is
    # somewhat of an implementation detail, but this is a list of dicts
    # where the dicts match the kwargs of the CustomArgument's __init__.
    # For example, if I want to add a '--argument-one' and an
    # '--argument-two' command, I'd say:
    #
    # ARG_TABLE = [
    #     {'name': 'argument-one', 'help_text': 'This argument does foo bar.',
    #      'action': 'store', 'required': False, 'cli_type_name': 'string',},
    #     {'name': 'argument-two', 'help_text': 'This argument does some other thing.',
    #      'action': 'store', 'choices': ['a', 'b', 'c']},
    # ]
    #
    # A `schema` parameter option is available to accept a custom JSON
    # structure as input. See the file `obscmd/schema.py` for more info.
    ARG_TABLE = []
    # If you want the command to have subcommands, you can provide a list of
    # dicts.  We use a list here because we want to allow a user to provide
    # the order they want to use for subcommands.
    # SUBCOMMANDS = [
    #     {'name': 'subcommand1', 'command_class': SubcommandClass},
    #     {'name': 'subcommand2', 'command_class': SubcommandClass2},
    # ]
    # The command_class must subclass from ``BasicCommand``.
    SUBCOMMANDS = []

    USAGE = ''

    # You can set the DESCRIPTION, SYNOPSIS, and EXAMPLES to FROM_FILE
    # and we'll automatically read in that data from the file.
    # This is useful if you have a lot of content and would prefer to keep
    # the docs out of the class definition.  For example:
    #
    # DESCRIPTION = FROM_FILE
    #
    # will set the DESCRIPTION value to the contents of
    # obscmd/examples/<command name>/_description.rst
    # The naming conventions for these attributes are:
    #
    # DESCRIPTION = obscmd/examples/<command name>/_description.rst
    # SYNOPSIS = obscmd/examples/<command name>/_synopsis.rst
    # EXAMPLES = obscmd/examples/<command name>/_examples.rst
    #
    # You can also provide a relative path and we'll load the file
    # from the specified location:
    #
    # DESCRIPTION = obscmd/examples/<filename>
    #
    # For example:
    #
    # DESCRIPTION = FROM_FILE('command, 'subcommand, '_description.rst')
    # DESCRIPTION = 'obscmd/examples/command/subcommand/_description.rst'
    #

    # At this point, the only other thing you have to implement is a _run_main
    # method (see the method for more information).

    def __init__(self, session=None):
        self.session = session
        self._arg_table = None
        self._subcommand_table = None

    def __call__(self, args, parsed_globals):
        # args is the remaining unparsed args.
        # We might be able to parse these args so we need to create
        # an arg parser and parse them.
        if not self.subcommand_table:
            self._add_common_args()
        self._subcommand_table = self._build_subcommand_table()

        self._arg_table = self._build_arg_table()

        parser = ArgTableArgParser(self.arg_table, self.subcommand_table)
        parsed_args, remaining = parser.parse_known_args(args)

        if hasattr(parsed_args, 'help') or (hasattr(parsed_globals, 'command') and parsed_globals.command == 'help'):
            return self._display_help(parsed_args, parsed_globals)
        elif getattr(parsed_args, 'subcommand', None) is None:
            # No subcommand was specified so call the main
            # function for this top level command.
            if remaining:
                raise ValueError("Unknown options: %s" % ','.join(remaining))
            return self._run_main(parsed_args, parsed_globals)
        else:
            return self.subcommand_table[parsed_args.subcommand](remaining,
                                                                 parsed_globals)

    def _add_common_args(self):
        """
        for some arguments, lots of command need
        :return: 
        """
        common_args = [
            {'name': 'ak',
             'help_text': "access key id"},
            {'name': 'sk',
             'help_text': "secret access key"},
            {'name': 'server',
             'help_text': "obs server"},
        ]
        self.ARG_TABLE += common_args

    def _run_main(self, parsed_args, parsed_globals):
        # Subclasses should implement this method.
        # parsed_globals are the parsed global args (things like region,
        # profile, output, etc.)
        # parsed_args are any arguments you've defined in your ARG_TABLE
        # that are parsed.  These will come through as whatever you've
        # provided as the 'dest' key.  Otherwise they default to the
        # 'name' key.  For example: ARG_TABLE[0] = {"name": "foo-arg", ...}
        # can be accessed by ``parsed_args.foo_arg``.
        raise NotImplementedError("_run_main")

    def _outprint(self, statement, outfile=None):
        """
        print to screen and write into file
        :param statement: content
        :param outfile: file path
        :return: 
        """
        if not statement.endswith("\n"):
            statement = statement + "\n"
        if isinstance(outfile, six.string_types):
            uni_print(statement, open(safe_decode(outfile), 'a'))
        # if isinstance(outfile, list):
        #     for afile in outfile:
        #         uni_print(statement, open(safe_decode(afile), 'a'))
        uni_print(safe_decode(statement))
        uni_print(statement, open(safe_decode(self.session.logfile), 'a'))
        # statement = statement.strip('\n')
        # if len(statement) > 0:
        #     self.session.logger.info(safe_encode_file(statement))

    def _build_subcommand_table(self):
        subcommand_table = OrderedDict()
        for subcommand in self.SUBCOMMANDS:
            subcommand_name = subcommand['name']
            subcommand_class = subcommand['command_class']
            subcommand_table[subcommand_name] = subcommand_class(self.session)
        return subcommand_table

    def _build_arg_table(self):
        arg_table = OrderedDict()
        for arg_data in self.ARG_TABLE:

            # If a custom schema was passed in, create the argument_model
            # so that it can be validated and docs can be generated.
            if 'schema' in arg_data:
                argument_model = create_argument_model_from_schema(
                    arg_data.pop('schema'))
                arg_data['argument_model'] = argument_model
            custom_argument = CustomArgument(**arg_data)

            arg_table[arg_data['name']] = custom_argument
        return arg_table

    def _display_help(self, parsed_args, parsed_globals):
        print('NAME')
        print('\t\t%s\n' % self.name)
        print('DESCRIPTION')
        print('\t\t%s\n' % self.DESCRIPTION)
        print('SYNOPSIS')
        print('\t\t%s\n' % self.SYNOPSIS)
        print('\t\t%s\n' % self.USAGE)
        print('OPTIONS')
        self._print_option(self.ARG_TABLE)
        print('EXAMPLES')
        print('\t\t%s\n' % self.EXAMPLES)

        return 0

    def _print_option(self, arg_table):
        for arg in arg_table:
            argname = arg.get('name', 'obscmd')
            argtype = arg.get('cli_type_name', 'string')
            action = arg.get('action')
            if action == 'store_true' or action == 'store_false':
                argtype = 'bool'
            desc = arg.get('help_text', '')
            choices = arg.get('choices', '')
            print('\t\t--{argname} ({argtype}) {choices} \n'
                  '\t\t{desc}\n'.format(argname=argname, choices=choices, argtype=argtype, desc=desc))

    @property
    def arg_table(self):
        if self._arg_table is None:
            self._arg_table = self._build_arg_table()
        return self._arg_table

    @property
    def subcommand_table(self):
        if self._subcommand_table is None:
            self._subcommand_table = self._build_subcommand_table()
        return self._subcommand_table

    @property
    def name(self):
        return self.NAME


class HelpCommand(BasicCommand):

    NAME = 'obscmd'
    DESCRIPTION = "The OBS Command Line Interface is a unified tool to manage your OBS services."
    SYNOPSIS = "obscmd [options] <command> <subcommand> [parameters]"
    USAGE = "Use *obscmd command help* for information on a specific command. Use *obscmd help topics* \n" \
            "to view a list of available help topics. \n" \
            "The synopsis for each command shows its parameters and their usage. Optional parameters \n" \
            "are shown in square brackets." \
            "obscmd obs/configure ..."

    ARG_TABLE = [
        {'name': 'help',
         'help_text': 'print help information',
         },
        {
            'name': 'obs',
            'help_text': 'obs command'
        },
        {
            'name': 'configure',
            'help_text': 'configure command'
        }
    ]
    EXAMPLES = ""