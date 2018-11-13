#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from __future__ import unicode_literals


class HcmdCoreError(Exception):
    """
    The base exception class for obscmd exceptions.

    :ivar msg: The descriptive message associated with the error.
    """
    fmt = 'An unspecified error occurred'

    def __init__(self, **kwargs):
        msg = self.fmt.format(**kwargs)
        Exception.__init__(self, msg)
        self.kwargs = kwargs


class MaxPartNumError(HcmdCoreError):
    """
    Excecuting command Error
    """
    fmt = 'Your partsize is too small, part number cannot exceed 10000! File size: {filesize}, partsize: {partsize}'


class NotSupportError(HcmdCoreError):
    """
    Excecuting command Error
    """
    fmt = '{msg} not support'


class FileOrDirNotFoundError(HcmdCoreError):
    """
    Excecuting command Error
    """
    fmt = 'File or Dir not found: {path}'


class InternalError(HcmdCoreError):
    """
    Excecuting command Error
    """
    fmt = 'Internal error. status: {status}, reason: {reason}.Error code: {code}, Error msg: {msg}'


class DataNotFoundError(HcmdCoreError):
    """
    The data associated with a particular path could not be loaded.

    :ivar path: The data path that the user attempted to load.
    """
    fmt = 'Unable to load data for: {data_path}'


class UnknownServiceError(DataNotFoundError):
    """Raised when trying to load data for an unknown service.

    :ivar service_name: The name of the unknown service.

    """
    fmt = (
        "Unknown service: '{service_name}'. Valid service names are: "
        "{known_service_names}")


class ApiVersionNotFoundError(HcmdCoreError):
    """
    The data associated with either that API version or a compatible one
    could not be loaded.

    :ivar path: The data path that the user attempted to load.
    :ivar path: The API version that the user attempted to load.
    """
    fmt = 'Unable to load data {data_path} for: {api_version}'


# class ConnectionClosedError(ConnectionError):
#     fmt = (
#         'Connection was closed before we received a valid response '
#         'from endpoint URL: "{endpoint_url}".')
#
#     def __init__(self, **kwargs):
#         msg = self.fmt.format(**kwargs)
#         kwargs.pop('endpoint_url')
#         super(ConnectionClosedError, self).__init__(msg, **kwargs)


class NoCredentialsError(HcmdCoreError):
    """
    No credentials could be found
    """
    fmt = 'Unable to locate credentials'


class PartialCredentialsError(HcmdCoreError):
    """
    Only partial credentials were found.

    :ivar cred_var: The missing credential variable name.

    """
    fmt = 'Partial credentials found in {provider}, missing: {cred_var}'


class CredentialRetrievalError(HcmdCoreError):
    """
    Error attempting to retrieve credentials from a remote source.

    :ivar provider: The name of the credential provider.
    :ivar error_msg: The msg explaning why credentials could not be
        retrieved.

    """
    fmt = 'Error when retrieving credentials from {provider}: {error_msg}'


class UnknownSignatureVersionError(HcmdCoreError):
    """
    Requested Signature Version is not known.

    :ivar signature_version: The name of the requested signature version.
    """
    fmt = 'Unknown Signature Version: {signature_version}.'


class ServiceNotInRegionError(HcmdCoreError):
    """
    The service is not available in requested region.

    :ivar service_name: The name of the service.
    :ivar region_name: The name of the region.
    """
    fmt = 'Service {service_name} not available in region {region_name}'


class BaseEndpointResolverError(HcmdCoreError):
    """Base error for endpoint resolving errors.

    Should never be raised directly, but clients can catch
    this exception if they want to generically handle any errors
    during the endpoint resolution process.

    """


class NoRegionError(BaseEndpointResolverError):
    """No region was specified."""
    fmt = 'You must specify a region.'


class ProfileNotFound(HcmdCoreError):
    """
    The specified configuration profile was not found in the
    configuration file.

    :ivar profile: The name of the profile the user attempted to load.
    """
    fmt = 'The config profile ({profile}) could not be found'


class ConfigParseError(HcmdCoreError):
    """
    The configuration file could not be parsed.

    :ivar path: The path to the configuration file.
    """
    fmt = 'Unable to parse config file: {path}'


class ConfigNotFound(HcmdCoreError):
    """
    The specified configuration file could not be found.

    :ivar path: The path to the configuration file.
    """
    fmt = 'The specified config file ({path}) could not be found.'


class MissingParametersError(HcmdCoreError):
    """
    One or more required parameters were not supplied.

    :ivar object: The object that has missing parameters.
        This can be an operation or a parameter (in the
        case of inner params).  The str() of this object
        will be used so it doesn't need to implement anything
        other than str().
    :ivar missing: The names of the missing parameters.
    """
    fmt = ('The following required parameters are missing for '
           '{object_name}: {missing}')


class ValidationError(HcmdCoreError):
    """
    An exception occurred validating parameters.

    Subclasses must accept a ``value`` and ``param``
    argument in their ``__init__``.

    :ivar value: The value that was being validated.
    :ivar param: The parameter that failed validation.
    :ivar type_name: The name of the underlying type.
    """
    fmt = ("Invalid value ('{value}') for param {param} "
           "of type {type_name} ")


class ParamValidationError(HcmdCoreError):
    fmt = 'Parameter validation failed:\n{report}'


class CommandParamValidationError(HcmdCoreError):
    fmt = 'Command parameter validation failed: {report}'


# These exceptions subclass from ValidationError so that code
# can just 'except ValidationError' to catch any possibly validation
# error.
class UnknownKeyError(ValidationError):
    """
    Unknown key in a struct paramster.

    :ivar value: The value that was being checked.
    :ivar param: The name of the parameter.
    :ivar choices: The valid choices the value can be.
    """
    fmt = ("Unknown key '{value}' for param '{param}'.  Must be one "
           "of: {choices}")


class RangeError(ValidationError):
    """
    A parameter value was out of the valid range.

    :ivar value: The value that was being checked.
    :ivar param: The parameter that failed validation.
    :ivar min_value: The specified minimum value.
    :ivar max_value: The specified maximum value.
    """
    fmt = ('Value out of range for param {param}: '
           '{min_value} <= {value} <= {max_value}')


class UnknownParameterError(ValidationError):
    """
    Unknown top level parameter.

    :ivar name: The name of the unknown parameter.
    :ivar operation: The name of the operation.
    :ivar choices: The valid choices the parameter name can be.
    """
    fmt = (
        "Unknown parameter '{name}' for operation {operation}.  Must be one "
        "of: {choices}"
    )


class ACLJsonAnalyzeError(ValidationError):
    """
    Unknown top level parameter.

    :ivar name: The name of the unknown parameter.
    :ivar operation: The name of the operation.
    :ivar choices: The valid choices the parameter name can be.
    """
    fmt = (
        "Wrong ACL json string, {acl_str}"
    )


class LCJsonAnalyzeError(ValidationError):
    """
    Unknown top level parameter.

    :ivar name: The name of the unknown parameter.
    :ivar operation: The name of the operation.
    :ivar choices: The valid choices the parameter name can be.
    """
    fmt = (
        "Wrong lifecycle json string, {rule_str}"
    )


class WebsiteJsonAnalyzeError(ValidationError):
    """
    Unknown top level parameter.

    :ivar name: The name of the unknown parameter.
    :ivar operation: The name of the operation.
    :ivar choices: The valid choices the parameter name can be.
    """
    fmt = (
        "Wrong website json string, {website_str}"
    )


class LogJsonAnalyzeError(ValidationError):
    """
    Unknown top level parameter.

    :ivar name: The name of the unknown parameter.
    :ivar operation: The name of the operation.
    :ivar choices: The valid choices the parameter name can be.
    """
    fmt = (
        "Wrong bucket log json string, {log_str}"
    )


class AliasConflictParameterError(ValidationError):
    """
    Error when an alias is provided for a parameter as well as the original.

    :ivar original: The name of the original parameter.
    :ivar alias: The name of the alias
    :ivar operation: The name of the operation.
    """
    fmt = (
        "Parameter '{original}' and its alias '{alias}' were provided "
        "for operation {operation}.  Only one of them may be used."
    )


class UnknownServiceStyle(HcmdCoreError):
    """
    Unknown style of service invocation.

    :ivar service_style: The style requested.
    """
    fmt = 'The service style ({service_style}) is not understood.'


class PaginationError(HcmdCoreError):
    fmt = 'Error during pagination: {message}'


class OperationNotPageableError(HcmdCoreError):
    fmt = 'Operation cannot be paginated: {operation_name}'


class ChecksumError(HcmdCoreError):
    """The expected checksum did not match the calculated checksum.

    """
    fmt = ('Checksum {checksum_type} failed, expected checksum '
           '{expected_checksum} did not match calculated checksum '
           '{actual_checksum}.')


class UnseekableStreamError(HcmdCoreError):
    """Need to seek a stream, but stream does not support seeking.

    """
    fmt = ('Need to rewind the stream {stream_object}, but stream '
           'is not seekable.')


class WaiterError(HcmdCoreError):
    """Waiter failed to reach desired state."""
    fmt = 'Waiter {name} failed: {reason}'

    def __init__(self, name, reason, last_response):
        super(WaiterError, self).__init__(name=name, reason=reason)
        self.last_response = last_response


class IncompleteReadError(HcmdCoreError):
    """HTTP response did not return expected number of bytes."""
    fmt = ('{actual_bytes} read, but total bytes '
           'expected is {expected_bytes}.')


class InvalidExpressionError(HcmdCoreError):
    """Expression is either invalid or too complex."""
    fmt = 'Invalid expression {expression}: Only dotted lookups are supported.'


class UnknownCredentialError(HcmdCoreError):
    """Tried to insert before/after an unregistered credential type."""
    fmt = 'Credential named {name} not found.'


class WaiterConfigError(HcmdCoreError):
    """Error when processing waiter configuration."""
    fmt = 'Error processing waiter config: {error_msg}'


class UnknownClientMethodError(HcmdCoreError):
    """Error when trying to access a method on a client that does not exist."""
    fmt = 'Client does not have method: {method_name}'


class UnsupportedSignatureVersionError(HcmdCoreError):
    """Error when trying to access a method on a client that does not exist."""
    fmt = 'Signature version is not supported: {signature_version}'


class ClientError(Exception):
    MSG_TEMPLATE = (
        'An error occurred ({error_code}) when calling the {operation_name} '
        'operation{retry_info}: {error_message}')

    def __init__(self, error_response, operation_name):
        retry_info = self._get_retry_info(error_response)
        error = error_response.get('Error', {})
        msg = self.MSG_TEMPLATE.format(
            error_code=error.get('Code', 'Unknown'),
            error_message=error.get('Message', 'Unknown'),
            operation_name=operation_name,
            retry_info=retry_info,
        )
        super(ClientError, self).__init__(msg)
        self.response = error_response
        self.operation_name = operation_name

    def _get_retry_info(self, response):
        retry_info = ''
        if 'ResponseMetadata' in response:
            metadata = response['ResponseMetadata']
            if metadata.get('MaxAttemptsReached', False):
                if 'RetryAttempts' in metadata:
                    retry_info = (' (reached max retries: %s)' %
                                  metadata['RetryAttempts'])
        return retry_info


class EventStreamError(ClientError):
    pass


class UnsupportedTLSVersionWarning(Warning):
    """Warn when an openssl version that uses TLS 1.2 is required"""
    pass


class ImminentRemovalWarning(Warning):
    pass


class InvalidRetryConfigurationError(HcmdCoreError):
    """Error when invalid retry configuration is specified"""
    fmt = (
        'Cannot provide retry configuration for "{retry_config_option}". '
        'Valid retry configuration options are: \'max_attempts\''
    )


class InvalidMaxRetryAttemptsError(InvalidRetryConfigurationError):
    """Error when invalid retry configuration is specified"""
    fmt = (
        'Value provided to "max_attempts": {provided_max_attempts} must '
        'be an integer greater than or equal to zero.'
    )


class InvalidConfigError(HcmdCoreError):
    fmt = '{error_msg}'




class MD5UnavailableError(HcmdCoreError):
    fmt = "This system does not support MD5 generation."


class MetadataRetrievalError(HcmdCoreError):
    fmt = "Error retrieving metadata: {error_msg}"
