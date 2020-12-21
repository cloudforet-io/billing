from spaceone.core.error import *


class ERROR_NOT_SUPPORT_RESOURCE_TYPE(ERROR_INVALID_ARGUMENT):
    _message = 'Data source not support resource_type. (supported_resource_type = {supported_resource_type})'

class ERROR_BILLING_REQUEST_FORMAT(ERROR_INVALID_ARGUMENT):
    _message = 'field: {key} is not valid, example := {example}'

class ERROR_BILLING_AGGREGATION(ERROR_INVALID_ARGUMENT):
    _message = 'failed to aggregate data, params={params}'

class ERROR_BILLING_CREATE_RESULT(ERROR_INVALID_ARGUMENT):
    _message = 'failed to aggregate data, params={params}'

