from spaceone.core.error import *


class ERROR_NOT_SUPPORT_RESOURCE_TYPE(ERROR_INVALID_ARGUMENT):
    _message = 'Data source not support resource_type. (supported_resource_type = {supported_resource_type})'

