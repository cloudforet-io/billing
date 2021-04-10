from spaceone.core.pygrpc.message_type import *

__all__ = ['BillingDataInfo']


def BillingDataInfo(billing_info):
    return change_struct_type(billing_info)
