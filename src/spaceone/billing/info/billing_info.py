import functools
from spaceone.api.billing.v1 import billing_pb2
from spaceone.core.pygrpc.message_type import *

__all__ = ['BillingDataInfo']

def BillingData(data):
    info = {
        'date': data['date'],
        'cost': data['cost']
    }
    if 'currency' in data:
        info.update({'currency': data['currency']})
    return billing_pb2.BillingData(**info)

def BillingInfo(billing_info):
    info = {
        'resource_type': billing_info['resource_type'],
        'billing_data': list(map(functools.partial(BillingData), billing_info['billing_data'])),
        'name': billing_info.get('name', None),
        'domain_id': billing_info['domain_id']
    }

    return billing_pb2.BillingInfo(**info)

def BillingDataInfo(billing_info_list):
    total_count = len(billing_info_list)
    billing_info = list(map(functools.partial(BillingInfo), billing_info_list))
    return billing_pb2.BillingDataInfo(results=billing_info, total_count=total_count)

