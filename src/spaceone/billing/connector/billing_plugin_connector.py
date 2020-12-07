import logging

from google.protobuf.json_format import MessageToDict

from spaceone.core.connector import BaseConnector
from spaceone.core import pygrpc
from spaceone.core.utils import parse_endpoint
from spaceone.core.error import *

__all__ = ['BillingPluginConnector']

_LOGGER = logging.getLogger(__name__)


class BillingPluginConnector(BaseConnector):

    def __init__(self, transaction, config):
        super().__init__(transaction, config)
        self.client = None

    def initialize(self, endpoint):
        e = parse_endpoint(endpoint)
        self.client = pygrpc.client(endpoint=f'{e.get("hostname")}:{e.get("port")}', version='plugin')

    def init(self, options):
        response = self.client.DataSource.init({
            'options': options,
        }, metadata=self.transaction.get_connection_meta())

        return self._change_message(response)
        # return response

    def verify(self, schema, options, secret_data):
        params = {
            'options': options,
            'secret_data': secret_data
        }

        if schema:
            params.update({
                'schema': schema
            })

        self.client.DataSource.verify(params, metadata=self.transaction.get_connection_meta())

    def get_data(self, schema, options, secret_data, filter, aggregation, start, end, granularity):
        params = {
            'options': options,
            'secret_data': secret_data,
            'filter': filter,
            'aggregation': aggregation,
            'start': start,
            'end': end,
            'granularity': granularity
        }

        if schema:
            params.update({
                'schema': schema
            })

        _LOGGER.debug(f'[get_data] {params}')
        responses = self.client.Billing.get_data(params, metadata=self.transaction.get_connection_meta())
        return self._change_message(responses)

    @staticmethod
    def _change_message(message):
        return MessageToDict(message, preserving_proto_field_name=True)
