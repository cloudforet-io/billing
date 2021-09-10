import logging

from spaceone.core.cache import cacheable
from spaceone.core.manager import BaseManager
from spaceone.billing.error import *
from spaceone.billing.connector.plugin_connector import PluginConnector
from spaceone.billing.connector.billing_plugin_connector import BillingPluginConnector

_LOGGER = logging.getLogger(__name__)


class PluginManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.plugin_connector: PluginConnector = self.locator.get_connector('PluginConnector')
        self.mp_connector: BillingPluginConnector = self.locator.get_connector('BillingPluginConnector')

    def initialize(self, plugin_info, domain_id):
        _LOGGER.debug(f'[initialize] plugin_info: {plugin_info}')

        plugin_id = plugin_info['plugin_id']
        upgrade_mode = plugin_info.get('upgrade_mode', 'AUTO')

        if upgrade_mode == 'AUTO':
            endpoint_response = self.plugin_connector.get_plugin_endpoint(plugin_id, domain_id)
        else:
            endpoint_response = self.plugin_connector.get_plugin_endpoint(plugin_id, domain_id,
                                                                          version=plugin_info.get('version'))

        endpoint = endpoint_response['endpoint']
        _LOGGER.debug(f'[init_plugin] endpoint: {endpoint}')
        self.mp_connector.initialize(endpoint)

        return endpoint_response

    def init_plugin(self, options):
        plugin_info = self.mp_connector.init(options)

        _LOGGER.debug(f'[plugin_info] {plugin_info}')
        return plugin_info.get('metadata', {})

    def call_init_plugin(self, options):
        metadata = self.mp_connector.init(options)
        return metadata

    def verify_plugin(self, options, secret_data, billing_type):
        plugin_info = self.mp_connector.init(options)

        _LOGGER.debug(f'[plugin_info] {plugin_info}')
        plugin_options = plugin_info.get('metadata', {})

        self._validate_plugin_option(plugin_options, billing_type)
        return plugin_options

    @cacheable(key='billing:{cache_key}', expire=3600)
    def get_data(self, schema, options, secret_data, filter, aggregation, start, end, granularity, cache_key):
        """
        Args:
            schema: str
            options: dict
            secret_data: dict
            filter: dict
            aggregation: list
            start: str
            end: str
            granularity: str
            cache_key: str for data caching
        """
        billing_data_info = self.mp_connector.get_data(schema, options, secret_data, filter, aggregation, start, end, granularity)

        return billing_data_info

