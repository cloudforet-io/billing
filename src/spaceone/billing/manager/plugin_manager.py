import logging

from spaceone.core.cache import cacheable
from spaceone.core.manager import BaseManager
from spaceone.core.connector.space_connector import SpaceConnector
from spaceone.billing.connector.billing_plugin_connector import BillingPluginConnector
from spaceone.billing.model.data_source_model import DataSource

_LOGGER = logging.getLogger(__name__)


class PluginManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.plugin_connector: SpaceConnector = self.locator.get_connector('SpaceConnector', service='plugin')
        self.billing_plugin_connector: BillingPluginConnector = self.locator.get_connector('BillingPluginConnector')

    def initialize(self, endpoint):
        _LOGGER.debug(f'[initialize] data source plugin endpoint: {endpoint}')
        self.billing_plugin_connector.initialize(endpoint)

    def init_plugin(self, options):
        plugin_info = self.billing_plugin_connector.init(options)

        _LOGGER.debug(f'[plugin_info] {plugin_info}')
        plugin_metadata = plugin_info.get('metadata', {})

        return plugin_metadata

    def verify_plugin(self, options, secret_data, schema):
        self.billing_plugin_connector.verify(options, secret_data, schema)

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
        billing_data_info = self.billing_plugin_connector.get_data(schema, options, secret_data, filter, aggregation, start, end, granularity)

        return billing_data_info

    def get_billing_plugin_endpoint_by_vo(self, data_source_vo: DataSource):
        plugin_info = data_source_vo.plugin_info.to_dict()
        endpoint, updated_version = self.get_billing_plugin_endpoint(plugin_info, data_source_vo.domain_id)

        if updated_version:
            _LOGGER.debug(f'[get_billing_plugin_endpoint_by_vo] upgrade plugin version: {plugin_info["version"]} -> {updated_version}')
            self.upgrade_billing_plugin_version(data_source_vo, endpoint, updated_version)

        return endpoint

    def get_billing_plugin_endpoint(self, plugin_info, domain_id):
        plugin_id = plugin_info['plugin_id']
        version = plugin_info.get('version')
        upgrade_mode = plugin_info.get('upgrade_mode', 'AUTO')

        response = self.plugin_connector.dispatch('Plugin.get_plugin_endpoint',
                                                  {'plugin_id': plugin_id,
                                                   'version': version,
                                                   'upgrade_mode': upgrade_mode,
                                                   'domain_id': domain_id})

        return response.get('endpoint'), response.get('updated_version')

    def upgrade_billing_plugin_version(self, data_source_vo: DataSource, endpoint, updated_version):
        plugin_info = data_source_vo.plugin_info.to_dict()
        self.initialize(endpoint)
        plugin_metadata = self.init_plugin(plugin_info.get('options', {}))
        plugin_info['version'] = updated_version
        plugin_info['metadata'] = plugin_metadata
        data_source_vo.update({'plugin_info': plugin_info})
