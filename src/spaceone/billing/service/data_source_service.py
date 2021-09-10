import logging

from spaceone.core.service import *
from spaceone.core import cache
from spaceone.core import config
from spaceone.core import utils
from spaceone.billing.error import *
from spaceone.billing.manager.repository_manager import RepositoryManager
from spaceone.billing.manager.secret_manager import SecretManager
from spaceone.billing.manager.plugin_manager import PluginManager
from spaceone.billing.manager.data_source_manager import DataSourceManager

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class DataSourceService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_source_mgr: DataSourceManager = self.locator.get_manager('DataSourceManager')
        self.plugin_mgr: PluginManager = self.locator.get_manager('PluginManager')

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['name', 'plugin_info', 'domain_id'])
    def register(self, params):
        """Register data source

        Args:
            params (dict): {
                'name': 'str',
                'billing_type': 'str',
                'plugin_info': 'dict',
                'tags': 'dict',
                'domain_id': 'str'
            }

        Returns:
            data_source_vo (object)
        """
        domain_id = params['domain_id']

        if 'tags' in params:
            params['tags'] = utils.dict_to_tags(params['tags'])

        self._check_plugin_info(params['plugin_info'])
        plugin_info = self._get_plugin(params['plugin_info'], domain_id)
        params['provider'] = plugin_info.get('provider')

        # Update metadata
        endpoint, updated_version = self.plugin_mgr.get_billing_plugin_endpoint(params['plugin_info'], domain_id)
        if updated_version:
            params['plugin_info']['version'] = updated_version

        options = params['plugin_info'].get('options', {})
        plugin_metadata = self._init_plugin(endpoint, options)
        params['plugin_info']['metadata'] = plugin_metadata

        return self.data_source_mgr.register_data_source(params)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['data_source_id', 'domain_id'])
    def update(self, params):
        """Update data source

        Args:
            params (dict): {
                'data_source_id': 'str',
                'name': 'dict',
                'plugin_info': 'dict',
                'tags': 'dict'
                'domain_id': 'str'
            }

        Returns:
            data_source_vo (object)
        """
        data_source_id = params['data_source_id']
        domain_id = params['domain_id']
        data_source_vo = self.data_source_mgr.get_data_source(data_source_id, domain_id)

        if 'tags' in params:
            params['tags'] = utils.dict_to_tags(params['tags'])

        if 'plugin_info' in params:
            self._check_plugin_info(params['plugin_info'])

            if params['plugin_info']['plugin_id'] != data_source_vo.plugin_info.plugin_id:
                raise ERROR_NOT_ALLOWED_PLUGIN_ID(old_plugin_id=data_source_vo.plugin_info.plugin_id,
                                                  new_plugin_id=params['plugin_info']['plugin_id'])

            # Update metadata
            endpoint, updated_version = self.plugin_mgr.get_billing_plugin_endpoint(params['plugin_info'], domain_id)
            if updated_version:
                params['plugin_info']['version'] = updated_version

            options = params['plugin_info'].get('options', {})
            plugin_metadata = self._init_plugin(endpoint, options)
            params['plugin_info']['metadata'] = plugin_metadata

        return self.data_source_mgr.update_data_source_by_vo(params, data_source_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['data_source_id', 'domain_id'])
    def enable(self, params):
        """ Enable data source

        Args:
            params (dict): {
                'data_source_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            data_source_vo (object)
        """

        data_source_id = params['data_source_id']
        domain_id = params['domain_id']
        data_source_vo = self.data_source_mgr.get_data_source(data_source_id, domain_id)

        return self.data_source_mgr.update_data_source_by_vo({'state': 'ENABLED'},
                                                             data_source_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['data_source_id', 'domain_id'])
    def disable(self, params):
        """ Disable data source

        Args:
            params (dict): {
                'data_source_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            data_source_vo (object)
        """

        data_source_id = params['data_source_id']
        domain_id = params['domain_id']
        data_source_vo = self.data_source_mgr.get_data_source(data_source_id, domain_id)

        return self.data_source_mgr.update_data_source_by_vo({'state': 'DISABLED'},
                                                             data_source_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['data_source_id', 'domain_id'])
    def deregister(self, params):
        """Deregister data source

        Args:
            params (dict): {
                'data_source_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            None
        """

        self.data_source_mgr.deregister_data_source(params['data_source_id'], params['domain_id'])

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['data_source_id', 'domain_id'])
    def verify_plugin(self, params):
        """ Verify data source plugin

        Args:
            params (dict): {
                'data_source_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            data_source_vo (object)
        """

        data_source_id = params['data_source_id']
        domain_id = params['domain_id']
        data_source_vo = self.data_source_mgr.get_data_source(data_source_id, domain_id)

        return {'status': True}

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['data_source_id', 'domain_id'])
    def get(self, params):
        """ Get data source

        Args:
            params (dict): {
                'data_source_id': 'str',
                'domain_id': 'str',
                'only': 'list
            }

        Returns:
            data_source_vo (object)
        """
        domain_id = params['domain_id']
        self._initialize_data_sources(domain_id)

        return self.data_source_mgr.get_data_source(params['data_source_id'], domain_id, params.get('only'))

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['domain_id'])
    @append_query_filter(['data_source_id', 'name', 'state', 'provider', 'domain_id'])
    @change_tag_filter('tags')
    @append_keyword_filter(['data_source_id', 'name', 'provider'])
    def list(self, params):
        """ List data sources

        Args:
            params (dict): {
                'data_source_id': 'str',
                'name': 'str',
                'state': 'str',
                'provider': 'str',
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.Query)'
            }

        Returns:
            data_source_vos (object)
            total_count
        """

        domain_id = params['domain_id']
        query = params.get('query', {})

        self._initialize_data_sources(domain_id)

        return self.data_source_mgr.list_data_sources(query)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id'])
    @change_tag_filter('tags')
    @append_keyword_filter(['data_source_id', 'name', 'provider'])
    def stat(self, params):
        """
        Args:
            params (dict): {
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)'
            }

        Returns:
            values (list) : 'list of statistics data'

        """

        query = params.get('query', {})
        return self.data_source_mgr.stat_data_sources(query)

    @staticmethod
    def _check_plugin_info(plugin_info_params):
        if 'plugin_id' not in plugin_info_params:
            raise ERROR_REQUIRED_PARAMETER(key='plugin_info.plugin_id')

        secret_id = plugin_info_params.get('secret_id')
        provider = plugin_info_params.get('provider')

        if secret_id is None and provider is None:
            raise ERROR_REQUIRED_PARAMETER(key='plugin_info.[secret_id | provider]')

    def _get_plugin(self, plugin_info, domain_id):
        plugin_id = plugin_info['plugin_id']

        repo_mgr: RepositoryManager = self.locator.get_manager('RepositoryManager')
        return repo_mgr.get_plugin(plugin_id, domain_id)

    def _init_plugin(self, endpoint, options):
        self.plugin_mgr.initialize(endpoint)
        return self.plugin_mgr.init_plugin(options)

    @cache.cacheable(key='init-data-source:{domain_id}', expire=300)
    def _initialize_data_sources(self, domain_id):
        _LOGGER.debug(f'[_initialize_data_source] domain_id: {domain_id}')

        query = {'filter': [{'k': 'domain_id', 'v': domain_id, 'o': 'eq'}]}
        data_source_vos, total_count = self.data_source_mgr.list_data_sources(query)

        installed_data_sources_ids = [data_source_vo.plugin_info.plugin_id for data_source_vo in data_source_vos]
        _LOGGER.debug(f'[_initialize_data_source] Installed Plugins : {installed_data_sources_ids}')

        global_conf = config.get_global()
        for _data_source in global_conf.get('INSTALLED_DATA_SOURCE_PLUGINS', []):
            if _data_source['plugin_info']['plugin_id'] not in installed_data_sources_ids:
                try:
                    _LOGGER.debug(
                        f'[_initialize_data_source] Create init data source: {_data_source["plugin_info"]["plugin_id"]}')
                    _data_source['domain_id'] = domain_id
                    self.register(_data_source)
                except Exception as e:
                    _LOGGER.error(f'[_initialize_data_source] {e}')

        return True
