import logging
import hashlib
import json
import re

import pandas as pd

from dateutil.parser import parse
from dateutil.relativedelta import relativedelta

from spaceone.core.service import *

from spaceone.billing.error import *
from spaceone.billing.manager.identity_manager import IdentityManager
from spaceone.billing.manager.secret_manager import SecretManager
from spaceone.billing.manager.data_source_manager import DataSourceManager
from spaceone.billing.manager.plugin_manager import PluginManager

_LOGGER = logging.getLogger(__name__)

# MultiIndex of pandas
AGGR_MAP = {
    'identity.Provider': 'provider',
    'identity.Project': 'project_id',
    'identity.ServiceAccount': 'service_account_id',
    'inventory.Region': 'region_code',
    'inventory.CloudServiceType': 'service_code'
}

DEFAULT_CURRENCY = 'USD'


def _dict_hash(data):
    """ hash dictionary
    return hex digest
    """
    data_hash = hashlib.md5()
    encoded = json.dumps(data, sort_keys=True).encode()
    data_hash.update(encoded)
    return data_hash.hexdigest()


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class BillingService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.identity_mgr: IdentityManager = self.locator.get_manager('IdentityManager')
        self.secret_mgr: SecretManager = self.locator.get_manager('SecretManager')
        self.data_source_mgr: DataSourceManager = self.locator.get_manager('DataSourceManager')
        self.plugin_mgr: PluginManager = self.locator.get_manager('PluginManager')

    @transaction(append_meta={
        'authorization.scope': 'PROJECT',
        'mutation.append_parameter': {'user_projects': 'authorization.projects'}
    })
    @check_required(['start', 'end', 'granularity', 'domain_id'])
    def get_data(self, params):
        """ Get billing data

        Args:
            params (dict): {
                'project_id': 'str',
                'project_group_id': 'str',
                'service_accounts': 'list',
                'filter': 'dict',
                'aggregation': 'list',
                'start': 'timestamp',
                'end': 'timestamp',
                'granularity': 'str',
                'domain_id': 'str',
                'sort': 'dict',
                'limit': 'int',
                'user_projects': 'list', // from meta
            }

        Examples:
            sort = {'date': '2020-12', 'desc': True}
        Returns:
            billing_data_info (list)
        """
        params = self._check_params(params)

        domain_id = params['domain_id']
        # Get possible service_account list from DataSources
        project_id = params.get('project_id', None)
        project_group_id = params.get('project_group_id', None)
        service_accounts = params.get('service_accounts', [])
        aggregation = params.get('aggregation', [])
        sort = params.get('sort', {'desc': True})
        limit = params.get('limit', None)
        self.currency = params.get('currency', DEFAULT_CURRENCY)

        # Initialize plugin_mgr
        # caching endpoints
        # data_source : {'label': 'endpoint'}
        self.merged_data = None
        endpoint_dic = {}
        possible_service_accounts = self._get_possible_service_accounts(domain_id, project_id, project_group_id, service_accounts)

        if possible_service_accounts == {}:
            # nothing to do
            return {'results': [], 'total_count': 0}

        _LOGGER.debug(f'[get_data] {possible_service_accounts}')
        data_arrays_list = []
        for (service_account_id, plugin_info) in possible_service_accounts.items():
            # get secret from service account
            secrets_info = self.secret_mgr.list_secrets_by_service_account_id(service_account_id, domain_id)
            for secret in secrets_info['results']:
                try:
                    secret_id = secret['secret_id']
                    secret_data = self.secret_mgr.get_secret_data(secret_id, domain_id)
                    # call plugin_manager for get data
                    # get data
                    param_for_plugin = {
                        'schema': secret['schema'],
                        'options': {},
                        'secret_data': secret_data,
                        'filter': {},
                        'aggregation': self._get_plugin_aggregation(aggregation),
                        'start': params['start'],
                        'end': params['end'],
                        'granularity': params['granularity'],
                    }
                    param_for_plugin['cache_key'] = self._make_cache_key(param_for_plugin, domain_id)
                    self.plugin_mgr.init_plugin(plugin_info['plugin_id'], plugin_info['version'], domain_id)
                    response = self.plugin_mgr.get_data(**param_for_plugin)
                    data_arrays = self._make_data_arrays(response, service_account_id, secret['project_id'])
                    data_arrays_list.extend(data_arrays)
                except Exception as e:
                    _LOGGER.error(f'[get_data] fail to get_data by {secret_id}, skip.....')

        _LOGGER.debug(f'[get_data] {data_arrays_list}')
        # Make DataFrame from data_arrays_list
        data_frames = pd.DataFrame(data_arrays_list)
        data_frames.fillna(0, inplace=True)

        try:
            result = self._get_aggregated_data(data_frames, aggregation, sort, limit)
        except Exception as e:
            # Nothing to aggregation
            return {'results': [], 'total_count': 0}
        # make to output format
        try:
            result = self._create_result(result, domain_id)
            return result
        except Exception as e:
            raise ERROR_BILLING_CREATE_RESULT(params=params)

    def _make_data_arrays(self, result, service_account_id, project_id):
        results = result.get('results', [])
        data_arrays = []
        for result in results:
            resource_type = result['resource_type'] + \
                f'&identity.Project={project_id}&identity.ServiceAccount={service_account_id}'
            fields = self._parse_resource_type(resource_type)
            billing_data = result['billing_data']
            single_data = fields.copy()
            for billing_info in billing_data:
                date = billing_info['date']
                cost = billing_info.get('cost', 0)
                currency = billing_info.get('currency', 'USD')
                single_data[date] = cost
            data_arrays.append(single_data)
        return data_arrays

    @staticmethod
    def _parse_resource_type(res_type):
        """ Return dict
        example
        {
            'resource_type': 'inventory.CloudService',
            'provider': 'aws',
            'region_code': 'ap-northeast-2'
            ...
        }
        """
        item = res_type.split('?')
        result = {'resource_type': item[0]}
        if len(item) > 1:
            query = item[1].split('&')
        else:
            query = []
        for q_item in query:
            (a,b) = q_item.split('=')
            result[a] = b
        return result

    def _create_result(self, df, domain_id):
        """ From DataFrame, create sult
        """
        index = df.index.names
        result = []
        count = 0
        for column_name, item in df.iterrows():
            res_info = self._create_resource_info(index, column_name)
            data = res_info.copy()
            sorted_cost = self._create_cost_data(item)
            data['billing_data'] = sorted_cost
            result.append(data)
            count += 1
        return {'results': result, 'total_count': count}

    def _get_last_date(self, df):
        """ Find last date for automatic sorting

        ex. provider    2020-10   2020-11   2020-12
            aws         10        20        30

        return 2020-12
        """
        date_columns = sorted(df.columns)
        return date_columns[-1]

    @staticmethod
    def _create_resource_info(index, value):
        """
        return:
        {
            'resource_type': 'inventory.CloudService?provider=aws&....',
            'identity.Project': {'project_id': 'project-1234'},
            'identity.ServiceAccount': {'service_account_id': 'sa-1234'},
            ...
        }
        """
        if isinstance(value,str):
            res_type = value + "?"
        else:
            # else tuple
            res_type = value[0] + "?"
        result = {}
        for idx in range(len(index) - 1):
            key = index[idx+1]
            val = value[idx+1]
            res_type = f"{res_type}{key}={val}&"
            result[key] = {AGGR_MAP[key]: val}
        result['resource_type'] = res_type[:-1]

        return result

    def _create_cost_data(self, cost):
        cost_dict = cost.to_dict()
        sorted_cost = sorted(cost_dict.items())
        result = []
        for item in sorted_cost:
            value = {'date': item[0], 'cost': item[1], 'currency': self.currency }
            result.append(value)
        return result

    def _get_aggregated_data(self, dataframe, aggregation, sort=None, limit=None):
        """ processing DataFrame
            1) aggregation
            2) sort
            3) limit

        Args:
            aggregation: list, ['PROJECT', 'SERVICE_ACCOUNT', 'REGION_CODE', 'RESOURCE_TYPE', None]

        aggregation is based on resource_type

        self.merged_data(DataFrame) :
            resource_type       provider     region_code      project_id   service_account_id   2020-10  2020-11  2020-12
            ----------------------------------------------------------------------------------+---------------------------
            inventory.CloudService   aws     ap-northeast-2  project-1111  sa-1111              10        12       30
            inventory.CloudService   aws     ap-northeast-2  project-1111  sa-2222              20        22       40
            inventory.CloudService   gcp     us-east-2       project-3333  sa-3333              2         50       100


        """
        # Based on aggregation
        # append group_by filter
        group_by = ['resource_type'] + aggregation

        # 1. aggregation
        grouped_data = dataframe.groupby(group_by).sum()
        _LOGGER.debug(f'\n\n[1. Aggregation]{group_by}\n {grouped_data}')
        """
        ##################################################
        resource_type           identity.Project      2020-10    2020-11    2020-12
        inventory.CloudService  project-1111          30         34         70
        inventory.CloudService  project-3333          2          50         100
        """

        # 2. Sort (default sort: desc=true, date={last date})
        print(f"#### Get values by sort request: {sort} ###")
        """
        resource_type           identity.Project
        inventory.CloudService  project-3e8c54e8c59a   -36463.706052
                                project-8b31217811f1   -72927.412105
                                project-f182e4c8ff5d   -36463.706052
        """
        desc = sort.get('desc', True)
        date = sort.get('date', self._get_last_date(grouped_data))

        if desc:
            ascending = False
        else:
            ascending = True
        grouped_data = grouped_data.sort_values(by=[date], ascending=ascending)
        _LOGGER.debug(f'\n\n[2. Sort]{sort}\n {grouped_data}')

        # 3. Limit
        if limit:
            grouped_data = grouped_data.iloc[:limit]
            _LOGGER.debug(f'\n\n[3. Limit]{limit}\n {grouped_data}')

        return grouped_data

    def _get_possible_service_accounts(self, domain_id, project_id=None, project_group_id=None, service_accounts=[]):
        """ Find possible service account list

        Returns:
            {
                service_account_id: {plugin_info}
                ...
            }
        """
        if len(service_accounts) > 0:
            # TODO: fix
            return service_accounts

        # get project_list
        project_list = []
        if project_id and self.identity_mgr.check_project(project_id, domain_id):
            project_list = [project_id]
        elif project_group_id:
            project_list = self.identity_mgr.list_projects_by_project_group_id(project_group_id, domain_id)
        else:
            project_list = self.identity_mgr.list_all_projects(domain_id)

        results = {}
        query = {'filter': [{'k': 'domain_id', 'v': domain_id, 'o': 'eq'}]}
        (data_source_vos, total_count) = self.data_source_mgr.list_data_sources(query)
        for data_source_vo in data_source_vos:
            if self._check_data_source_state(data_source_vo) == False:
                # Do nothing
                continue
            # Find all service accounts with data_source.provider
            service_accounts_by_provider = self.identity_mgr.list_service_accounts_by_provider(data_source_vo.provider, domain_id)
            _LOGGER.debug(f'[_get_possible_service_accounts] service_accounts: {service_accounts_by_provider}')
            for service_account in service_accounts_by_provider:
                # check project_id
                if service_account['project_info']['project_id'] in project_list:
                    data_source_dict = data_source_vo.to_dict()
                    results[service_account['service_account_id']] = data_source_dict['plugin_info']
        return results

    def _get_plugin_aggregation(self, aggregation):
        """ Return for aggregation list for plugin
        plugin only support, inventory.Region, inventory.CloudServiceType
        """
        supported = ['inventory.Region', 'inventory.CloudServiceType']
        return list(set(supported) & set(aggregation))

    @staticmethod
    def _check_data_source_state(data_source_vo):
        if data_source_vo.state == 'DISABLED':
            return False
        return True

    @staticmethod
    def _make_cache_key(params, domain_id):
        """ from params, create cache key
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
        cache_key = f'billing:{domain_id}:{_dict_hash(params)}'
        return cache_key

    @staticmethod
    def _check_params(params):
        """ check params

        if
        start=yyyy-mm --> yyyy-mm-01
        end=yyyy-mm   --> yyyy-mm-dd (last day)
        """
        new_params = params.copy()
        r1 = re.compile('^20\d\d-\d\d$')
        r2 = re.compile('^20\d\d-\d\d-\d\d$')
        start = params['start']
        end = params['end']

        # convert start
        if r1.match(start):
            start_date = parse(start+"-01")
        elif r2.match(start):
            start_date = parse(start)
        else:
            raise ERROR_BILLING_REQUEST_FORMAT(key='start', example='yyyy-mm | yyyy-mm-dd')

        if r1.match(end):
            end_date = parse(end) + relativedelta(day=31)
        elif r2.match(end):
            end_date = parse(end)
        else:
            raise ERROR_BILLING_REQUEST_FORMAT(key='start', example='yyyy-mm | yyyy-mm-dd')

        new_params['start'] = start_date.strftime('%Y-%m-%d')
        new_params['end'] = end_date.strftime('%Y-%m-%d')

        _LOGGER.debug(f'[_check_params] check start, end : \n{params} \n {new_params}')

        return new_params
