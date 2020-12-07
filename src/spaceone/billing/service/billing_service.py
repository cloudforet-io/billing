import logging
import traceback

import pandas as pd

from spaceone.core.service import *

from spaceone.billing.error import *
from spaceone.billing.manager.identity_manager import IdentityManager
from spaceone.billing.manager.secret_manager import SecretManager
from spaceone.billing.manager.data_source_manager import DataSourceManager
from spaceone.billing.manager.plugin_manager import PluginManager

_LOGGER = logging.getLogger(__name__)

AGGR_MAP = {
    'REGION': 'region_code',
    'RESOURCE_TYPE': 'service_code'
}

@authentication_handler
@authorization_handler
@event_handler
class BillingService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.identity_mgr: IdentityManager = self.locator.get_manager('IdentityManager')
        self.secret_mgr: SecretManager = self.locator.get_manager('SecretManager')
        self.data_source_mgr: DataSourceManager = self.locator.get_manager('DataSourceManager')
        self.plugin_mgr: PluginManager = self.locator.get_manager('PluginManager')

    @transaction
    @check_required(['start', 'end', 'granularity', 'domain_id'])
    @change_timestamp_value(['start', 'end'], timestamp_format='iso8601')
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
                'domain_id': 'str'
            }

        Returns:
            billing_data_info (list)
        """
        domain_id = params['domain_id']
        # Get possible service_account list from DataSources
        project_id = params.get('project_id', None)
        project_group_id = params.get('project_group_id', None)
        service_accounts = params.get('service_accounts', [])
        aggregation = params.get('aggregation', [])

        # Initialize plugin_mgr
        # caching endpoints
        # data_source : {'label': 'endpont'}
        self.merged_data = None
        endpoint_dic = {}
        possible_service_accounts = self._get_possible_service_accounts(domain_id, project_id, project_group_id, service_accounts)
        _LOGGER.debug(f'[get_data] {possible_service_accounts}')
        dataframe_list = []
        for (service_account_id, plugin_info) in possible_service_accounts.items():
            # get secret from service accunt
            secrets_info = self.secret_mgr.list_secrets_by_service_account_id(service_account_id, domain_id)
            for secret in secrets_info['results']:
                secret_id = secret['secret_id']
                secret_data = self.secret_mgr.get_secret_data(secret_id, domain_id)
                # call plugin_manager for get data
                # get data
                param_for_plugin = {
                    'schema': 'aws_hyperbilling',
                    'options': {},
                    'secret_data': secret_data,
                    'filter': {},
                    'aggregation': aggregation,
                    'start': params['start'],
                    'end': params['end'],
                    'granularity': params['granularity']
                }
                self.plugin_mgr.init_plugin(plugin_info['plugin_id'], plugin_info['version'], domain_id)
                response = self.plugin_mgr.get_data(**param_for_plugin)
                print(response)
                df = self._make_dataframe(response)
                print(df)
                dataframe_list.append(df)

        _LOGGER.debug(f'[get_data] {dataframe_list}')
        # Merge All data
        merged_data = pd.concat(dataframe_list)

        result = self._get_aggregated_data(merged_data, aggregation)

        # make to output format
        return self._create_result(result, domain_id)


    def _make_dataframe(self, result):
        results = result.get('results', [])
        multi_index = []
        cost_list = []
        for result in results:
            #resource_type = result['resource_type']
            (multiple_index, columns) = self._parse_resource_type(result['resource_type'])
            columns.append('date')
            billing_data = result['billing_data']
            for billing_info in billing_data:
                date = billing_info['date']
                cost = billing_info.get('cost', 0)
                currency = billing_info.get('currency', 'USD')
                index = multiple_index.copy()
                index.append(date)
                multi_index.append(index)
                cost_list.append(cost)

        # crete DataFrame

        df = pd.DataFrame(multi_index, columns=columns)
        idx = pd.MultiIndex.from_frame(df)
        s1 = pd.Series(cost_list, index=idx)
        return s1

    @staticmethod
    def _parse_resource_type(res_type):
        """ Return
        (multiple_index, columns)
        """
        item = res_type.split('?')
        multiple_index = [item[0]]
        columns = ['resource_type']
        if len(item) > 1:
            query = item[1].split('&')
        else:
            query = []
        for q_item in query:
            (a,b) = q_item.split('=')
            multiple_index.append(b)
            columns.append(a)

        return (multiple_index, columns)

    def _create_result(self, df, domain_id):
        """ From df, create result
        """
        index = df.index.names
        dict_vaule = df.to_dict()
        result = {}
        for k,v in dict_vaule.items():
            # k (res_type, ..., date): cost
            resource_type = k[0] + "?"
            id = 0
            for id in range(len(k)-2):
                resource_type = f'{resource_type}{index[id+1]}={k[id+1]}&'
            date = k[-1]
            cost = v
            value = result.get(resource_type[:-1], [])
            value.append({'date': date, 'cost': cost, 'currency': 'USD'})
            result[resource_type[:-1]] = value
        _LOGGER.debug(f'[_create_result] {result}')
        output = []
        total_count = 0
        for k,v in result.items():
            total_count += 1
            output.append({'resource_type': k, 'billing_data': v, 'domain_id': domain_id})
        return output

    def _get_aggregated_data(self, data, aggregation):
        """ processing self.merged_data based on aggregation

        Args:
            aggregation: list, ['REGION', 'RESOURCE_TYPE', None]

        aggregation is based on resource_type

        self.merged_data(DataFrame) :
            resource_type       provider     region_code                 date          cost
            ---------------------------------------------------------------------------------
            inventory.CloudService   aws     ap-northeast-2             2020-10         12
                                                                        2020-11         13
                                                                        2020-12         16


            inventory.CloudService   gcp     us-east-2                  2020-10         12
                                                                        2020-11         13
                                                                        2020-12         16


        """
        # Based on aggregation
        # append group_by filter
        group_by = ['resource_type']
        for aggr in aggregation:
            group_by.append(AGGR_MAP[aggr])
        # Last add date by
        group_by.append('date')

       # processing
        processed_data = data.groupby(level=group_by).sum()
        return processed_data

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
        if project_id:
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

    @staticmethod
    def _check_data_source_state(data_source_vo):
        if data_source_vo.state == 'DISABLED':
            return False
        return True

