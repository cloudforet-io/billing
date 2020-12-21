import logging

from spaceone.core import utils
from spaceone.core.manager import BaseManager
from spaceone.billing.error import *
from spaceone.billing.connector.identity_connector import IdentityConnector

_LOGGER = logging.getLogger(__name__)

_GET_RESOURCE_METHODS = {
    'identity.Project': 'get_project',
    'identity.ServiceAccount': 'get_service_account',
}


class IdentityManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.identity_connector: IdentityConnector = self.locator.get_connector('IdentityConnector')

    def get_resource(self, resource_id, resource_type, domain_id):
        get_method = _GET_RESOURCE_METHODS[resource_type]
        return getattr(self.identity_connector, get_method)(resource_id, domain_id)

    def get_resource_key(self, resource_type, resource_info, reference_keys):
        return None

    def check_project(self, project_id, domain_id):
        response = self.identity_connector.get_project(project_id, domain_id)
        return response

    def list_projects_by_project_group_id(self, project_group_id, domain_id):
        response = self.identity_connector.list_projects_by_project_group_id(project_gorup_id, domain_id)
        project_list = []
        if 'results' in response:
            for result in response['results']:
                project_list.append(result['project_id'])
        return project_list

    def list_all_projects(self, domain_id):
        response = self.identity_connector.list_projects(domain_id)
        project_list = []
        if 'results' in response:
            for result in response['results']:
                project_list.append(result['project_id'])
        return project_list

    def list_service_accounts_by_provider(self, provider, domain_id):
        response = self.identity_connector.list_service_accounts_by_provider(provider, domain_id)
        if 'results' in response:
            return response['results']
        return []
