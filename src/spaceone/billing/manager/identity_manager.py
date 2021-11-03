import logging

from spaceone.core.manager import BaseManager
from spaceone.core.connector.space_connector import SpaceConnector

_LOGGER = logging.getLogger(__name__)
_GET_RESOURCE_METHODS = {
    'identity.Project': {
        'dispatch_method': 'Project.get',
        'key': 'project_id'
    },
    'identity.ServiceAccount': {
        'dispatch_method': 'ServiceAccount.get',
        'key': 'service_account_id'
    },
}

class IdentityManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.identity_connector: SpaceConnector = self.locator.get_connector('SpaceConnector', service='identity')

    def get_resource(self, resource_id, resource_type, domain_id):
        get_method = _GET_RESOURCE_METHODS[resource_type]
        return self.identity_connector.dispatch(get_method['dispatch_method'],
                                                {get_method['key']: resource_id, 'domain_id': domain_id})

    def get_resource_key(self, resource_type, resource_info, reference_keys):
        return None

    def check_project(self, project_id, domain_id):
        return self.identity_connector.dispatch('Project.get', {'project_id': project_id, 'domain_id': domain_id})

    def list_projects_by_project_group_id(self, project_group_id, domain_id):
        response = self.identity_connector.dispatch('ProjectGroup.list_projects',
                                                    {'project_group_id': project_group_id,
                                                     'recursive': True,
                                                     'query': {'only': ['project_id']},
                                                     'domain_id': domain_id})

        project_list = []
        if 'results' in response:
            for result in response['results']:
                project_list.append(result['project_id'])
        return project_list

    def list_all_projects(self, domain_id):
        response = self.identity_connector.dispatch('Project.list',
                                                    {'query': {'only': ['project_id']},
                                                     'domain_id':domain_id})

        project_list = []
        if 'results' in response:
            for result in response['results']:
                project_list.append(result['project_id'])
        return project_list

    def list_service_accounts_by_provider(self, provider, domain_id):
        response = self.identity_connector.dispatch('ServiceAccount.list',
                                                    {'provider': provider, 'domain_id': domain_id})

        if 'results' in response:
            return response['results']
        return []
