"""
Deprecated:
  Not used. Integrated with SpaceConnector.
"""

import logging

from google.protobuf.json_format import MessageToDict

from spaceone.core.connector import BaseConnector
from spaceone.core import pygrpc
from spaceone.core.utils import parse_grpc_endpoint
from spaceone.core.error import *

__all__ = ['IdentityConnector']

_LOGGER = logging.getLogger(__name__)


class IdentityConnector(BaseConnector):

    def __init__(self, transaction, config):
        super().__init__(transaction, config)
        self._check_config()
        self._init_client()

    def _init_client(self):
        for version, uri in self.config['endpoint'].items():
            e = parse_grpc_endpoint(uri)
            self.client = pygrpc.client(endpoint=e['endpoint'], ssl_enabled=e['ssl_enabled'])

    def _check_config(self):
        if 'endpoint' not in self.config:
            raise ERROR_CONNECTOR_CONFIGURATION(backend=self.__class__.__name__)

        if len(self.config['endpoint']) > 1:
            raise ERROR_CONNECTOR_CONFIGURATION(backend=self.__class__.__name__)

    def get_project(self, project_id, domain_id):
        response = self.client.Project.get({
            'project_id': project_id,
            'domain_id': domain_id
        }, metadata=self.transaction.get_connection_meta())

        return self._change_message(response)

    def get_service_account(self, service_account_id, domain_id):
        response = self.client.ServiceAccount.get({
            'service_account_id': service_account_id,
            'domain_id': domain_id
        }, metadata=self.transaction.get_connection_meta())

        return self._change_message(response)

    def list_service_accounts_by_provider(self, provider, domain_id):
        response = self.client.ServiceAccount.list({
            'provider': provider,
            'domain_id': domain_id
        }, metadata=self.transaction.get_connection_meta())
        return self._change_message(response)

    def list_projects(self, domain_id):
        query = {'only': ['project_id']}
        response = self.client.Project.list({
            'query': query,
            'domain_id':domain_id
        }, metadata=self.transaction.get_connection_meta())
        return self._change_message(response)

    def list_projects_by_project_group_id(self, project_group_id, domain_id):
        query = {'only': ['project_id']}
        reponse = self.client.ProjectGroup.list_projects({
            'project_group_id': project_group_id,
            'recursive': True,
            'query': query,
            'domain_id': domain_id
        }, metadata=self.transaction.get_connection_meta())

        return self._change_message(response)

    @staticmethod
    def _change_message(message):
        return MessageToDict(message, preserving_proto_field_name=True)
