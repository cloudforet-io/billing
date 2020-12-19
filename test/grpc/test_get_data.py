import os
import uuid
import unittest
import random

from datetime import datetime, timedelta

from spaceone.core.utils import random_string
from spaceone.core.unittest.runner import RichTestRunner

from spaceone.tester import TestCase, print_json

PLUGIN_ID='plugin-xxxxxxx'
VERSION='0.2'
PROJECT_ID='project-8ca8edea20c0'
PROJECT_ID='project-a2370f6003ad'

PROVIDER=os.environ.get('PROVIDER', 'spaceone')

# aggregation = PROJECT | SERVICE_ACCOUNT | REGION_CODE | RESOURCE_TYPE

class TestGetData(TestCase):

    def _create_data_source(self):
        params = {
            "name": "aws-hyperbilling",
            "plugin_info": {
              "plugin_id": PLUGIN_ID,
              "version": VERSION,
              "options": {},
              "provider": PROVIDER
              },
              "domain_id": self.domain.domain_id
            }
        self.billing.DataSource.register(params, metadata=self.meta)

    def test_get_data(self):
        """ Get data
        """
        #self._create_data_source()

        param = {'domain_id': self.domain.domain_id}
        response = self.billing.DataSource.list(param, metadata=self.meta)
        print_json(response)
        start = '2020-10-01'
        end = '2020-12-17'
        granularity = 'MONTHLY'
        param = {"start": start, "end": end, "granularity": granularity,
            "domain_id": self.domain.domain_id}
        repos = self.billing.Billing.get_data(param, metadata=self.meta)
        print(repos)

    def _test_get_data_by_region(self):
        """ Get data
        """
        #self._create_data_source()

        param = {'domain_id': self.domain.domain_id}
        response = self.billing.DataSource.list(param, metadata=self.meta)
        print_json(response)
        end = datetime.utcnow()
        start = end - timedelta(days=60)
        granularity = 'MONTHLY'
        aggregation = ['REGION']
        param = {"start": start, "end": end, "granularity": granularity, "aggregation": aggregation,
            "domain_id": self.domain.domain_id}
        repos = self.billing.Billing.get_data(param, metadata=self.meta)
        print(repos)

    def _test_get_data_by_project(self):
        """ Get data
        """
        #self._create_data_source()

        param = {'domain_id': self.domain.domain_id}
        response = self.billing.DataSource.list(param, metadata=self.meta)
        print_json(response)
        start = "2020-10-01"
        end = "2020-12-10"
        granularity = 'DAILY'
        granularity = 'MONTHLY'
        aggregation = []
        param = {"project_id": PROJECT_ID, "start": start, "end": end, "granularity": granularity, "aggregation": aggregation,
            "domain_id": self.domain.domain_id}
        repos = self.billing.Billing.get_data(param, metadata=self.meta)
        print(repos)


if __name__ == "__main__":
    unittest.main(testRunner=RichTestRunner)
