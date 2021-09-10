DATABASE_AUTO_CREATE_INDEX = True
DATABASE_CASE_INSENSITIVE_INDEX = False
DATABASES = {
    'default': {
        'db': 'billing',
        'host': 'localhost',
        'port': 27017,
        'username': '',
        'password': ''
    }
}

CACHES = {
    'default': {},
    'local': {
        'backend': 'spaceone.core.cache.local_cache.LocalCache',
        'max_size': 128,
        'ttl': 300
    }
}

HANDLERS = {
}

CONNECTORS = {
    'IdentityConnector': {
    },
    'InventoryConnector': {
    },
    'PluginConnector': {
    },
    'RepositoryConnector': {
    },
    'SecretConnector': {
    },
    'BillingPluginConnector': {
    },
}

INSTALLED_DATA_SOURCE_PLUGINS = [
    # {
    #     'name': '',
    #     'plugin_info': {
    #         'plugin_id': '',
    #         'version': '',
    #         'options': {},
    #         'secret_data': {},
    #         'schema': '',
    #         'upgrade_mode': ''
    #     },
    #     'tags':{
    #         'description': ''
    #     }
    # }
]