from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class DataSourceTag(EmbeddedDocument):
    key = StringField(max_length=255)
    value = StringField(max_length=255)


class PluginInfo(EmbeddedDocument):
    plugin_id = StringField(max_length=40)
    version = StringField(max_length=255)
    options = DictField()
    metadata = DictField()
    secret_id = StringField(max_length=40, default=None, null=True)
    provider = StringField(max_length=40, default=None, null=True)

    def to_dict(self):
        return self.to_mongo()


class DataSource(MongoModel):
    data_source_id = StringField(max_length=40, generate_id='ds', unique=True)
    name = StringField(max_length=255, unique_with='domain_id')
    state = StringField(max_length=20, default='ENABLED', choices=('ENABLED', 'DISABLED'))
    provider = StringField(max_length=40, default=None, null=True)
    capability = DictField()
    plugin_info = EmbeddedDocumentField(PluginInfo, default=None, null=True)
    tags = ListField(EmbeddedDocumentField(DataSourceTag))
    domain_id = StringField(max_length=255)
    created_at = DateTimeField(auto_now_add=True)

    meta = {
        'updatable_fields': [
            'name',
            'plugin_info',
            'state',
            'tags'
        ],
        'minimal_fields': [
            'data_source_id',
            'name',
            'state',
            'provider'
        ],
        'ordering': [
            'name'
        ],
        'indexes': [
            'data_source_id',
            'state',
            'provider',
            'domain_id',
            ('tags.key', 'tags.value')
        ]
    }
