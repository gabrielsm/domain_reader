from pydash import objects
from datetime import datetime
from autologging import logged

from platform_sdk.process_memory import ProcessMemoryApi
from .sql_executor_base import SqlExecutorBase
import uuid


@logged
class DomainWriter(SqlExecutorBase):
    QUERIES = {
        'insert': 'INSERT INTO entities.{table} ({columns}) VALUES ({values});',
        'update': 'UPDATE entities.{table} SET {values} WHERE id=%s;',
        'count_entity': 'SELECT count(1) FROM entities.{table} WHERE id=%s;'
    }

    HOLDERS = {
        "'": '"',
        "%": '%%'
    }

    def __init__(self, orm, db_settings, process_memory_settings=None):
        super().__init__(orm, db_settings)
        self.query_translator = str.maketrans(self.HOLDERS)
        if process_memory_settings:
            self.process_memory_api = ProcessMemoryApi(process_memory_settings)

    def save_data(self, process_memory_id):
        data = self.process_memory_api.get_process_memory_data(process_memory_id)
        content = objects.get(data, 'map.content')
        entities = objects.get(data, 'dataset.entities')
        instance_id = objects.get(data, 'instanceId')
        reproduction_id = objects.get(data, 'ReproductionId')

        if content and entities:
            bulk_sql = self._get_bulk_sql(entities, content, instance_id, reproduction_id)
            self._execute_query(bulk_sql)
            return True

    def _get_bulk_sql(self, data, schemas, instance_id, reproduction_id):
        for _type, entities in data.items():
            if entities:
                schema = self._get_schema(schemas)[_type]
                table = schema['table']
                fields = schema['fields']

                for entity in entities:
                    entity['meta_instance_id'] = instance_id
                    branch = objects.get(entity, '_metadata.branch')
                    change_track = objects.get(entity, '_metadata.changeTrack')
                    from_id = objects.get(entity, '_metadata.from_id')

                    if self._is_update_from_reproduction(change_track, instance_id, reproduction_id):
                        self.fill_updated_reproduction_entity(change_track, entity)
                        # we will create a new entity when updating one when in reproduction
                        yield from self._create(entity, fields, from_id, table)
                    if self._is_update_on_master(branch, change_track, instance_id):
                        yield from self._update_on_master(branch, change_track, entity, fields, instance_id, table)
                    elif self._is_update_on_branch(branch, change_track, instance_id):
                        yield from self._update_on_branch(change_track, entity, fields, from_id, instance_id, table)
                    elif self._is_create(change_track):
                        if reproduction_id:
                            entity['reproduction_from_id'] = entity['id']

                        yield from self._create(entity, fields, from_id, table)

    @staticmethod
    def fill_updated_reproduction_entity(change_track, entity):
        entity['deleted'] = change_track == 'destroy'
        entity['modified'] = datetime.now()
        entity['reproduction_from_id'] = entity['id']
        entity['id'] = uuid.uuid4()

    @staticmethod
    def _is_update_from_reproduction(change_track, instance_id, reproduction_id):
        return change_track and change_track in {'update', 'destroy'} and instance_id and reproduction_id

    def _update_on_master(self, change_track, entity, fields, table):
        yield self._get_update_sql(entity['id'], table, entity, fields, change_track)

    def _update_on_branch(self, change_track, entity, fields, from_id, table):
        if self._entity_exists(entity, table):
            yield self._get_update_sql(entity['id'], table, entity, fields, change_track, from_id)
        else:
            yield self._get_insert_sql(table, entity, fields, from_id)

    def _create(self, entity, fields, from_id, table):
        yield self._get_insert_sql(table, entity, fields, from_id)

    def _entity_exists(self, entity, table):
        count_entity = self._execute_scalar_query(
            self.QUERIES['count_entity'].format(table=table), (entity['id'],)
        )
        return count_entity > 0

    @staticmethod
    def _is_update_on_branch(branch, change_track, instance_id):
        return change_track and change_track in {'update', 'destroy'} and instance_id and branch != 'master'

    @staticmethod
    def _is_create(change_track):
        return change_track and change_track == 'create'

    @staticmethod
    def _is_update_on_master(branch, change_track, instance_id):
        return change_track and change_track in {'update', 'destroy'} and instance_id and branch == 'master'

    def _get_update_sql(self, entity_id, table, entity, fields, change_track, from_id=None):
        if from_id:
            entity['from_id'] = from_id

        entity['deleted'] = change_track == 'destroy'
        entity['branch'] = objects.get(entity, '_metadata.branch')
        entity['modified'] = datetime.now()

        values = [f"{field['column']}=%s" for field in fields]
        params = tuple(entity[field['name']] if field['name'] in entity else None for field in fields)
        params += (entity_id,)  # entity id parameter

        return {
            'query': self.QUERIES['update'].format(
                table=table,
                values=str.join(',', values)),
            'params': params
        }

    def _get_insert_sql(self, table, entity, fields, from_id):
        if from_id:
            entity['from_id'] = from_id

        if not any(field.get('column') == 'id' for field in fields):
            fields.append({'name': 'id', 'column': 'id'})

        entity['branch'] = objects.get(entity, '_metadata.branch')

        columns = [field['column'] for field in fields if field['name'] in entity]

        values = ['%s' for field in fields if field['name'] in entity]

        params = tuple(entity[field['name']] for field in fields if field['name'] in entity)

        return {
            'query': self.QUERIES['insert'].format(
                table=table,
                values=str.join(',', values),
                columns=str.join(',', columns)
            ),
            'params': params
        }

    @staticmethod
    def _get_schema(content):
        schema = {}
        for key in content.keys():
            fields = content[key]['fields']
            fields['deleted'] = {'name': 'deleted', 'column': 'deleted'}
            fields['meta_instance_id'] = {'name': 'meta_instance_id', 'column': 'meta_instance_id'}
            fields['modified'] = {'name': 'modified', 'column': 'modified'}
            fields['from_id'] = {'name': 'from_id', 'column': 'from_id'}
            fields['branch'] = {'name': 'branch', 'column': 'branch'}

            schema[key] = {
                'table': content[key]['model'],
                'fields': [{'name': k, 'column': v['column']} for k, v in fields.items()]
            }

        return schema
