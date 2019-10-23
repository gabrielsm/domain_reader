from peewee import SQL
from pydash import objects
import autologging

from platform_sdk.domain.schema.api import SchemaApi

from .mapper import RemoteField, RemoteMap
from .sql import QueryParser


@autologging.logged
class DomainReader:
    QUERIES = {
        'not_deleted': '(deleted is null or not deleted)',

        'where_branch': '(branch in (%s, \'master\')) and '
                        'id not in (select from_id from entities.{table} where from_id is not null and branch=%s) AND '
    }

    def __init__(self, orm, db_settings, schema_settings):
        self.orm = orm
        self.db_settings = db_settings
        self.schema_api = SchemaApi(schema_settings)
        self._create_db(orm, db_settings)

        self._trace_local = lambda v, m: \
            self._DomainReader__log.log(
                msg=f'{v}:{m}', level=autologging.TRACE)

    def _create_db(self, orm, db_settings):
        self.db = orm.db_factory('postgres', **db_settings)()

    def get_data(self, _map, _type, filter_name, params, history=False):
        """
        Gets the data from database, based on the given parameters.
        :param _map: EntityMap is the main entity.
        :param _type: The data type or class of object.
        :param filter_name: Filter name as it was given in the configuration file: "byId".
        :param params: Filter expression, that will be converted into into SQL Query filters.
        :param history: A bool to determine if we should get data from entity or its history.
        :return: Dataset object containing the data found.
        """
        api_response = self.schema_api.get_schema(_map, _type)

        if api_response:
            model = self._get_model(api_response['model'], api_response['fields'] + api_response['metadata'], history)
            sql_filter = self._get_sql_filter(filter_name, api_response['filters'], history)

            branch = params.get('branch')
            table = objects.get(api_response, 'model.table')

            page = params.get('page')
            page_size = params.get('page_size')
            params = {k: v for k, v in params.items() if k and v}
            self._trace_local('params', params)

            sql_query = self._get_sql_query(sql_filter, params)
            self._trace_local('sql_query', sql_query)

            # Data may come from current entity or history
            data = self._obtain_data(api_response, branch, history, model, page, page_size, sql_query, table)

            return list(self._get_response_data(data, api_response['fields'], api_response['metadata']))

    def _obtain_data(self, api_response, branch, history: bool, model, page, page_size, sql_query, table):
        """
        Gets the data either from the original entity or its history. Obtain data works by checking if history is
        empty and still needs to return the first record.
        :param api_response: Data schema to be returned.
        :param branch: Branch name.
        :param history: Bool. Try to get data from entity´s history.
        :param model: Entity model from Domain.
        :param page: Which page it should return.
        :param page_size: Page size. Values should be reasonable or query may take a long time or timeout.
        :param sql_query: Query to be performed in database
        :param table: Entity to perform query.
        :return: Returns a dataset from an entity or its history.
        """
        # Get data from the entity
        data = self._execute_query(model, table, branch, sql_query, page, page_size)

        # If data from history is empty, means the record is on the original entity.
        if history and not data:
            model = self._get_model(api_response['model'], api_response['fields'] + api_response['metadata'],
                                    False)
            data = self._execute_query(model, table, branch, sql_query, page, page_size)
        return data

    def _execute_query(self, model, table, branch, sql_query, page, page_size=20):  # pragma: no cover
        try:
            self.db.connect(reuse_if_open=True)
            with self.db.atomic():
                query_user = ''
                query_params = ()
                proxy_model = model.build(self.db)
                query = proxy_model.select()
                query_not_deleted = self.QUERIES['not_deleted']

                if sql_query and sql_query['sql_query']:
                    query_user = sql_query['sql_query']
                    query_params = sql_query['query_params']

                if not branch:
                    branch = 'master'
                query_branch = self.QUERIES['where_branch'].format(table=table)
                query_params = (branch, branch,) + query_params

                if query_user != '':
                    query_not_deleted += ' AND '

                query = query.where(SQL(
                    query_branch + query_not_deleted + query_user,
                    query_params
                ))

                if page and page_size:
                    query = query.paginate(int(page), int(page_size))

                return query
        except Exception as e:
            self._trace_local('##### _execute_query ##### ERROR ', e)
        finally:
            self.db.close()

    @staticmethod
    def _get_sql_query(sql_filter, params):
        if sql_filter and params:
            parser = QueryParser(sql_filter)
            query, params = parser.parse(params)
            return {
                'sql_query': query,
                'query_params': params
            }

    @staticmethod
    def _get_response_data(entities, fields, metadata):
        if entities:
            for entity in entities:
                dic = {field['alias']: getattr(entity, field['alias']) for field in fields}
                dic['_metadata'] = {meta['alias']: getattr(entity, meta['alias']) for meta in metadata}
                yield dic

    @staticmethod
    def _get_fields(fields):
        return [RemoteField(
            f['alias'], f['field_type'], f['column_name']) for f in fields]

    def _get_model(self, model, fields, history=False):
        return RemoteMap(model['name'], model['table'], self._get_fields(fields), self.orm, history)

    @staticmethod
    def _get_sql_filter(filter_name, filters, history):
        if history:
            return 'id = :id'
        if filters and filter_name:
            return next(f['expression'] for f in filters if f['name'] == filter_name)
