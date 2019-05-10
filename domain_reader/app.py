import falcon

from platform_sdk.domain.reader import DomainReader

from .api import resources, settings


api = falcon.API()
api_version = 1

domain_reader = DomainReader(settings.ORM)
domain_resource = resources.DomainResource(domain_reader)

api.add_route(
    settings.BASE_URI[api_version] + '{solution}/{app}/{map}/{filter}/{query}', domain_resource)
