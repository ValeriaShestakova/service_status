import yaml
from aiohttp import web
import ipaddress
import aiopg.sa


def get_config(path):
    with open(path) as f:
        config = yaml.load(f)
    return config


async def index(request):
    return web.Response(text='Hello in Service Status App!')


async def get_records_by_ip(request):
    resp = []
    service_ip = request.match_info['ip']
    try:
        ipaddress.ip_address(service_ip)
    except ValueError:
        raise web.HTTPBadRequest()
    async with request.app['db'].acquire() as conn:
        services = await conn.execute("""SELECT ip, port, available FROM
                                      services WHERE ip=%s;""", (service_ip,))
        if services.rowcount == 0:
            raise web.HTTPNotFound()
        async for s in services:
            dict = {}
            dict['available'] = s[2]
            dict['ip'] = s[0]
            dict['port'] = s[1]
            resp.append(dict)
        return web.json_response(resp)


async def get_records_by_ip_and_port(request):
    resp = []
    service_ip = request.match_info['ip']
    try:
        service_port = int(request.match_info['port'])
    except ValueError:
        raise web.HTTPBadRequest
    try:
        ipaddress.ip_address(service_ip)
    except ValueError:
        raise web.HTTPBadRequest()

    if int(service_port) > 65535:
        raise web.HTTPBadRequest
    async with request.app['db'].acquire() as conn:
        services = await conn.execute("""SELECT ip, port, available FROM
                                      services WHERE ip=%s;""", (service_ip,))
        if services.rowcount == 0:
            raise web.HTTPNotFound()
        async for s in services:
            if s[1] == service_port:
                dict = {}
                dict['available'] = s[2]
                dict['ip'] = s[0]
                dict['port'] = s[1]
                resp.append(dict)
        return web.json_response(resp)


def setup_routes(app):
    app.router.add_get('/', index)
    app.router.add_get('/service_status/get_records_by_ip/{ip}'
                       , get_records_by_ip)
    app.router.add_get('/service_status/get_records_by_ip_and_port/{ip}'
                       '/{port}'
                       , get_records_by_ip_and_port)


async def init_pg(app):
    conf = app['config']['postgres']
    engine = await aiopg.sa.create_engine(
        database=conf['database'],
        user=conf['user'],
        password=conf['password'],
        host=conf['host'],
        port=conf['port'],
    )
    app['db'] = engine


async def close_pg(app):
    app['db'].close()
    await app['db'].wait_closed()


if __name__ == '__main__':
    app = web.Application()
    config = get_config('config.yaml')
    app['config'] = config
    app.on_startup.append(init_pg)
    app.on_cleanup.append(close_pg)
    setup_routes(app)
    web.run_app(app)

