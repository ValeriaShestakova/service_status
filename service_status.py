import yaml
from aiohttp import web
import asyncio
import ipaddress
import aiopg.sa
import socket
from contextlib import closing

TIMEOUT = 0.1


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


async def update_db(app, available, id):
    async with app['db'].acquire() as conn:
        await conn.execute(
            """UPDATE services SET available=(%s) WHERE id=(%s);""",
            (available, id))


async def check_available(ip, port):
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.settimeout(TIMEOUT)
        if sock.connect_ex((ip, port)) == 0:
            return True
        else:
            return False


async def update_service_available(app):
    while True:
        async with app['db'].acquire() as conn:
            services = await conn.execute("""SELECT id, ip, port, available FROM
                                             services;""")
            async for service in services:
                available = await check_available(service[1],
                                                  service[2])
                if service[3] is not available:
                    await update_db(app, available, service[0])
        await asyncio.sleep(30)


async def start_background_tasks(app):
    conf = app['config']['postgres']
    engine = await aiopg.sa.create_engine(
        database=conf['database'],
        user=conf['user'],
        password=conf['password'],
        host=conf['host'],
        port=conf['port'],
    )
    app['db'] = engine
    app['update_available'] = app.loop.create_task(update_service_available(app))


async def cleanup_background_tasks(app):
    app['update_available'].cancel()
    await app['update_available']
    app['db'].close()
    await app['db'].wait_closed()


def setup_routes(app):
    app.router.add_get('/', index)
    app.router.add_get('/service_status/get_records_by_ip/{ip}'
                       , get_records_by_ip)
    app.router.add_get('/service_status/get_records_by_ip_and_port/{ip}'
                       '/{port}'
                       , get_records_by_ip_and_port)


if __name__ == '__main__':
    app = web.Application()
    config = get_config('config.yaml')
    app['config'] = config
    app.on_startup.append(start_background_tasks)
    app.on_cleanup.append(cleanup_background_tasks)
    setup_routes(app)
    web.run_app(app)
