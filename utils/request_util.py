import asyncio
import json

import aiohttp


class Response:
    text = ""
    status_code = 0
    content = b""
    headers = {}
    cookies = {}
    url = ""
    method = ""

    def json(self):
        return json.loads(self.text)


class AioHttpRequest(object):
    async def get(self, url, headers=None, params=None, proxy=None, allow_redirects=True, timeout=20, verify=None):
        resp = Response()
        async with aiohttp.ClientSession() as ssesion:
            response = await ssesion.get(url, headers=headers, params=params, allow_redirects=allow_redirects,
                                         proxy=proxy, timeout=timeout, verify_ssl=verify)
            resp.text = await response.text()
            resp.status_code = response.status
            resp.headers = dict(response.headers)
            resp.cookies = response.cookies
            resp.method = response.method
            return resp

    async def post(self, url, headers=None, params=None, data=None, json=None, proxy=None, allow_redirects=True,
                   timeout=20, verify=None):
        resp = Response()
        async with aiohttp.ClientSession() as ssesion:
            response = await ssesion.post(url, headers=headers, params=params, data=data, json=json,
                                          allow_redirects=allow_redirects, proxy=proxy, timeout=timeout, verify_ssl=verify)
            resp.text = await response.text()
            resp.status_code = response.status
            resp.headers = dict(response.headers)
            resp.cookies = response.cookies
            resp.method = response.method
            return resp


requests = AioHttpRequest()


class AioHttpRequestV1(object):
    def __init__(self, session):
        self.session = session

    async def get(self, url, headers=None, params=None, proxy=None, allow_redirects=True, timeout=20, verify=None):
        resp = Response()
        response = await self.session.get(url, headers=headers, params=params, allow_redirects=allow_redirects,
                                          proxy=proxy, timeout=timeout, verify_ssl=verify)
        resp.text = await response.text()
        resp.status_code = response.status
        resp.headers = dict(response.headers)
        resp.cookies = response.cookies
        resp.method = response.method
        return resp

    async def post(self, url, headers=None, params=None, data=None, json=None, proxy=None, allow_redirects=True,
                   timeout=20, verify=None):
        resp = Response()
        response = await self.session.post(url, headers=headers, params=params, data=data, json=json,
                                           allow_redirects=allow_redirects, proxy=proxy, timeout=timeout,
                                           verify_ssl=verify)
        resp.text = await response.text()
        resp.status_code = response.status
        resp.headers = dict(response.headers)
        resp.cookies = response.cookies
        resp.method = response.method
        return resp
