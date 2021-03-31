"""
测试转换请求头
"""
from utils import log
import asyncio
from utils.request_util import  requests
from utils.async_proxy_util import *
logger = log.write_log(work_name='瑞树js')

async def get():
    url = "http://fgw.hubei.gov.cn/fbjd/tzgg/tz/202005/t20200512_2271315.shtml"
    headers = {
        'Connection': 'keep-alive',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Referer': url,
        'Accept-Language': 'zh-CN,zh;q=0.9',
    }
    data = await requests.get(url, headers=headers, timeout=10)
    print(data.text)
    print(data.headers)
    s80 = data.headers['Set-Cookie'].split(";")[0]
    print(s80)
    try:
        a==None
    except Exception as err:
        logger.info(err)

asyncio.run(get())