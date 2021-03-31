import json

from utils.log import write_log
from utils.request_util import requests
logger = write_log(work_name="proxy")



async def get_proxy():
    """
    存储-获取模式
    :param platform: 2808、dubsix
    :param want_type:
    :return:
    """
    url = "http://192.168.5.101:5020/"
    response = await requests.get(url, timeout=8)
    k = await response.text("utf-8")
    return 'http://' + str(k.replace('"', '').strip())

