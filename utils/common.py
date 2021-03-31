import json
import sys
import time

sys.path.append('/data/web/pyflow')

from sanic.response import json
from .message import CODE_MSG_DICT

from utils.log import write_log
log = write_log(work_name="request_body_common_log")


def responseFormat(data=None, code=None):
    """
    响应格式
    :param data: 返回数据
    :param code: 返回状态码
    :return:
    """
    if data is None:
        data = {}
    timestamp = int(time.time())
    format_result = {
        "cache_time": timestamp,
        "code": code,
        "data": data,
        "msg": CODE_MSG_DICT[str(code)],
    }
    return format_result


def global_catch_exception(func):
    """
    全局捕获异常装饰器
    :param func: 装饰函数
    :return:
    """
    async def wrapper(request, *args, **kwargs):
        try:
            log.info("接口参数数据 {}".format(request.body))
            result = await func(request, *args, **kwargs)
            return result
        except Exception as e:
            log.exception("接口发生错误 {}".format(e))
            data = {"err": str(e)}
            err_result = responseFormat(data, 500)
            return json(err_result)
    return wrapper


def bodyFormat(url, headers, method, parameters=None, proxy=None):
    """
    通用返回请求体格式
    :param url:
    :param headers:
    :param method:  GET,POST,FORM_POST,HEAD,FORM_FILES
    :param parameters:
    :param proxy:
    :return:
    """
    body = {
        "url": url,
        "headers": headers,
        "method": method,
    }
    if parameters is not None:
        body["parameters"] = parameters
    if proxy is not None:
        body["proxy"] = proxy
    return body