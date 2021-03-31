#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：公司政策爬虫 
@File    ：aiohttps_GP.py
@IDE     ：PyCharm 
@Author  ：13192272582
@Date    ：2021/1/21 15:39 
'''

import aiohttp
import re
from urllib.parse import urljoin
from utils.request_util import requests
from  utils.async_proxy_util import *
import  json
from  utils.UA_util import *
urls = ['http://yjglt.nmg.gov.cn/api/Data/GetContents', 'http://tyj.hlj.gov.cn/hljtyjapi/governmentAffairs/infoGuideList']

status_code=[404,407]
async def Aiohttp_get(url, cookie,proxies,headers,logger):
    """33
    asycn def   aiohttp get请求
    :param url:
    :return:3 代表网页是附件格式 2 代表重定向 1 代表网页源代码
    """
    if not  headers:
        if cookie:
            headers = {
                'User-Agent': get_random_ua(3),
                'Cookie': cookie,
                'Referer': url
            }
        else:
            headers = {
                'User-Agent': get_random_ua(3),
                'Referer': url
            }

    headers['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3'
    if "http://xfj.shanxi.gov.cn/app/api-xfwz" in url:
        headers['Authorization'] = "Bearer 9f3b8387-8a8d-43e4-9599-4396aadca475"
    if "https://www.neijiang.gov.cn/" in url:
        headers['X-PJAX']="X-PJAX"
    response=await  requests.get(url,headers=headers,timeout=10,proxy=proxies,verify=False)
    if response.status  not  in status_code :
        pd_headers = str(response.headers)
        if 'filename' in pd_headers or 'application/pdf' in pd_headers:
            age_text = "annex"
            return  await parsr_html(response),3
        else:
            age_text = await parsr_html(response)
            if '<script>window.location.href=' in age_text:
                next_url = urljoin(url, "".join(re.compile("<script>window.location.href='(.*?)';").findall(age_text)).strip())
                return  next_url,2
            elif '<script>location.replace' in age_text:
                next_url = urljoin(url, "".join(re.compile("<script>location.replace\('(.*?)'\);").findall(age_text)).strip())
                return  next_url,2
            elif 'http-equiv="refresh" content="0;url=' in age_text:
                next_url = urljoin(url, "".join(re.compile('http-equiv="refresh" content="0;url=(.*?)"').findall(age_text)).strip())
                return next_url,2
            else:
                return  age_text,1
    else:
        logger.info("当前网页 {} 状态码 {} 请检查网址".format(url,response.status_code))


async def Aiohttp_post(url, cookie,proxies,post_data,headers,logger):
    """
    asycn def   aiohttp get请求
    :param url:
    :return:3 代表网页是附件格式 2 代表重定向 1 代表网页源代码
    """
    if not headers:
        if cookie:
            headers = {
                'User-Agent': get_random_ua(3),
                'Cookie': cookie
            }
        else:
            headers = {
                'User-Agent': get_random_ua(3),
            }
    headers['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3'
    if "http://xfj.shanxi.gov.cn/app/api-xfwz" in url:
        headers['Authorization'] = "Bearer 9f3b8387-8a8d-43e4-9599-4396aadca475"
    # post_data=json.loads(post_data)
    if "*^*"  in url:
        url=url.split("*^*")[0]
    try:
        # post_data = json.loads(post_data)
        if url in urls:
            post_data = str(post_data)
            headers['Content-Type'] = 'application/json;charset=UTF-8'
        elif "&%#&" in url:
            post_data = str(post_data)
            headers['Content-Type'] = 'application/json'
            url = url.replace("&%#&", '')
    except:
        logger.error("网址 {} post请求data数据转换失败 {}".format(url,str(post_data)))
        post_data = None


    response=await requests.post(url,headers=headers,proxy=proxies,timeout=15,verify=False,data=post_data)
    if response.status  not  in status_code:
        age_text = await parsr_html(response)
        if '<script>window.location.href=' in age_text:
            next_url = urljoin(url, "".join(re.compile("<script>window.location.href='(.*?)';").findall(age_text)).strip())
            return  next_url,2
        elif '<script>location.replace' in age_text:
            next_url = urljoin(url, "".join(re.compile("<script>location.replace\('(.*?)'\);").findall(age_text)).strip())
            return  next_url,2
        elif 'http-equiv="refresh" content="0;url=' in age_text:
            next_url = urljoin(url, "".join(re.compile('http-equiv="refresh" content="0;url=(.*?)"').findall(age_text)).strip())
            return next_url,2
        else:
            return  age_text,1
    else:
        logger.info("当前post网页 {} 状态码 {} 请检查网址".format(url,response.status_code))




async def Aiohttp_ip():
    """
    获取代理ip
    :return: 字典形式代理ip
    """

    url = "http://192.168.5.101:5020/"
    async with aiohttp.ClientSession() as session:  # 获取session
        async with session.request('GET', url) as resp:  # 提出请求
            k = await resp.text("utf-8")
    return 'http://' + str(k.replace('"', '').strip())



async def parsr_html(response):
    """
    网页转码
    :param response:
    :return:
    """
    try:
        age_text = await response.text("utf-8")
    except Exception as err:
        age_text = None
    if age_text == None:
        try:
            age_text = await response.text("GBK")
        except:
            age_text = None
    if age_text == None:
        try:
            age_text = await response.text("gb2312")
        except:
            age_text = None
    if age_text == None:
        try:
            age_text = await response.text("GB18030")
        except:
            age_text = None
    if age_text == None:
        code = re.compile('<meta .*(http-equiv="?Content-Type"?.*)?charset="?([a-zA-Z0-9_-]+)"?', re.I).search(await response.text(errors="ignore"))
        if code and code.lastindex == 2:
            code = code.group(2).lower()
        else:
            code = "utf-8"
        age_text = await response.text(code, "ignore")
    return age_text

