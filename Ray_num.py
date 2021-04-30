#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：公司政策爬虫 
@File    ：Ray_num.py
@IDE     ：PyCharm 
@Author  ：13192272582
@Date    ：2021/1/21 16:20 
'''
import execjs
import re
from utils import log
from utils.request_util import  requests
from utils.async_proxy_util import *
from aiohttp import ClientConnectorError, ClientOSError, ClientHttpProxyError
from concurrent.futures import TimeoutError
async  def Ray_html(url,headers,logger):
    """
    加载瑞数js，解决部分湖北上海网页有反爬
    :param url: 网址
    :return: str页面源码
    """
    js = r"""
            const jsdom = require("jsdom");
            const { JSDOM } = jsdom;

            function genCookie(ht, ck, link) {
                var options = {
                    referrer: link,
                    userAgent: "Mozilla/5.0 (X11; OpenBSD i386) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36",
                    // resources: "usable",
                    runScripts: "dangerously",
                    url: link,
                    beforeParse(window) {
                        window.js = 'none';

                        window.setInterval = function (s, b) {
                            // eval(s)
                            return 1
                        };
                        window.setTimeout = function (s, b) {
                            // eval(s)
                            return 1
                        }
                    },
                    // virtualConsole: new jsdom.VirtualConsole(),
                    cookieJar: new jsdom.CookieJar(),
                };

                options.cookieJar.setCookie(ck, link, cp);

                function cp(val) {
                    if (val) {
                    }
                }
                const dom = (new JSDOM(ht, options));
                const window = dom.window;
                cookies = window.document.cookie;
                return cookies
            }

            async function asyncGenCookie(ht, ck, kstype, link) {
                var options = {
                    referrer: link,
                    userAgent: "Mozilla/5.0 (X11; OpenBSD i386) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36",
                    // resources: "usable",
                    runScripts: "dangerously",
                    url: link,
                    beforeParse(window) {
                        window.js = 'none';

                        window.setInterval = function (s, b) {
                            // eval(s)
                            return 1
                        };
                        window.setTimeout = function (s, b) {
                            // eval(s)
                            return 1
                        }
                    },
                    // virtualConsole: new jsdom.VirtualConsole(),
                    cookieJar: new jsdom.CookieJar(),
                };

                var vv_ck = "";
                if (kstype === "TOEFL") {
                    vv_ck = "E2SAlsDbQXmI443T=none";
                }

                if (ck !== vv_ck) {
                    options.cookieJar.setCookie(ck, link, cp);
                }

                function cp(val) {
                    if (val) {
                    }
                }
                const dom = (new JSDOM(ht, options));
                const window = dom.window;
                cookies = window.document.cookie;
                return cookies
            }

            function encryptParam(ht, ck, link, target) {
                var options = {
                    referrer: link,
                    userAgent: "Mozilla/5.0 (X11; OpenBSD i386) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36",
                    // resources: "usable",
                    runScripts: "dangerously",
                    url: link,
                    beforeParse(window) {
                        window.js = 'none';

                        window.setInterval = function (s, b) {
                            // eval(s)
                            return 1
                        };
                        window.setTimeout = function (s, b) {
                            // eval(s)
                            return 1
                        }
                    },
                    // virtualConsole: new jsdom.VirtualConsole(),
                    cookieJar: new jsdom.CookieJar(),
                };

                options.cookieJar.setCookie(ck, link, cp);

                function cp(val) {
                    if (val) {
                    }
                }
                const dom = (new JSDOM(ht, options));
                const window = dom.window;
            }

            module.exports = {
                asyncGenCookie
            };"""
    if headers==None:
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

    a_num = 0
    while a_num < 3:
        try:
            proxy=await get_proxy()
            if a_num==0:
                proxy=None
            html =await requests.get(url, headers=headers, timeout=10, proxies=proxy,verify=False)
            f_js = execjs.compile(js, cwd=r'C:\Program Files\nodejs\node_cache\node_modules')
            s80 = html.headers['Set-Cookie'].split(";")[0]
            cookie = f_js.call("genCookie", html.text, s80, url)
            headers["cookie"] = cookie
            html =await  requests.get(url, headers=headers, proxies=proxy, timeout=15,verify=False)
            if html.status_code == 200:
                info = Ray_parse_html(html)
                return info
            else:
                a_num += 1
        except (ClientHttpProxyError, ClientOSError, ClientConnectorError, TimeoutError) as e:
            logger.error(f"瑞树js 政策 {url} get  代理出错 {a_num}次")
        except Exception as err:
            logger.error("瑞树网页爬取失败 url: {} error: {} 当前循环次数{}".format(url,err,str(a_num+1)))
            a_num += 1

    return None


def Ray_proxies():
    """
    获取代理ip
    :return: 字典形式代理ip
    """
    url = "http://192.168.5.101:5020/"
    headers = {
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36'

    }

    data = requests.get(url, headers=headers, timeout=(10, 10))
    k = data.text

    proxies = {
        "http": 'http://' + str(k.replace('"', '').strip()),
        # "https":'http://' + str(k.replace('"', '').strip()),
    }
    return proxies


def Ray_parse_html(data):
    """
    万能解码函数
    :param data: 网页requests源码
    :return: str网页源码
    """
    info = None
    try:
        info = data.content.decode("utf-8")
    except Exception as err:
        pass
    if info == None:
        try:
            info = data.content.decode("GBK")
        except Exception as err:
            pass
    if info == None:
        try:
            info = data.content.decode("gb2312")
        except Exception as err:
            pass
    if info == None:
        try:
            info = data.content.decode("GB18030")
        except Exception as err:
            pass
    if info == None:
        try:
            type = Ray_pick_charset(data.text)
            info = data.content.decode(type, 'ignore')
        except Exception as err:
            print(err)
    return info


def Ray_pick_charset(html):
    """
    从文本中提取 meta charset
    :param html:
    :return:
    """
    charset = "utf-8"
    m = re.compile('<meta .*(http-equiv="?Content-Type"?.*)?charset="?([a-zA-Z0-9_-]+)"?', re.I).search(html)
    if m and m.lastindex == 2:
        charset = m.group(2).lower()
    return charset