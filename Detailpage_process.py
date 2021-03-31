#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：公司政策爬虫 
@File    ：Detailpage_process.py
@IDE     ：PyCharm 
@Author  ：13192272582
@Date    ：2021/1/21 15:58 
'''
import hashlib

from lxml import  etree
from urllib import parse
import re
from datetime import datetime
from urllib.parse import urljoin
from lxml.html import tostring
import jsonpath
import  json
from bs4 import BeautifulSoup
import requests
import  traceback


def get_suffix(name):
    """
    获取附件名字尾缀函数

    :return: 尾缀
    """
    if name==None:
        name=''
    all_name = ['.doc', '.pdf', '.xls', '.ppt', '.zip', '.rar', '.tiff', '.ofd', '.wps', '.jpg', '.png', '.ceb', '.et']
    op_name = ['.docx', '.xlsx', '.pptx']
    for j in op_name:
        if j in name.lower():
            break
        else:
            j = ''
            continue
    if not j:
        for i in all_name:
            if i in name.lower():
                break
            else:
                i = ''
    else:
        i = j
    return i.replace(".", '')


def DPP_contentr_e(logger,**kwargs):
    html = kwargs['html']
    title_type = kwargs['title_type']
    host_name = kwargs['host_name']
    judge_time = kwargs['judge_time']
    content_tag = kwargs['content_tag']
    content_tag_end = kwargs['content_tag_end']
    url = kwargs['url']
    judge_model = kwargs['judge_model']
    pub_time = kwargs['pub_time']
    accessory_xpath = kwargs['accessory_xpath']
    accessory_re = kwargs['accessory_re']
    accessory_judge = kwargs['accessory_judge']
    title_details = kwargs['title_details']
    re_time = kwargs['re_time']
    xpath_time = kwargs['xpath_time']
    try:
        title = kwargs['title']
    except:
        title = '本网页'
    if "http://www.shptdj.cn/website/Ajax/content_0.ashx" in url or "http://gkml.dbw.cn/gkml/web/data/detail.ashx?" in url:
        url_org = parse.unquote(html)
        html = url_org.replace("%u", '\\u').encode('utf-8').decode('unicode_escape')
    html_x = etree.HTML(html)
    source = source_pd(html_x)  #####获取来源
    if not source or len(source) >= 10:
        source = host_name
    text = ''
    if int(judge_time) > 3:
        c_time = xq_public_time(html, judge_time, re_time, xpath_time)
    else:
        c_time = pub_time
    html_xy = html.replace('crossorigin="anonymous"', '')
    pat = '{}([\w\W]*?){}'.format(content_tag, content_tag_end)
    content = "".join(re.compile(pat).findall(html_xy))
    content = prse_src(content, url)
    if len(content) > 0:
        content = prse_text(content, url)
        if judge_model == "test":
            md, fj_one, fj_all = GET_accessory(html, url, host_name, 1, "www.baidu.com", c_time, title, accessory_xpath, accessory_re, accessory_judge)
            if int(title_type) == 1:
                try:
                    if "^" in title_details:
                        title = "".join(re.compile(title_details.replace("^", '')).findall(html))
                    else:
                        title = "".join(html_x.xpath(title_details))
                except Exception as err:
                    print(err)
                item = {"source": source, 'time': c_time, 'content': text, 'bq_conten': replace_html(content), 'fj_info': fj_all,
                        'title': title}
            else:
                item = {"source": source, 'time': c_time, 'content': text, 'bq_conten': replace_html(content), 'fj_info': fj_all}
            return item
        elif judge_model == "formal":
            l_type = kwargs['l_type']
            host = kwargs['host']
            cid = kwargs["cid"]
            title = kwargs['title']
            if int(title_type) == 1:
                try:
                    title = "".join(html_x.xpath(title_details))
                except:
                    title = kwargs['title']
            md, fj_one, fj_all = GET_accessory(html, url, host_name, l_type, host, c_time, title, accessory_xpath, accessory_re, accessory_judge)
            item = (source, c_time, replace_html(content), md, title, url, cid)
            return item, fj_one
    else:
        logger.error("网址 {}  标签有问题".format(url))
        if judge_model == "test":
            return ""
        return None, None



def prse_text(content, url):
    """
    处理图片链接和视频
    :param content:
    :param url:
    :return:
    """
    imgs = re.compile("<img.*?(?:>|\/>)", re.IGNORECASE).findall(content)
    for i in imgs:
        try:
            src = "".join(re.compile(" src=[\'\"]?([^\'\"]*)[\'\"]?", re.IGNORECASE).findall(i))
        except:
            src = ""
        if src:
            if 'http' in src or 'data:image' in src:
                pass
            else:
                src_i_i = urljoin(url, src)
                content = content.replace(src, src_i_i)
        else:
            pass
    videos = re.compile("<video.*?(?:>|\/>)", re.IGNORECASE).findall(content)
    for j in videos:
        try:
            src = "".join(re.compile(" src=[\'\"]?([^\'\"]*)[\'\"]?", re.IGNORECASE).findall(j))
        except:
            src = ""
        if src:
            if 'http' in src:
                pass
            else:
                video_src = urljoin(url, src)
                content = content.replace(src, video_src)
        else:
            pass

    return  content
def DPP_content_xpath(**kwargs):
    html = kwargs['html']
    title_type = kwargs['title_type']
    host_name = kwargs['host_name']
    judge_time = kwargs['judge_time']
    url = kwargs['url']
    xpath_xq = kwargs['xpath_xq']
    judge_model = kwargs['judge_model']
    pub_time = kwargs['pub_time']
    accessory_xpath = kwargs['accessory_xpath']
    accessory_re = kwargs['accessory_re']
    accessory_judge = kwargs['accessory_judge']
    title_details = kwargs['title_details']
    re_time = kwargs['re_time']
    xpath_time = kwargs['xpath_time']
    try:
        title = kwargs['title']
    except:
        title = '本网页'
    html_x = etree.HTML(html)
    source = source_pd(html_x)  #####获取来源
    if not source or len(source) >= 10:
        source = host_name
    text = ""
    if int(judge_time) > 3:
        c_time = xq_public_time(html, judge_time, re_time, xpath_time)
    else:
        c_time = pub_time
    data = etree.HTML(html)
    xpath_list = xpath_xq.split("^")
    data_xpath_new = None
    for i in xpath_list:
        data_xpath = data.xpath(i)
        if data_xpath:
            data_xpath_new = data_xpath
            break
        else:
            pass
    if data_xpath_new is not None:
        code = "utf-8"
        content = ""
        for i in data_xpath_new:
            ror = tostring(i, encoding=code).decode(code)
            ror = prse_src(ror, url)
            content += ror
        content = prse_text(content, url)
        if judge_model == "test":
            md, fj_one, fj_all = GET_accessory(html, url, host_name, 1, "www.baidu.com", c_time, title, accessory_xpath, accessory_re, accessory_judge)
            if int(title_type) == 1:
                try:
                    if "^" in title_details:
                        title = "".join(re.compile(title_details.replace("^", '')).findall(html))
                    else:
                        title = "".join(html_x.xpath(title_details))
                except:
                    pass
                item = {'source': source, 'time': c_time, 'content': text, 'bq_conten': replace_html(content), 'fj_info': fj_all,
                        'title': title}
            else:
                item = {'source': source, 'time': c_time, 'content': text, 'bq_conten': replace_html(content), 'fj_info': fj_all}
            return item
        elif judge_model == "formal":
            l_type = kwargs['l_type']
            host = kwargs['host']
            cid = kwargs["cid"]
            title = kwargs['title']
            if int(title_type) == 1:
                try:
                    title = "".join(html_x.xpath(title_details))
                except:
                    title = kwargs['title']
            md, fj_one, fj_all = GET_accessory(html, url, host_name, l_type, host, c_time, title, accessory_xpath, accessory_re, accessory_judge)
            item = (source, c_time, replace_html(content), md, title, url, cid)
            return item, fj_one
    else:
        if judge_model == "test":
            return ""
        return None, None


def DPP_content_json(**kwargs):
    html = kwargs['html']
    title_type = kwargs['title_type']
    host_name = kwargs['host_name']
    judge_time = kwargs['judge_time']
    content_tag = kwargs['content_tag']
    content_tag_end = kwargs['content_tag_end']
    url = kwargs['url']
    judge_model = kwargs['judge_model']
    pub_time = kwargs['pub_time']
    json_xq = kwargs['json_xq']
    accessory_xpath = kwargs['accessory_xpath']
    accessory_re = kwargs['accessory_re']
    accessory_judge = kwargs['accessory_judge']
    title_details = kwargs['title_details']
    re_time = kwargs['re_time']
    xpath_time = kwargs['xpath_time']
    try:
        title = kwargs['title']
    except:
        title = '本网页'
    if "http://www.shptdj.cn/website/Ajax/content_0.ashx" in url or "http://gkml.dbw.cn/gkml/web/data/detail.ashx?" in url:
        url_org = parse.unquote(html)
        html = url_org.replace("%u", '\\u').encode('utf-8').decode('unicode_escape')
    content = jsonpath.jsonpath(json.loads(html), json_xq)[0]
    html = content
    html_x = etree.HTML(html)
    source = source_pd(html_x)  #####获取来源
    if not source or len(source) >= 10:
        source = host_name
    text = ''
    if int(judge_time) > 3:
        c_time = xq_public_time(html, judge_time, re_time, xpath_time)
        # c_time = time_pd(html)####获取发布日期
    else:
        c_time = pub_time
    content = prse_src(content, url)
    if len(content) > 0:
        imgs = re.compile("<img.*?(?:>|\/>)", re.IGNORECASE).findall(content)
        for i in imgs:
            try:
                src = re.compile(" src=[\'\"]?([^\'\"]*)[\'\"]?", re.IGNORECASE).findall(i.replace("\\", ''))[0]
            except:
                src = ""
            if src:
                if 'http' in src or 'data:image' in src:
                    pass
                else:
                    src_i_i = urljoin(url, src)
                    content = content.replace(src, src_i_i)
            else:
                pass
        videos = re.compile("<video.*?(?:>|\/>)", re.IGNORECASE).findall(content)
        for j in videos:
            try:
                src = "".join(re.compile(" src=[\'\"]?([^\'\"]*)[\'\"]?", re.IGNORECASE).findall(j))
            except:
                src = ""
            if src:
                if 'http' in src:
                    pass
                else:
                    video_src = urljoin(url, src)
                    content = content.replace(src, video_src)
            else:
                pass
        if judge_model == "test":
            md, fj_one, fj_all = GET_accessory(html, url, host_name, 1, "www.baidu.com", c_time, title, accessory_xpath, accessory_re, accessory_judge)
            if int(title_type) == 1:
                try:
                    if "^" in title_details:
                        title = "".join(re.compile(title_details.replace("^", '')).findall(html))
                    else:
                        title = "".join(html_x.xpath(title_details))
                except Exception as err:
                    print(err)
                item = {"source": source, 'time': c_time, 'content': text, 'bq_conten': replace_html(content), 'fj_info': fj_all,
                        'title': title}
            else:
                item = {"source": source, 'time': c_time, 'content': text, 'bq_conten': replace_html(content), 'fj_info': fj_all}
            return item
        elif judge_model == "formal":
            l_type = kwargs['l_type']
            host = kwargs['host']
            cid = kwargs["cid"]
            title = kwargs['title']
            if int(title_type) == 1:
                try:
                    title = "".join(html_x.xpath(title_details))
                except:
                    title = kwargs['title']
            md, fj_one, fj_all = GET_accessory(html, url, host_name, l_type, host, c_time, title, accessory_xpath, accessory_re, accessory_judge)
            item = (source, c_time, replace_html(content), md, title, url, cid)
            return item, fj_one
    else:
        if judge_model == "test":
            return ""
        return None, None


def source_pd(html_x):  #####判断来源
    ##来源
    try:
        io = html_x.xpath('//*[contains(text(),"来源")]//text()')
    except:
        io = ""
    if io:
        io = io[0]
        source_text = "".join(io)
        source_text_i = "".join(re.compile('[\u4e00-\u9fa5]').findall(source_text)).replace('\n', '').replace('\t', '').replace('\r', '').replace(' ', '')
        source_text_i_i = source_text_i.replace('时间', '').replace('发布', '').replace('发表', '').replace('打印', '').replace('字体', '').replace("日期", '')
        source_x = "".join(re.compile('来源(.+)').findall(source_text_i_i))
        source = source_x.replace('来源', '')
    else:
        source = ''

    return source

def xq_public_time(html, judge_time, re_time, xpath_time):
    """
    栏目页时间获取函数
    :param html: 网页源码
    :param judge_time: 时间获取值
    :param re_time: 正则获取时间规则
    :param xpath_time: xpath获取时间规则
    :return:
    """
    if int(judge_time) == 4:  ###手动获取发布时间
        if re_time and re_time != None:
            all_time = re.compile(re_time).findall(html)[0]
            return doVerification(all_time)
    elif int(judge_time) == 5:
        if xpath_time and xpath_time != None:
            data = etree.HTML(html)
            all_time = data.xpath(xpath_time)[0]
            return doVerification(all_time)
    elif int(judge_time) == 6:
        all_time = time_pd(html)
        return all_time

def doVerification(val):
    """验证时间"""
    try:
        if len(val) <= 10:
            format_date = "%Y-%m-%d"
        elif 10 < len(val) <= 16:
            format_date = "%Y-%m-%d %H:%M"
        else:
            format_date = "%Y-%m-%d %H:%M:%S"
        date = datetime.strptime(val, format_date)
        return str(date) if date else None
    except:
        try:
            format_date = "%Y-%m-%d"
            if len(val) >= 10:
                val = val.split(" ", 1)[0]
            date = datetime.strptime(val, format_date)
            return str(date) if date else None
        except:
            return None


def get_date(context):
    """清洗日期"""
    if not context:
        return None
    context = re.sub(u"[年月/\.]", '-', context).replace("日", ' ')
    pattern = re.compile(r'(20\d{2}[_,/,\-,年]\d{1,2}[/,_,\-,月]\d{0,2})(\s{1}\d{1,2}:\d{1,2}(:\d{1,2}){0,1}){0,1}')
    # 拿到文章中提取的时间[('2017-05-08', '', ''), ('2009年10月', '', ''), ('2012年2月', '', ''), ('2012年7月', '', ''), ('2012年10月23', '', '')]
    date_match = re.findall(pattern, context)
    date_list = []
    i = datetime.now()
    for date_tuple in date_match:
        try:
            jj = datetime.strptime(date_tuple[0], '%Y-%m-%d')
            if jj > i:
                date_match.remove(date_tuple)
            else:
                date_list.append(date_tuple[0] + " " + date_tuple[1])
        except:
            pass
    list = []
    try:
        c = max(date_list, key=len)
        date_list.remove(c)
        list.append(c)
        for i in date_list:
            if len(i) == len(c):
                list.append(i)
    except:
        pass
    data = [doVerification(i.strip()) if doVerification(i.strip()) else None for i in list]
    data = [i.strip() for i in data if i]
    data = sorted(data, reverse=True)
    return data[0] if data else None


def time_pd(html):
    html = html.replace(r"\n", '').replace(r"\t", '').replace("&nbsp;", '').replace(r"\r", '')
    data = etree.HTML(html)
    now_time = datetime.now().strftime('%Y-%m-%d')
    try:
        ttime = data.xpath('//publishtime|//strong[contains(text(),"成文日期：")]/../following-sibling::td[1]|//*[contains(text(),"发布日期:")]|//*[contains(text(),"发布时间:")]|//*[contains(text(),"发布日期：")]|//*[contains(text(),"发布时间：")]|//*[contains(text(),"发表日期:")]|//*[contains(text(),"发表时间:")]|//*[contains(text(),"发表日期：")]|//*[contains(text(),"发表时间：")]')
        if len(ttime) > 0:
            info = ttime[0]
            all = "".join(info.xpath(".//text()"))
            if '登记日期' in all:
                pb_time = None
            else:
                all = all.replace("\n", '').replace("\t", '').replace("\r", '')
                pb_time = get_date(all)
                if not pb_time or pb_time == None:
                    all = "".join(info.xpath("./..//text()"))
                    pb_time = get_date(all)
            if not pb_time or pb_time == None:
                pb_time = get_date(html)
        else:
            pb_time = get_date(html)
        if not pb_time or pb_time == None:
            pb_time = now_time
        return pb_time
    except:
        return now_time


def prse_src(html, url):
    href = re.compile(" (src|href)=[\'\"]?([^\'\"]*)[\'\"]?", re.IGNORECASE).findall(html.replace("\\", ''))
    for i in href:
        src = i[1]
        if 'http' in src or 'data:image' in src:
            src_i_i = src
        else:
            src_i_i = urljoin(url, src)
        html = html.replace(src, src_i_i, 1)
    return html

def GET_accessory(html, url, host_name, l_type, host, c_time, title, accessory_xpath, accessory_re, accessory_judge):
    """
    附件查找的正则和xpath模式会自动请求规则下面所有的url 来区分哪个是url  慎用
    :param html:
    :param url:
    :param host_name:
    :param l_type:
    :param host:
    :param c_time:
    :param title:
    :param accessory_xpath:
    :param accessory_re:
    :param accessory_judge:
    :return:
    """
    if int(accessory_judge) == 1:
        try:
            a_html = re.compile(str(accessory_re)).findall(html)[0]
            a_response = etree.HTML(a_html)
            all_fj, all_md, fj_all = Parse_accessory(a_response, a_html, url, title, host_name, l_type, host, c_time)
        except Exception as err:
            print(err)
            all_fj = None
            all_md = None
            fj_all = ""
    elif int(accessory_judge) == 2:
        try:
            response = etree.HTML(html)
            data = response.xpath(accessory_xpath)[0]
            content = tostring(data, encoding="utf-8").decode("utf-8")
            all_fj, all_md, fj_all = Parse_accessory(data, content, url, title, host_name, l_type, host, c_time)
        except:
            all_fj = None
            all_md = None
            fj_all = ""
    else:
        all_fj, all_md, fj_all = Power_accessory(url, html, title, host_name, l_type, host, c_time)
        if all_fj == None or len(all_fj) < 1:
            all_fj, all_md, fj_all = Iframe_accessory(url, html, title, host_name, l_type, host, c_time)
        if all_fj == None or len(all_fj) < 1:
            all_fj, all_md, fj_all = ALL_accessory(url, html, title, host_name, l_type, host, c_time)
    try:
        return all_md, all_fj, fj_all
    except:
        p = ""
        return None, None, p
def Power_accessory(url, html, title, host_name, l_type, host, c_time):
    all_fj = []
    all_md = []
    fj_all = []
    html_xy = etree.HTML(html)
    all = html_xy.xpath("//img[@data-powerurl]")
    for i in all:
        img_fj = "".join(i.xpath("./@data-powerurl"))
        img_fj = img_fj.replace(" ", '').replace("%20", '')
        if 'http' not in img_fj:
            fj_url = urljoin(url, img_fj.strip())
        else:
            fj_url = img_fj
        name_cn = "".join(i.xpath(".//text()"))
        if not name_cn or len(name_cn) < 1:
            name_cn = "{}的附件".format(title)
        suffix = get_suffix(img_fj)
        if not suffix or len(suffix) < 1:
            suffix = get_suffix(name_cn)
        m = hashlib.md5()
        m.update(str(fj_url).encode('utf-8'))
        md = m.hexdigest()  ####拿到url 哈希值
        item = {
            'fj_title': name_cn,
            'fj_url': fj_url
        }
        fj_one = fj_get_one(name_cn, suffix, fj_url, md, host_name, l_type, host, c_time)
        if fj_one != None:
            all_fj.append(fj_one)
            all_md.append(md)
            fj_all.append(item)
    return all_fj, all_md, fj_all

def Iframe_accessory(url, html, title, host_name, l_type, host, c_time):
    all_fj = []
    all_md = []
    fj_all = []
    html_xy = etree.HTML(html)
    iframe_src = html_xy.xpath("//iframe")
    for i in iframe_src:
        src = "".join(i.xpath("./@src"))
        src = src.replace(" ", '').replace("%20", '')
        try:
            name_cn = "".join(i.xpath("//iframe[@src='{}']//text()".format(src)))
        except:
            name_cn = ""
        suffix = get_suffix(src)
        if not suffix or len(suffix) < 1:
            suffix = get_suffix(name_cn)
        if 'http' not in src:
            fj_url = urljoin(url, src.strip())
        else:
            fj_url = src
        if not name_cn or len(name_cn) < 1:
            name_cn = "{}的附件".format(title)
        m = hashlib.md5()
        m.update(str(fj_url).encode('utf-8'))
        md = m.hexdigest()  ####拿到url 哈希值
        item = {
            'fj_title': name_cn,
            'fj_url': fj_url
        }
        fj_one = fj_get_one(name_cn, suffix, fj_url, md, host_name, l_type, host, c_time)
        if fj_one != None:
            all_fj.append(fj_one)
            all_md.append(md)
            fj_all.append(item)
    return all_fj, all_md, fj_all


def ALL_accessory(url, html, title, host_name, l_type, host, c_time):
    all_fj = []
    all_md = []
    fj_all = []
    all_a = re.compile("<a [\w\W]*?</a>", re.IGNORECASE).findall(html)
    response = etree.HTML(html)
    for index, i in enumerate(all_a):
        href = re.compile("href=[\'\"]?([^\'\"]*)[\'\"]?", re.IGNORECASE).findall(i)
        for src in href:
            src = src.replace(" ", '').replace("%20", '')
            pat = "//a[@href='{}']/@title".format(src)
            name_cn = "".join(response.xpath(pat))
            if not name_cn or len(name_cn) < 1:
                pat = "//a[@href='{}']//text()".format(src)
                name_cn = "".join(response.xpath(pat))
            if not name_cn or len(name_cn) < 1:
                name_cn = "{}的附件{}".format(title, str(index))
            fj_url = urljoin(url, src.strip())
            suffix = get_suffix(fj_url)
            if not suffix or len(suffix) < 1:
                suffix = get_suffix(name_cn)
            if suffix:
                m = hashlib.md5()
                m.update(str(fj_url).encode('utf-8'))
                md = m.hexdigest()  ####拿到url 哈希值
                item = {
                    'fj_title': name_cn,
                    'fj_url': fj_url
                }
                fj_one = fj_get_one(name_cn, suffix, fj_url, md, host_name, l_type, host, c_time)
                if fj_one != None:
                    all_fj.append(fj_one)
                    all_md.append(md)
                    fj_all.append(item)
            else:
                pass
    return all_fj, all_md, fj_all

def Parse_accessory(a_response, a_html, url, title, host_name, l_type, host, c_time):
    """
    传入str和xml 集中处理 返回字典
    :param response:
    :param html:
    :return:
    """
    all_fj = []
    all_md = []
    fj_all = []
    all_href = re.compile("href=[\'\"]?([^\'\"]*)[\'\"]?", re.IGNORECASE).findall(a_html)
    all_SRC = re.compile("src=[\'\"]?([^\'\"]*)[\'\"]?", re.IGNORECASE).findall(a_html)
    all_src = all_href + all_SRC
    for index, i in enumerate(all_src):
        i = i.replace(" ", '').replace("%20", '')
        name_cn = "".join(a_response.xpath("//*[@href='{}']/@title|//*[@href='{}']/text()|//*[@src='{}']/@title|//*[@src='{}']/text()".format(i, i, i, i)))
        if not name_cn or len(name_cn) < 1:
            name_cn = "附件{}".format(index)
        fj_url = urljoin(url, i.strip())
        g = get_type(fj_url)
        suffix = get_suffix(str(g))
        if suffix:
            name_cn = name_cn.replace(".", '') + "." + str(suffix)
            m = hashlib.md5()
            m.update(str(fj_url).encode('utf-8'))
            md = m.hexdigest()  ####拿到url 哈希值
            item = {
                'fj_title': name_cn,
                'fj_url': fj_url
            }
            fj_one = fj_get_one(name_cn, suffix, fj_url, md, host_name, l_type, host, c_time)
            if fj_one != None:
                all_fj.append(fj_one)
                all_md.append(md)
                fj_all.append(item)
        else:
            pass
    return all_fj, all_md, fj_all


def replace_html(html):
    try:
        soup = BeautifulSoup(html, 'html.parser')
        html = soup.prettify()
    except:
        pass
    title = html.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", '&').replace("&nbsp;", '').replace("&nbsp", '').replace("nbsp", '').replace("display:none", '')
    return title


def fj_get_one(name_cn, suffix, fj_url, md, host_name, l_type, host, c_time):
    ##附件英文名字
    if len(suffix) > 0:
        if c_time == None:
            c_time = "2000-01-01"
        name_eng = fj_url.split("/")[-1]
        if int(l_type) == 1:
            path_two = 'zhengcezixun'
        elif int(l_type) == 2:
            path_two = 'zhengcewenjian'
        elif int(l_type) == 4:
            path_two = 'xinwenyaowen'
        elif int(l_type) == 5:
            path_two = 'zhaobiaocaigou'
        else:
            path_two = "fujian"
        try:
            path = "public/" + str(host).strip() + "/" + str(path_two) + "/" + str(c_time).replace(":", "-").strip() + "/" + str(md) + "." + str(suffix)  ###拼接路径
        except Exception as err:
            print(str(err) + "  路径拼接")
            path = "public/" + str(host).strip() + "/" + str(path_two) + "/wsfj/" + str(md) + "." + str(suffix).strip()  ###拼接路径
        fj_one = (name_cn, name_eng, path, md, fj_url)
        return fj_one
    else:
        return None


def get_type(url):
    headers = {
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36 Edg/87.0.664.60",  # self.ua.random,
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Accept-Encoding': 'gzip, deflate',
        'Accept': 'ext/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3'
    }
    num = 0
    while num < 2:
        try:
            with requests.session() as s:
                data = requests.get(url, headers=headers, timeout=(6, 6))
                s.close()
            if data.status_code == 200:
                return data.headers
            else:
                num += 1
        except:
            num += 1
    return None