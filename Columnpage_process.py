#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：公司政策爬虫 
@File    ：Columnpage_process.py
@IDE     ：PyCharm 
@Author  ：13192272582
@Date    ：2021/1/21 17:21 
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
import  traceback

pd_json_urls = ['http://www.sqzwfw.gov.cn/Sqzwy_FrontGlobalAction/CommonService/Pages.asmx/GetSearchInfoListAll', 'http://zwgk.shcn.gov.cn:9091/Epoint-MiddleForWeb/rest/lightfrontaction/getgovinfolist', 'https://gaj.sh.gov.cn/crj/api/invoke', 'http://yjglt.nmg.gov.cn/api/Data/GetContents', 'http://tyj.hlj.gov.cn/hljtyjapi/governmentAffairs/infoGuideList']


def Handle_url(all_urls, **kwargs):
    """
    返回格式为（网址，cookie，i(里面post就是data get就是网址)，cid,kwargs）
    :param all_urls:
    :param kwargs:
    :return:
    """
    url_real = kwargs['url_real']
    collect_type = kwargs['collect_type']
    form_data = kwargs['form_data']
    info_page = kwargs['info_page']
    url = kwargs['url']
    cid = kwargs['cid']
    cookie = kwargs['cookie']
    url_two = kwargs['url_two']
    immit_js = kwargs['immit_js']
    ####post请求
    if url_real and collect_type == 'B' and form_data != None:
        url_real = url_real.strip()
        kwargs['page_url'] = url_real
        item_list = get_pageurl_post(form_data, url_real, info_page)
        for i in item_list:
            tuple = (url_real, cookie, i, 1, cid, kwargs)
            all_urls.append(tuple)
    else:
        url = url.strip()
        kwargs['page_url'] = url
        item_list = get_pageurl_get(url, url_two, info_page)
        for i in item_list:
            tuple = (url, cookie, i, 0, cid, kwargs)
            all_urls.append(tuple)
    return all_urls


def get_pageurl_post(form_data, url_real, info_pgae):
    item_list = []
    for i in range(1, info_pgae + 1):
        print(form_data)
        if url_real == 'http://www.gdzj110.gov.cn/Ashx/PostHandler.ashx?action=News-GetPublishNewsList':
            item = {}
            l = form_data.split("^")
            item = {
                'args': l[0].replace("*", str(i)).replace('args:', '')
            }
            item_list.append(item)
        if url_real == 'http://112.35.128.198:8087/EpointZJDT/rest/webGovInfoSearchAction/getCMSInfo':
            item = {

                'params': form_data.replace("*", str(i))
            }
            item_list.append(item)
        elif url_real in pd_json_urls or  "&%#&" in url_real:
            item = str(form_data).replace("*", str(i))
            item_list.append(item)
        else:
            if "*" in form_data:
                form_data_new = form_data.replace("*", str(i))
            else:
                form_data_new = form_data
            l = form_data_new.split("^")
            item = {}
            try:
                for i in l:
                    if i:
                        p = i.split(':')[0].strip()
                        pp = i.split(':')[1].strip()
                        item[p] = pp
                    else:
                        pass
            except:
                pass
            item_list.append(item)
    return item_list


def get_pageurl_get(url_one, url_two, info_page):
    item_list = []
    for i in range(0, info_page):
        if i == 0:
            url_next = url_one
        else:
            url_next = url_two.replace('*', str(i))
        item_list.append(url_next)
    return item_list



def Handle_tttt(**kwargs):
    """
    正则解析模式
    :param kwargs:
    :return:
    """
    judge_model = kwargs['judge_model']
    cid = kwargs['cid']
    title_tag = kwargs['title_tag']
    html = kwargs['html']
    pageurl = kwargs['page_url']
    judge_time = kwargs['judge_time']
    include = kwargs['include']
    not_include = kwargs['not_include']
    r_replace = kwargs['r_replace']
    l_replace = kwargs['l_replace']
    def_src = kwargs['def_src']
    xpath_time = kwargs['xpath_time']
    re_time = kwargs['re_time']
    detail_type = kwargs['detail_type']
    detail_data = kwargs['detail_data']
    ####拥有标题标签的处理模式
    json_list = []
    title_start = title_tag.split(',')[0].strip()
    title_end = title_tag.split(',')[1].strip()
    html_xy = html.replace('\n', ' ').replace('\t', '').replace('\r', '')
    pat_end = '{}([\w\W]*?){}'.format(title_start, title_end)
    content = "".join(re.compile(pat_end).findall(html_xy)).replace("<!", '').replace("!>", '')
    if int(judge_time) != 0 and int(judge_time) < 4: ###判断是否需要从栏目页获取发布日期
        all_time = public_time(content, judge_time, re_time, xpath_time) ###获取到所有的发布日期
    else:
        all_time = None
    try:
        content_xpath = etree.HTML(content)
        all_a_xpath = content_xpath.xpath("//a[@href]")
        if all_time != None:
            pd_i = 0
        index = 0
        for ko in all_a_xpath:
            title = "".join(ko.xpath("./@title")).replace('\n', '').replace('\r', '').replace('\t', '')
            if not title:
                title = "".join(ko.xpath(".//text()")).replace('\n', '').replace('\r', '').replace('\t', '')
            if len(title.replace(" ", '')) < 1:
                continue
            href = "".join(ko.xpath("./@href")).replace('\n', '').replace('\r', '').replace('\t', '').replace(" ", '').replace('\\', '')
            href = urljoin(pageurl, href).replace("site/readneed.jsp?id", 'need/viewNeed.jsp?needId')
            if '查看详情' in title or 'javascript:;' in href:
                continue
            a_num = pd_title(title, include, not_include)
            title = title.replace(' ', '')
            title = replace_title(r_replace, l_replace, title, def_src)
            if all_time != None:
                date = all_time[pd_i]
                pub_time = handle_pubdate(date)
                pd_i += 1
            else:
                pub_time = ''
            pub_time = str(pub_time)
            if int(detail_type) == 1:###判断详情页是否是post采集
                post_data_str = POST_data(detail_data, content, index)  ####拿到post所需data数据
                index += 1
                m = hashlib.md5()
                m.update(str(post_data_str).encode('utf-8'))
                right = m.hexdigest()  ####拿到url 哈希值
                href = href + "*^*" + right
            else:
                post_data_str = ""
            item = {'title': replace_biaoti(title),
                    'xq_url': href.strip().replace('\n', '').replace('\r', '').replace('\t', '').replace("&amp;", '&'),
                    'pageurl': pageurl,
                    'public_time': pub_time,
                    'post_data': post_data_str
                    }
            if a_num == 0:
                if judge_model == "formal":
                    info = (json.dumps(post_data_str), replace_biaoti(title), cid, 0, 0, pub_time, href.strip().replace("&amp;", '&').replace('\n', '').replace('\r', '').replace('\t', ''))
                    json_list.append(info)
                elif judge_model == "test":
                    json_list.append(item)
        return json_list
    except:
        P = ""
        return P


def pd_pub_time(a):
    try:
        k = datetime.strptime(a, '%Y-%m-%d')
    except:
        k = datetime.strptime("1970-02-01", '%Y-%m-%d')
    return k


def parse_joint(xq_url_light, html, xq_url_right):
    if "*" in xq_url_light:
        count = xq_url_light.count("*")
        all_right = xq_url_right.split("^")
        all = []
        for i in range(0, count):
            xpath = all_right[i]
            p = re.compile(xpath).findall(html)
            p_len = len(p)
            all.extend(p)
        all_url = []
        for o in range(0, p_len):
            for i in range(0, count):
                if i > 0:
                    url = url.replace("*", all[o + p_len], 1)
                else:
                    url = xq_url_light.replace("*", all[o], 1)
            all_url.append(url)
    else:
        all_url = []
        p = re.compile(xq_url_right).findall(html)
        for i in p:
            all_url.append(xq_url_light)
    return all_url


def handle_pubdate(date):
    """
    栏目日发布日期处理

    :param date:
    :return:处理完的发布日期
    """
    pub_time = date.strip().replace("&amp;", '&').replace('\n', '').replace('\r', '').replace('\t', '').replace(" ", '').replace("-", '-')
    if pub_time.isdigit() and len(pub_time) > 8:
        kl = pub_time[:10]
        dateArray = datetime.fromtimestamp(int(kl))
        pub_time = str(dateArray.strftime("%Y-%m-%d %H:%M:%S"))
    else:
        pub_time = re.sub(u"[年月/\.]", '-', date).replace("日", '').replace(" ", '').replace('\n', '').replace('\r', '').replace('\t', '')
        if len(pub_time) > 9:
            pub_time = pub_time[:10]
            pub_time = pd_pub_time(pub_time)
        elif len(pub_time) == 8 or len(pub_time) == 9:
            one = pub_time.split("-")[0]
            two = pub_time.split("-")[1]
            three = pub_time.split("-")[2]
            if len(str(two)) < 2:
                two = "0" + str(two)
            if len(str(three)) < 2:
                three = "0" + str(three)
            if len(str(one)) < 3:
                one = "20" + str(one)
            pub_time = one + "-" + two + "-" + three
            pub_time = pd_pub_time(pub_time)
        elif len(pub_time) == 5:
            r_time = pub_time[-5:]
            co_time = pub_time[-3:-2]
            pub_time = str(datetime.now().year) + str(co_time) + r_time
            pub_time = pd_pub_time(pub_time)
        else:
            if str(pub_time) == "2019":
                pub_time = "2019-01-01"
            elif str(pub_time) == "2020":
                pub_time = "2020-01-01"
            else:
                pub_time = "2020-01-01"
            pub_time = pd_pub_time(pub_time)
    return pub_time


def Handle_title_re(**kwargs):
    """
    正则2处理标题模式
    :param kwargs:
    :return:
    """
    #####拥有正则标题处理模式
    judge_model = kwargs['judge_model']
    cid = kwargs['cid']
    title_re = kwargs['title_re']
    xq_url_right = kwargs['xq_url_right']
    xq_url_light = kwargs['xq_url_light']
    html = kwargs['html']
    pageurl = kwargs['page_url']
    judge_time = kwargs['judge_time']
    include = kwargs['include']
    not_include = kwargs['not_include']
    r_replace = kwargs['r_replace']
    l_replace = kwargs['l_replace']
    def_src = kwargs['def_src']
    xpath_time = kwargs['xpath_time']
    re_time = kwargs['re_time']
    transcode = kwargs['transcode']
    detail_type = kwargs['detail_type']
    detail_data = kwargs['detail_data']
    json_list = []
    title = re.compile(title_re).findall(html)
    title = [i for i in title if i != '']
    if "http://edu.sh.gov.cn/html/applistxml/xxgk" in pageurl:
        a = re.compile("<dt_generateDate>(.*?)</dt_generateDate>").findall(html)
        x = [c[:-2].replace('-', '') for c in a]
        b = re.compile("<str_catchNum_K>(.*?)</str_catchNum_K>").findall(html)
        g = lambda a, b: [x[i] + "/" + b[i] for i in range(len(x))]
        href_a = g(a, b)
    if xq_url_light:
        href_b = parse_joint(xq_url_light, html, xq_url_right)
    else:
        href_a = re.compile(xq_url_right).findall(html)
        href_b = href_a
    if int(judge_time) != 0 and int(judge_time) < 4:
        all_time = public_time(html, judge_time, re_time, xpath_time)
    else:
        all_time = None
    print("标题{}网址{}".format(len(title), len(href_b)))
    index = 0
    for l in range(0, len(title)):
        href = href_b[l].replace('\\', '').replace("\n", '').replace("\t", '').strip()
        try:
            href = urljoin(pageurl, href).replace("site/readneed.jsp?id", 'need/viewNeed.jsp?needId')
        except:
            pass
        if 'zfcxjst.gd.gov.cn/gkmlpt/api/all/1427?' in pageurl or 'zfcxjst.gd.gov.cn/gkmlpt/api/all' in pageurl or 'gdaudit.gd.gov.cn/gkmlpt/api/all/0?' in pageurl or 'www.zhxz.gov.cn/gkmlpt/api/all/0?' in pageurl or transcode == 2:
            title[l] = title[l].replace("%u", '\\u').encode('utf-8').decode('unicode_escape', 'ignore')

        title[l] = title[l].replace(' ', '')
        title[l] = replace_title(r_replace, l_replace, title[l], def_src)
        a_num = pd_title(title[l], include, not_include)
        if all_time != None:
            date = all_time[l]
            pub_time = handle_pubdate(date)
        else:
            pub_time = ''
        pub_time = str(pub_time)
        title[l] = re.sub("<.*?>", '', title[l])
        if int(detail_type) == 1:
            post_data_str = POST_data(detail_data, html, index)  ####拿到post所需data数据
            index += 1
            m = hashlib.md5()
            m.update(str(post_data_str).encode('utf-8'))
            right = m.hexdigest()  ####拿到url 哈希值
            href = href + "*^*" + right
            if pageurl == "http://zwgk.shcn.gov.cn:9091/Epoint-MiddleForWeb/rest/lightfrontaction/getgovinfolist":
                post_data_str_1 = {}
                post_data_str_1['params'] = post_data_str
                post_data_str = post_data_str_1
        else:
            post_data_str = ""
        lol = {
            'title': replace_biaoti(title[l]),
            'xq_url': href.replace('\n', '').replace('\r', '').replace('\t', '').replace("&amp;", '&'),
            'pageurl': pageurl,
            'public_time': pub_time,
            'post_data': post_data_str
        }
        info = (json.dumps(post_data_str), replace_biaoti(title[l]), cid, 0, 0, pub_time, href.strip().replace("&amp;", '&').replace('\n', '').replace('\r', '').replace('\t', ''))
        if a_num == 0:
            if judge_model == "formal":
                json_list.append(info)
            elif judge_model == "test":
                json_list.append(lol)
        else:
            pass
    return json_list


def Handle_xpath(**kwargs):
    ###拥有xpath标题处理模式
    judge_model = kwargs['judge_model']
    cid = kwargs['cid']
    html = kwargs['html']
    pageurl = kwargs['page_url']
    judge_time = kwargs['judge_time']
    include = kwargs['include']
    not_include = kwargs['not_include']
    r_replace = kwargs['r_replace']
    l_replace = kwargs['l_replace']
    def_src = kwargs['def_src']
    xpath_time = kwargs['xpath_time']
    re_time = kwargs['re_time']
    xpath_list = kwargs['xpath_list']
    detail_type = kwargs['detail_type']
    detail_data = kwargs['detail_data']
    json_list = []
    xpath_html = etree.HTML(html)
    try:
        xpath_info = xpath_html.xpath(xpath_list)[0]
        if int(judge_time) != 0 and int(judge_time) < 4:
            content = tostring(xpath_info, encoding="utf-8").decode("utf-8")
            all_time = public_time(content, judge_time, re_time, xpath_time)
        else:
            all_time = None
        all_a_xpath = xpath_info.xpath(".//a[@href]")
        if all_time != None:
            pd_i = 0
        index = 0
        for ko in all_a_xpath:
            title = "".join(ko.xpath("./@title")).replace('\n', '').replace('\r', '').replace('\t', '')
            if not title:
                title = "".join(ko.xpath(".//text()")).replace('\n', '').replace('\r', '').replace('\t', '')
            if len(title.replace(" ", '')) < 1:
                continue
            href = "".join(ko.xpath("./@href")).replace('\n', '').replace('\r', '').replace('\t', '').replace(" ", '').replace('\\', '')
            href = urljoin(pageurl, href).replace("site/readneed.jsp?id", 'need/viewNeed.jsp?needId')
            if '查看详情' in title or 'javascript:;' in href:
                continue
            a_num = pd_title(title, include, not_include)
            title = title.replace(' ', '')
            title = replace_title(r_replace, l_replace, title, def_src)
            if all_time != None:
                date = all_time[pd_i]
                pub_time = handle_pubdate(date)
                pd_i += 1
            else:
                pub_time = ''
            pub_time = str(pub_time)
            if int(detail_type) == 1:
                post_data_str = POST_data(detail_data, html, index)  ####拿到post所需data数据
                index += 1
                m = hashlib.md5()
                m.update(str(post_data_str).encode('utf-8'))
                right = m.hexdigest()  ####拿到url 哈希值
                href = href + "*^*" + right
            else:
                post_data_str = ""
            item = {'title': replace_biaoti(title),
                    'xq_url': href.strip().replace('\n', '').replace('\r', '').replace('\t', '').replace("&amp;", '&'),
                    'pageurl': pageurl,
                    'public_time': pub_time,
                    'post_data': post_data_str,
                    }
            if a_num == 0:
                if judge_model == "formal":
                    info = (post_data_str, replace_biaoti(title), cid, 0, 0, pub_time, href.strip().replace("&amp;", '&').replace('\n', '').replace('\r', '').replace('\t', ''))
                    json_list.append(info)
                elif judge_model == "test":
                    json_list.append(item)
        return json_list
    except Exception as err:
        traceback.print_exc()
        P = ""
        return P

def public_time(html, judge_time, re_time, xpath_time):
    """
    栏目页时间获取函数
    :param html: 网页源码
    :param judge_time: 时间获取值
    :param re_time: 正则获取时间规则
    :param xpath_time: xpath获取时间规则
    :return:
    """
    if int(judge_time) == 1:  ###手动获取发布时间
        if re_time and re_time != None:
            all_time = re.compile(re_time).findall(html)
            return all_time
    elif int(judge_time) == 2:
        if xpath_time and xpath_time != None:
            data = etree.HTML(html)
            all_time = data.xpath(xpath_time)
            return all_time
    elif int(judge_time) == 3:  ###自动获取时间
        context = re.sub(u"[年月/\.]", '-', html).replace("日", ' ')
        pattern = re.compile(r'(20\d{2}[_,/,\-,年]\d{1,2}[/,_,\-,月]\d{0,2})(\s{1}\d{1,2}:\d{1,2}(:\d{1,2}){0,1}){0,1}')
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
        return list

def pd_title(title, include, not_include):
    """
    判断标题包含与不包含
    :param title: 标题
    :param include:必须包含的字符串
    :param not_include: 不能包含的字符串
    :return:
    """
    if not include:
        item = {
            'pd': 0
        }
    else:
        item = {
            'pd': 1
        }
    #####返回值为0才能入库
    if include:
        include_list = include.split(',')
        for i in include_list:
            if i in title:
                item = {
                    'pd': 0
                }

    if not_include:
        not_include_list = not_include.split(',')
        for j in not_include_list:
            if j in title:
                item = {
                    'pd': 1
                }

    return item['pd']


def replace_title(r_replace, l_replace, title, def_src):
    """
    移除指定标题的字符串
    :param r_replace:左替换
    :param l_replace:右替换
    :param title:标题
    :param def_src:删除指定下标字符串
    :return:
    """
    if r_replace:
        for i in range(len(r_replace)):
            if '*' in r_replace[i]:
                zp_r = r_replace[i].split('*')[0]
                zp_l = r_replace[i].split('*')[1]
                zp_p = '{}(.*?){}'.format(zp_r, zp_l)
                zp_o = "".join(re.compile(zp_p).findall(title))

                ap_all = str(zp_r).replace('\\', '') + str(zp_o) + str(zp_l).replace('\\', '')
                title = title.replace(ap_all, l_replace[i])
            else:
                title = title.replace(r_replace[i], l_replace[i])
    elif def_src:
        def_r = def_src.split(';')[0]  ###左删除位数
        def_l = def_src.split(';')[1]  ###右删除位数
        if def_r:
            if int(def_r) == 99:
                r_time = del_time_r(title)
                title = title.replace(r_time, '')
            else:
                title = title[int(def_r) - 1:]
        if def_l:
            if int(def_l) == 99:
                l_time = del_time_l(title)
                title = title.replace(l_time, '')
            else:
                title = title[:-int(def_l)]
    return title


def del_time_r(html):
    """
    删除标题里面的日期
    :param html:
    :return:
    """
    k_html = html.replace('\r', '').replace('\t', '').replace('\n', '')
    c_time = re.compile('([2]\d{3}[-|/|.]\d{1,2}[-|/|.]\d{1,2})').findall(k_html)
    if c_time:
        c_time = c_time[0]
    else:
        c_time = re.compile('(\d{4}年\d{1,2}月\d{1,2}日)').findall(k_html)
        if c_time:
            c_time = c_time[0]
        else:
            c_time = ''
    return c_time


def del_time_l(html):
    """
    删除标题里面的日期
    :param html:
    :return:
    """
    k_html = html.replace('\r', '').replace('\t', '').replace('\n', '')
    c_time = re.compile('([2]\d{3}[-|/|.]\d{1,2}[-|/|.]\d{1,2})').findall(k_html)
    if c_time:
        c_time = c_time[-1]
    else:
        c_time = re.compile('(\d{4}年\d{1,2}月\d{1,2}日)').findall(k_html)
        if c_time:
            c_time = c_time[-1]
        else:
            c_time = ''
    return c_time

def POST_data(data, html, num):
    """
    :param data: post所需字典
    :param html: str文本
    :param num: 当前下标
    :return:  一个详情页post所需的字典
    """
    data = json.loads(data)
    for key in data:
        if "^" in data[key]:
            pat = str(data[key]).replace("^", '')
            p = re.compile(pat).findall(html)[num]
            data[key] = p
    return data

def replace_biaoti(html):
    title = html.replace('showTitle', '').replace('(', '').replace(')', '').replace('\\', '').replace("&amp;", '&').replace('\n', '').replace('\r', '').replace('\t', '').replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", '&').replace("&nbsp;", '').replace("&nbsp", '').replace("nbsp", '')
    return title