#!/usr/bin/env python
# encoding: utf-8
"""
@project : Policy_acquisition
@author  : tanghongxin
@file   : COLUMN_PARSE.py
@ide    : PyCharm
@time   : 2021-04-22 16:48:59
"""
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
from spider.setting import dont_allow

pd_json_urls = ['http://www.sqzwfw.gov.cn/Sqzwy_FrontGlobalAction/CommonService/Pages.asmx/GetSearchInfoListAll', 'http://zwgk.shcn.gov.cn:9091/Epoint-MiddleForWeb/rest/lightfrontaction/getgovinfolist', 'https://gaj.sh.gov.cn/crj/api/invoke', 'http://yjglt.nmg.gov.cn/api/Data/GetContents', 'http://tyj.hlj.gov.cn/hljtyjapi/governmentAffairs/infoGuideList']

class COLUNm(object):
    def __init__(self,log,**kwargs):
        self.judge_model = kwargs['judge_model']
        self.cid = kwargs['cid']
        self.title_tag = kwargs['title_tag']
        self.html = kwargs['html']
        self.pageurl = kwargs['page_url']
        self.judge_time = kwargs['judge_time']
        self.include = kwargs['include']
        self.not_include = kwargs['not_include']
        self.r_replace = kwargs['r_replace']
        self.l_replace = kwargs['l_replace']
        self.def_src = kwargs['def_src']
        self.xpath_time = kwargs['xpath_time']
        self.re_time = kwargs['re_time']
        self.detail_type = kwargs['detail_type']
        self.detail_data = kwargs['detail_data']
        self.logger=log

    def public_time(self,html, judge_time, re_time, xpath_time):
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

    def pd_title(self,title, include, not_include):
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

    def replace_title(self,r_replace, l_replace, title, def_src):
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
                    r_time = self.del_time_r(title)
                    title = title.replace(r_time, '')
                else:
                    title = title[int(def_r) - 1:]
            if def_l:
                if int(def_l) == 99:
                    l_time = self.del_time_l(title)
                    title = title.replace(l_time, '')
                else:
                    title = title[:-int(def_l)]
        return title

    def del_time_l(self,html):
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
    def del_time_r(self,html):
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

    def handle_pubdate(self,date):
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
                pub_time = self.pd_pub_time(pub_time)
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
                pub_time = self.pd_pub_time(pub_time)
            elif len(pub_time) == 5:
                r_time = pub_time[-5:]
                co_time = pub_time[-3:-2]
                pub_time = str(datetime.now().year) + str(co_time) + r_time
                pub_time = self.pd_pub_time(pub_time)
            else:
                if str(pub_time) == "2019":
                    pub_time = "2019-01-01"
                elif str(pub_time) == "2020":
                    pub_time = "2020-01-01"
                else:
                    pub_time = "2020-01-01"
                pub_time = self.pd_pub_time(pub_time)
        return pub_time

    def pd_pub_time(self,a):
        try:
            k = datetime.strptime(a, '%Y-%m-%d')
        except:
            k = datetime.strptime("1970-02-01", '%Y-%m-%d')
        return k

    def POST_data(self,data, html, num):
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
    def Handle_tttt(self):
        """
        正则解析模式
        :param kwargs:
        :return:
        """
        ####拥有标题标签的处理模式
        json_list = []
        title_start = self.title_tag.split(',')[0].strip()
        title_end = self.title_tag.split(',')[1].strip()
        pat_end = '{}([\w\W]*?){}'.format(title_start, title_end)
        content = "".join(re.compile(pat_end).findall(self.html)).replace("<!", '').replace("!>", '')
        if int(self.judge_time) != 0 and int(self.judge_time) < 4:  ###判断是否需要从栏目页获取发布日期
            all_time = self.public_time(content, self.judge_time, self.re_time, self.xpath_time)  ###获取到所有的发布日期
        else:
            all_time = None
        try:
            content_xpath = etree.HTML(content)
            all_a_xpath = content_xpath.xpath("//a[@href]")
            if all_time != None:
                pd_i = 0
            index = 0
            for ko in all_a_xpath:
                title = "".join(ko.xpath("./@title")).replace('\n', '').replace('\r', '').replace('\t', '').replace(' ', '')
                if not title:
                    title = "".join(ko.xpath(".//text()")).replace('\n', '').replace('\r', '').replace('\t', '').replace(' ', '')
                if len(title) < 1:
                    continue
                href = "".join(ko.xpath("./@href")).replace('\n', '').replace('\r', '').replace('\t', '').replace(" ", '').replace('\\', '')
                href = urljoin(self.pageurl, href).replace("site/readneed.jsp?id", 'need/viewNeed.jsp?needId')
                if '查看详情' in title or 'javascript:;' in href:
                    continue
                a_num = self.pd_title(title, self.include, self.not_include)
                if a_num == 0:
                    href=href.strip().replace("&amp;", '&').replace('\n', '').replace('\r', '').replace('\t', '')
                    title = self.replace_title(self.r_replace, self.l_replace, title, self.def_src)
                    if all_time != None:
                        date = all_time[pd_i]
                        pub_time = self.handle_pubdate(date)
                    else:
                        pub_time = ''
                    pub_time = str(pub_time)
                    if int(self.detail_type) == 1:  ###判断详情页是否是post采集
                        post_data_str = self.POST_data(self.detail_data, content, index)  ####拿到post所需data数据
                        index += 1
                        m = hashlib.md5()
                        m.update(str(post_data_str).encode('utf-8'))
                        right = m.hexdigest()  ####拿到url 哈希值
                        href = href + "*^*" + right
                    else:
                        post_data_str = ""
                    item = {'title': self.replace_biaoti(title),
                            'xq_url': href,
                            'pageurl': self.pageurl,
                            'public_time': pub_time,
                            'post_data': post_data_str
                            }

                    if self.judge_model == "formal":
                        info = (json.dumps(post_data_str), self.replace_biaoti(title), self.cid, 0, 0, pub_time, href)
                        json_list.append(info)
                    elif self.judge_model == "test":
                        json_list.append(item)
                else:
                    pass
                pd_i += 1

            return json_list
        except Exception as err:
            P = ""
            self.logger.error("栏目id:{} 正则解析模式出错 msg={}".format(self.cid,err))
            return P


    def replace_biaoti(self,html):
        title = html.replace('showTitle', '').replace('(', '').replace(')', '').replace('\\', '').replace("&amp;", '&').replace('\n', '').replace('\r', '').replace('\t', '').replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", '&').replace("&nbsp;", '').replace("&nbsp", '').replace("nbsp", '')
        return title