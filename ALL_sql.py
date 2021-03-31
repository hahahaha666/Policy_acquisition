#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：公司政策爬虫 
@File    ：ALL_sql.py
@IDE     ：PyCharm 
@Author  ：13192272582
@Date    ：2021/1/21 16:18 
'''
import json
from utils import log

logger = log.write_log(work_name='work_info')


def dispose_dict(info_item):
    """
    处理详情页采集的字典
    :param item:
    :return:
    """
    content_tag = info_item['content_tag']
    title_type = info_item['title_type']
    host_name = info_item['host_name']
    xpath_xq = info_item['xpath_xq']
    l_type = info_item['type']
    cid = info_item['id']
    cookie = info_item['cookie']
    host = info_item['host']
    immit_js = info_item['immit_js']
    judge_time = info_item['judge_time']
    accessory_xpath = info_item['accessory_xpath']
    accessory_re = info_item['accessory_re']
    accessory_judge = info_item['accessory_judge']
    detail_tran = info_item['detail_tran']
    detail_type = info_item['detail_type']
    title_details = info_item['title_details']
    re_time = info_item['re_time']
    xpath_time = info_item['xpath_time']
    json_xq = info_item['json_xq']
    res_headers=info_item['res_headers']
    if json_xq == None or not json_xq:
        json_xq = ""
    if host == None or not host:
        host = "fujiancunfang"
    if l_type == None or not l_type:
        l_type = ''
    if xpath_xq == None or not xpath_xq:
        xpath_xq = ''
    if not content_tag or content_tag == None:
        content_tag = ''
    if not host_name or host_name == None:
        host_name = '本网站'
    if not cookie or cookie == None:
        cookie = ''
    if immit_js == None or not immit_js:
        immit_js = 0
    if accessory_xpath == None or not accessory_xpath:
        accessory_xpath = ""
    if accessory_re == None or not accessory_re:
        accessory_re = ""
    if accessory_judge == None or not accessory_judge:
        accessory_judge = 0
    if detail_tran == None or not detail_tran:
        detail_tran = 0
    if detail_type == None or not detail_type:
        detail_type = 0
    if title_details == None or not title_details:
        title_details = ""
    if re_time == None or not re_time:
        re_time = ""
    if xpath_time == None or not xpath_time:
        xpath_time = ""
    if res_headers==None or not res_headers:
        res_headers=""
    argument = {
        "content_tag": content_tag,
        "title_type": title_type,
        "host_name": host_name,
        "xpath_xq": xpath_xq,
        "l_type": l_type,
        "cookie": cookie,
        "host": host,
        "judge_model": "formal",
        'immit_js': immit_js,
        'judge_time': judge_time,
        'accessory_xpath': accessory_xpath,
        'accessory_re': accessory_re,
        'accessory_judge': int(accessory_judge),
        'detail_tran': int(detail_tran),
        'detail_type': int(detail_type),
        'title_details': title_details,
        'xpath_time': xpath_time,
        're_time': re_time,
        'json_xq': json_xq,
        'res_headers':res_headers
    }
    return argument


def HY_SQL(cursor, id):
    """
    行业采集测试sql
    :param cursor:
    :param id:
    :return:
    """
    if id == None:
        sql = """select id,url,include,not_include,title_tag,url_real,title_re,xq_url_light,xq_url_right,form_data,info_page,transcode,r_replace,
                               l_replace,del_str,xpath_list,collect_type,cookie,judge_time,re_time,xpath_time,url_two,host_name,name,immit_js,detail_type,detail_data   from tbl_industry_info  where id>0 and   eveday_status=0 """

    else:
        sql = """select id,url,include,not_include,title_tag,url_real,title_re,xq_url_light,xq_url_right,form_data,info_page,transcode,r_replace,
                               l_replace,del_str,xpath_list,collect_type,cookie,judge_time,re_time,xpath_time,url_two,host_name,name,immit_js,detail_type,detail_data   from tbl_industry_info  where id=%s""" % id
    cursor.execute(sql)
    resultss = cursor.fetchall()
    return resultss


def ED_SQL(cursor, id):
    """
    列表页查询所需规则搭配
    :param cursor:
    :param id:
    :return:
    """
    if id == None:
        sql_1 = "select * from tbl_collect_info where immit_js=1 and id>0"
        cursor.execute(sql_1)
        sql_s_1 = cursor.fetchall()
        limit_ids = []
        for q in sql_s_1:
            limit_id = q['id']
            limit_ids.append(limit_id)
        sql = "select id,url,include,not_include,title_tag,url_real,title_re,xq_url_light,xq_url_right,form_data,info_page,transcode,r_replace,l_replace,del_str,xpath_list,collect_type,cookie,url_two,id,host_name,name,judge_time,re_time,xpath_time,immit_js,detail_type,detail_data,res_headers   from tbl_collect_info  where id>0 and eveday_status=0 and type !=3   ORDER BY RAND() limit 5000 "  #
        # sql = "select id,url,include,not_include,title_tag,url_real,title_re,xq_url_light,xq_url_right,form_data,info_page,transcode,r_replace,l_replace,del_str,xpath_list,collect_type,cookie,url_two,id,host_name,name,judge_time,re_time,xpath_time,immit_js,detail_type,detail_data   from tbl_collect_info  where id>0 and eveday_status=0 and type !=3   and id not in ({}) and id>0 ORDER BY RAND() limit 150".format(",".join(str(i) for i in limit_ids))
        # sql = "select id,url,include,not_include,title_tag,url_real,title_re,xq_url_light,xq_url_right,form_data,info_page,transcode,r_replace,l_replace,del_str,xpath_list,collect_type,cookie,url_two,id,host_name,name,judge_time,re_time,xpath_time,immit_js,detail_type,detail_data   from tbl_collect_info  where  id=14792 "
    else:
        sql = "select id,url,include,not_include,title_tag,url_real,title_re,xq_url_light,xq_url_right,form_data,info_page,transcode,r_replace,l_replace,del_str,xpath_list,collect_type,cookie,url_two,host_name,name,judge_time,re_time,xpath_time,immit_js,detail_type,detail_data,res_headers   from tbl_collect_info  where id=%s" % id
    try:
        cursor.execute(sql)
        resultss = cursor.fetchall()
    except Exception as err:
        print(err)
        pass
    return resultss


def argument_get(resultss):
    """
    处理栏目页的字典
    :param resultss:
    :return:
    """
    url = resultss["url"]
    include = resultss['include']
    not_include = resultss['not_include']  ##不包含
    ####栏目页解析模式C
    title_tag = resultss['title_tag']
    if title_tag == None:
        title_tag = ''

    url_real = resultss['url_real']
    if url_real == None or not url_real:
        url_real = ''
    ####下面三个代表一种解析方式 A
    title_re = resultss['title_re']
    if title_re == None:
        title_re = ''
    xq_url_light = resultss['xq_url_light']
    if xq_url_light == None:
        xq_url_light = ''
    xq_url_right = resultss['xq_url_right']
    if xq_url_right == None:
        xq_url_right = ''

    form_data = resultss['form_data']
    if form_data == None or not form_data:
        form_data = None
    info_page = resultss['info_page']  ###页数
    transcode = resultss['transcode']
    r_replace = resultss['r_replace']
    if r_replace == None:
        r_replace = ''

    l_replace = resultss['l_replace']
    if l_replace == None:
        l_replace = ''

    def_src = resultss['del_str']
    ###代表栏目解析模式B
    xpath_list = resultss['xpath_list']
    if xpath_list == None:
        xpath_list = ''

    collect_type = resultss['collect_type']
    ###网页是否需要cookie

    cookie = resultss['cookie']
    url_two = resultss['url_two']
    if url_two == None or not url_two:
        url_two = ''
    cid = resultss['id']
    host_name = resultss['host_name']
    name = resultss['name']
    judge_time = resultss['judge_time']
    re_time = resultss['re_time']
    xpath_time = resultss['xpath_time']
    immit_js = resultss['immit_js']
    res_headers = resultss['res_headers']
    detail_type = resultss['detail_type']
    if res_headers == None or not res_headers:
        res_headers = headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36',
        }
    else:
        try:
            res_headers = json.loads(res_headers)
        except Exception as err:
            logger.error("请求头处理失败")

    if detail_type == None or not detail_type:
        detail_type = 0

    detail_data = resultss['detail_data']

    if detail_data == None or not detail_data:
        detail_data = ""

    if cookie == None:
        cookie = ''

    if include == None:
        include = ''

    if not_include == None:
        not_include = ''

    if def_src == None:
        def_src = ''
    if not immit_js or immit_js == None:
        immit_js = 0

    r_replace, l_replace = replace_r_l(r_replace, l_replace)
    argument = {
        'url': url,
        'include': include,
        'not_include': not_include,
        'title_tag': title_tag,
        'url_real': url_real,
        'title_re': title_re,
        'xq_url_light': xq_url_light,
        'xq_url_right': xq_url_right,
        'form_data': form_data,
        'info_page': info_page,
        'transcode': transcode,
        'r_replace': r_replace,
        'l_replace': l_replace,
        'def_src': def_src,
        'xpath_list': xpath_list,
        'collect_type': collect_type,
        'cookie': cookie,
        'url_two': url_two,
        'cid': cid,
        'host_name': host_name,
        'name': name,
        'judge_time': judge_time,
        're_time': re_time,
        'xpath_time': xpath_time,
        'immit_js': immit_js,
        'judge_model': "formal",
        'detail_type': detail_type,
        'detail_data': detail_data,
        'res_headers': res_headers
    }
    return argument


def replace_r_l(r_replace, l_replace):
    """
    标题替换
    :param r_replace: 需要被替换的字符
    :param l_replace: 替换成什么字符
    :return:
    """
    if r_replace:
        if not l_replace:
            l_replace = ''
        r_replace = r_replace.split(';')
        l_replace = l_replace.split(';')

        if len(r_replace) > len(l_replace):
            k = len(r_replace) - len(l_replace)
            for i in range(k):
                l_replace.append('')
    else:
        r_replace = ''
        l_replace = ''
    return r_replace, l_replace


def SQLhy_not_js(conn, cursor):
    try:
        sql = "select  id,cid,title,url,release_at,post_data from tbl_industry_details   where id>0 and status=0"
        cursor.execute(sql)
        results = cursor.fetchall()
    except:
        results = None
    return results


def SQL_not_js(conn, cursor):
    """

    :param conn:
    :param cursor:
    :return: 不需要加载js采集的数据
    """
    try:
        sql_1 = "select * from tbl_collect_info where immit_js=1 and id>0"
        cursor.execute(sql_1)
        sql_s_1 = cursor.fetchall()
        limit_ids = []
        for q in sql_s_1:
            limit_id = q['id']
            limit_ids.append(limit_id)
        sql = "select  cid,title,url,release_at,post_data,id from tbl_article_info_temp   where  status=0 and cid not in ({}) and id>0 ORDER BY RAND() limit 2000".format(
            ",".join(str(i) for i in limit_ids))
        # sql="select  id,cid,title,url,release_at,post_data from tbl_article_info_temp   where id=36242690"
        cursor.execute(sql)
        results = cursor.fetchall()
    except Exception as err:
        print(err)
        results = None
    return results


def SQL_ALL(cursor):
    try:
        sql = "select  id,cid,title,url,release_at,post_data from tbl_article_info_temp   where  status=0  ORDER BY RAND() limit 2000"
        # sql="select  id,cid,title,url,release_at,post_data from tbl_article_info_temp   where  url='http://sthjt.shaanxi.gov.cn/dynamic/city/2021-01-25/66727.html'"
        cursor.execute(sql)
        results = cursor.fetchall()
        return results
    except:
        return None


def SQL_XW(conn, cursor):
    """

    :param conn:
    :param cursor:
    :return: 返回需要采集封面图片的新闻id
    """
    try:
        ###需要采集封面图片 单独拿出来
        sql_i = "select id from tbl_collect_info where type=4"
        cursor.execute(sql_i)
        all_id = cursor.fetchall()
    except:
        all_id = ""
    return all_id


def SQL_disposition(conn, cursor):
    try:
        sql_o = "select content_tag, title_type, host_name,id,xpath_xq,type,cookie,host,immit_js,judge_time,accessory_xpath,accessory_re,accessory_judge,detail_tran,detail_type,title_details,re_time,xpath_time,json_xq       from tbl_collect_info  where id>1"
        cursor.execute(sql_o)
        resultss = cursor.fetchall()
    except:
        resultss = ""
    all_dis = {}
    for i in resultss:
        id = i['id']
        all_dis[id] = i
    return all_dis


def SQLhy_disposition(conn, cursor):
    try:
        sql_o = "select content_tag, title_type, host_name,id,xpath_xq,type,cookie,host,immit_js,judge_time,accessory_xpath,accessory_re,accessory_judge,detail_tran,detail_type,title_details,re_time,xpath_time,json_xq       from tbl_industry_info  where id>0"
        cursor.execute(sql_o)
        resultss = cursor.fetchall()
    except:
        resultss = ""
    all_dis = {}
    for i in resultss:
        id = i['id']
        all_dis[id] = i
    return all_dis


def delete_sql(conn, cursor):
    try:
        sql = "delete from tbl_article_info_temp where status=3 "
        cursor.execute(sql)
        conn.commit()
        sql_1 = 'delete from tbl_article_info_temp where url like "%mp.weixin.qq.com%"  '
        cursor.execute(sql_1)
        conn.commit()
        sql_2 = 'delete from tbl_article_info_temp where url not like"%http%"  '
        cursor.execute(sql_2)
        conn.commit()
        print("删除无效数据完成")
    except:
        pass
