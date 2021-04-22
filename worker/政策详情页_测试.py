import asyncio

import  pymysql
from DBUtils.PooledDB import PooledDB
from  aiohttps_GP import  *
from  Detailpage_process import  *
from  Ray_num import  *
from  ALL_sql import  *
from utils.PoolDB import POOL_DB
from worker.政策栏目页_测试 import ZC_getcolumn
import nest_asyncio
nest_asyncio.apply()

logger = log.write_log(work_name='政策详情页测试')

class work():
    def __init__(self):
        pass

    async def work_b(self,id,url,c,post_data):
        c_all = self.select_sql(id,c)
        argument = dispose_dict(c_all[0])
        if int(argument['immit_js']) == 1:
            html = await  Ray_html(url, argument['res_headers'],logger)
        else:
            if int(argument['detail_type']) == 0: ##详情页是get：
                html=await  ZC_getcolumn().AIO_GET(argument,url,logger)
            else:
                ###详情页是post
                html=await  ZC_getcolumn().AIO_POST(argument,post_data,url,logger)
        ###网页源码是否需要转吗
        if int(argument['detail_tran']) == 1:
            html=html.encode('utf-8').decode('unicode_escape', 'ignore')
        ###判断网页是否是json数据
        if r"\u003c" in html:
            html = str(json.loads(html))
        argument['html']=html
        argument['cid']=id
        argument['pub_time']=None
        argument['judge_model']="test"
        content_tag = argument['content_tag']
        argument['url'] = url
        xpath_xq = argument['xpath_xq']
        json_xq=['json_xq']
        print(html)
        if content_tag:  ####正文标签
            content_tag_start = content_tag.split(',')[0].strip()
            content_tag_end = content_tag.split(',')[1].strip()
        else:
            content_tag_start = ""
            content_tag_end = ""
        argument['content_tag'] = content_tag_start
        argument['content_tag_end'] = content_tag_end
        if content_tag:
            item,wy=DPP_contentr_e(logger,**argument)
        elif xpath_xq:
            item,wy=DPP_content_xpath(logger,**argument)
        elif json_xq:
            item,wy = DPP_content_json(logger,**argument)
        #####测试不加入自动获取文本 以防不打标签
        if item!=None:
            all = []
            all.append(item)
            all_info = {'info': all}
            ck = json.dumps(all_info,ensure_ascii=False)
            print(ck.replace(r'\"',"'").replace(r'\r','').replace(r'\n','').replace(r'\t',''))
            now_time = datetime.now()
            m = hashlib.md5()
            pp = str(url) + str(now_time)
            m.update(str(pp).encode('utf-8'))
            md = m.hexdigest()  ####把url转码md5
            print(md)
            self.sql_insert(md, ck)
        else:
            md=''
        return md



    def sql_insert(self,md,ck):
        try:
            conn, cursor = POOL_DB().create_conn()
            sql = "insert into tbl_collect_temp(md5,data) values (%s,%s)"
            cursor.execute(sql, (md, ck))
            conn.commit()
            POOL_DB().close_conn(conn, cursor)
        except Exception as  err:
            print(err)
            pass



    def select_sql(self,id,c):
        if c=="zc":
            sql = "select content_tag, title_type, host_name,id,xpath_xq,type,cookie,host,immit_js,judge_time,accessory_xpath,accessory_re,accessory_judge,detail_tran,detail_type,title_details,re_time,xpath_time,json_xq,res_headers       from tbl_collect_info  where id>=%s"%id
        elif c=="hy":
            sql = "select content_tag, title_type, host_name,id,xpath_xq,type,cookie,host,immit_js,judge_time,accessory_xpath,accessory_re,accessory_judge,detail_tran,detail_type,title_details,re_time,xpath_time,json_xq ,res_headers     from tbl_industry_info where id=%s " % id
        conn, cursor =POOL_DB(). create_conn()
        cursor.execute(sql )
        results = cursor.fetchall()
        POOL_DB().close_conn(conn, cursor)
        return results




def main_i(a,b,c,post_data):
    start=work()
    loop = asyncio.get_event_loop()
    get_future = asyncio.ensure_future(start.work_b(a,b,c,post_data))  # 相当于开启一个future
    loop.run_until_complete(get_future)  # 事件循环
    return  get_future.result()

