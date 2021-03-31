import  pymysql
from datetime import  datetime

def sql(id,type):
    conn = pymysql.connect(host="192.168.5.105", user="test_zhengce365", password="zhengce365%123",
                           database="test_zhengce365_collect",
                           port=3306)
    cursor = conn.cursor()
    # pd=cs_sql(id,cursor)
    # if  pd:
    #     pass
    # else:
    try:
        if type=="zc":
            sql = """
                       insert into tbl_sdpc_info(cid) values (%s)
                       """
            cursor.execute(sql, id)
            conn.commit()
            print("插入一条成功")
        elif type=="hy":
            sql = """
                                   insert into tbl_sdpc_info(cid,xq_type) values (%s,%s)
                                   """
            cursor.execute(sql, (id,1))
            conn.commit()
            print("插入一条成功")

    except Exception as err:
        print(err)
    cursor.close()
    conn.close()

# def main(id):





def cs_sql(id,cursor):
    c = datetime.now()
    timee = str(c)[:13]
    sql = "SELECT id FROM tbl_sdpc_info where cid={} and create_at like '%%{}%%'".format(id,timee)
    print(sql)
    cursor.execute(sql)
    results = cursor.fetchall()
    if results:
        return  1
    else:
        return 0
    # return  results

def main(id,type):
    for i in id:
        sql(i,type)