from sshtunnel import SSHTunnelForwarder
import pymongo
import pprint
import datetime
import hashlib
import time
import sqlite3
import copy
import pymysql


MONGO_HOST = "1.119.44.200"
MONGO_DB = "crawlab_test"
MONGO_USER = "LOGIN"
MONGO_PASS = "PASSWORD"
FILTER_SPIDER = ['netgear_en_download', 'utt-en-download', 'Edimax_download', '维盟_download', '艾泰_download', 'asus_download', '锐捷_download', 'htrdh', 'hf', 'gfd', 'CVE', 'download_test', '2']

def get_spider(db):
    spider_list = []
    mycol = db["spiders"]
    Spiders = mycol.distinct( "_id")

    # 遍历爬虫，获取需要的数据
    for sp in Spiders:
        myquery={ "_id" : sp }
        tmp_re=mycol.find(myquery)
        tmp_keyvalue={}

        for re in tmp_re:
            tmp_keyvalue={} ##遍历每条记录的字段
            tmp_keyvalue['spider_id'] = re['_id']
            tmp_keyvalue['spider_name'] = re['name']
            tmp_keyvalue['spider_col'] = re['col']
            if tmp_keyvalue['spider_name'] not in FILTER_SPIDER:
                spider_list.append(tmp_keyvalue)
    return spider_list


#去任务表查询所有任务：，检查对应状态，找出最新的任务。
def get_tasks(db,spider_list):
    mycol = db["tasks"]

    # 字典列表需要扩充
    for sp in spider_list:
        if sp['spider_col'] != None and sp['spider_col'] !="":   # 如果存在结果集
            myquery={ "spider_id" : sp['spider_id'] } 
            tmp_re=mycol.find(myquery)
            
            # 遍历查询结果，寻找最新成功结束task
            latest_time = datetime.datetime.min;
            latest_task = ["","","",""]
            for re in tmp_re:
                tmp_id = re['_id']
                tmp_status = re['status']
                tmp_start = re['start_ts']
                tmp_end = re['finish_ts']
                if tmp_status is "" or tmp_start is "" or tmp_end is  None:   # 如果数据有问题直接跳过
                    continue
                
                if tmp_start > latest_time and tmp_status == "finished":    #如果更好就更新时间
                    latest_task = [tmp_id,tmp_status,tmp_start,tmp_end]
                    latest_time = tmp_start
                    
            
            # 扩充字段
            sp['task_id'] = latest_task[0]
            sp['task_status'] = latest_task[1]
            sp['task_start_time'] = latest_task[2]
            sp['task_end_time'] = latest_task[3]
        
        else:
            sp['task_id'] = ""
            sp['task_status'] = ""
            sp['task_start_time'] = ""
            sp['task_end_time'] = ""
               
               
# 获取爬虫爬取信息
def get_data(db,spider_list):

    data = []
    for sp in spider_list:
    
        if sp['spider_col'] != None and sp['spider_col'] !="" and sp['task_id']!="":
            print("数据库名称"+sp['spider_col'])
            mycol = db[sp['spider_col']]
            myquery={ "task_id" : sp['task_id'] } 
            tmp_re=mycol.find(myquery)
                
            # 直接合并数据 暂时是字典
            for re in tmp_re:
                tmp_data = copy.deepcopy(sp)
                for key in re.keys():
                    if key not in ['_id','task_id']:
                        tmp_data[key] = re[key]
                
                data.append(tmp_data)
                #print(tmp_data)
                #print("\n")
              
                
    return data        
            
def data_processon(data):
    # 处理列名，合并相似列名字
    col = set()
    for d in data:
        tmp_list = list(d.keys())
        #print(tmp_list)
        col = col.union(set(tmp_list))
    print(col)            
                   

def get_hash_id(hash_str):
    hasht = hashlib.sha256()
    hasht.update(hash_str.encode('utf-8'))
    hash_id = hasht.hexdigest()
    return str(hash_id)
    
def alter_data(data):
    new_data = []
    for line in data:
        tmp_dict = {'spider_name':'','spider_id':'','spider_col':'','task_id':'','task_start_time':'','task_end_time':'','published_date':'','firmware_name':'','download_link':'','url':'','model':'','version':'','Vendor':'','osname':'','hash_id':''}
        tmp_str = ""
        for key in line.keys():
            if key in ['pdhashedid','file_urls','abstract','description','task_status','next_url']:
                continue
            if key in ['file_date','publish_date','published_date']:
                tmp_dict['published_date'] = line[key]
                tmp_str = tmp_str+'published_date:'+ str(line[key])+"#"
            if key in ['title','product_type_name','types','name']:
                tmp_dict['firmware_name'] = line[key]
                tmp_str = tmp_str+'firmware_name:'+ str(line[key])+"#"
            if key in ['model','real_type']:
                tmp_dict['firmware_model'] = line[key]
                tmp_str = tmp_str+'firmware_model:'+ str(line[key])+"#"

            tmp_dict[key] = line[key]
            tmp_str = tmp_str+'key:'+ str(line[key])+"#"

        # 数据类型检查
        tmp_dict['spider_id'] = str(tmp_dict['spider_id'])
        tmp_dict['task_id'] = str(tmp_dict['spider_id'])
        if type(tmp_dict['download_link']) == list:
            tmp_dict['download_link'] = tmp_dict['download_link'][0]

        # 数据长度检查
        if tmp_dict['spider_name'] != None:
            tmp_dict['spider_name'] = tmp_dict['spider_name'].strip("\n").strip()[:50].replace('"','#')
        if tmp_dict['spider_id']!= None:
            tmp_dict['spider_id'] = tmp_dict['spider_name'].strip("\n").strip()[:100].replace('"','#')
        if tmp_dict['spider_col']!= None:
            tmp_dict['spider_col'] = tmp_dict['spider_col'].strip("\n").strip()[:30].replace('"','#')
        if tmp_dict['task_id']!= None:
            tmp_dict['task_id'] = tmp_dict['task_id'].strip("\n").strip()[:50].replace('"','#')
        if tmp_dict['published_date'] != None:
            tmp_dict['published_date'] = tmp_dict['published_date'].strip("\n").strip()[:100].replace('"','#')
        if tmp_dict['firmware_name']!= None:
            tmp_dict['firmware_name'] = tmp_dict['firmware_name'].strip("\n").strip()[:100].replace('"','#')
        if tmp_dict['download_link']!= None:
            tmp_dict['download_link'] = tmp_dict['download_link'].strip("\n").strip()[:300].replace('"','#')
        if tmp_dict['url'] != None:
            tmp_dict['url'] = tmp_dict['url'].strip("\n").strip()[:300].replace('"','#')
        if tmp_dict['model']!= None:
            tmp_dict['model'] = tmp_dict['model'].strip("\n").strip()[:100].replace('"','#')
        if tmp_dict['version']!= None:
            tmp_dict['version'] = tmp_dict['version'].strip("\n").strip()[:100].replace('"','#')
        if tmp_dict['Vendor']!= None:
            tmp_dict['Vendor'] = tmp_dict['Vendor'].strip("\n").strip()[:100].replace('"','#')
        if tmp_dict['osname']!= None:
            tmp_dict['osname'] = tmp_dict['osname'].strip("\n").strip()[:100].replace('"','#')
        if tmp_dict['hash_id']!= None:
            tmp_dict['hash_id'] = tmp_dict['hash_id'].strip("\n").strip()[:75].replace('"','#')

        # 增加哈希值和时间戳
        #print(tmp_str)
        tmp_dict['hash_id'] = get_hash_id(tmp_str)
        tmp_dict['timestamp'] = time.time()
        new_data.append(tmp_dict)

    return new_data

def update_database(sqlite_db,data):
    conn = sqlite3.connect(sqlite_db)
    
    for line in data:
         cur = conn.cursor()
         cur.execute("select * from spiders_data where hash_id = '%s'" %(line['hash_id']))
         result = cur.fetchall()
         #print('result 查询结果 ',line['hash_id'],result)
         if len(result)==0:
            #print(line["task_end_time"])
            tmp_str = """INSERT INTO spiders_data(spider_name,spider_id,spider_col,task_id,task_start_time,task_end_time,published_date,firmware_name,\
                    download_link,url,model,version,Vendor,osname,hash_id,timestamp) VALUES( "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s",'%s\
                    ', "%s", "%s", "%s")""" %(line["spider_name"],line["spider_id"],line["spider_col"],line["task_id"],line["task_start_time"],line["task_end_time"],line["published_date"],line["firmware_name"],line["download_link"],line["url"], line["model"],line["version"],line["Vendor"],line["osname"],line["hash_id"],line["timestamp"] )
            #print(tmp_str)
            cur.execute(tmp_str)
    
    conn.commit()
    conn.close()
    
    
    server = SSHTunnelForwarder(
    MONGO_HOST,
    ssh_username="huang",
    ssh_password="cuc@huang",
    #ssh_pkey ="/home/cuc/.ssh/id_rsa",
    #ssh_private_key_pasisword=None,
    remote_bind_address=('127.0.0.1', 27017)
)


def update_database_mysql(data):
    # 首先连接mysql数据库
    # 检查是否已有
    # 编写插入语句
    
    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='cuc123',
        db='FuzzPlatform',
        port=3306
    )

    for line in data:
        cur = conn.cursor() # 获取操作游标
        cur.execute("select * from spiders_data where hash_id = '%s'" %(line['hash_id']))
        result = cur.fetchall()
        print('result 查询结果 ',line['hash_id'],result)
        if len(result)==0:
        #print(line["task_end_time"])
            tmp_str = """INSERT INTO spiders_data(spider_name,spider_id,spider_col,task_id,task_start_time,task_end_time,published_date,firmware_name,\
                download_link,url,model,version,Vendor,osname,hash_id,timestamp) VALUES( "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s",'%s\
                ', "%s", "%s", "%s")""" %(line["spider_name"],line["spider_id"],line["spider_col"],line["task_id"],line["task_start_time"],line["task_end_time"],line["published_date"],line["firmware_name"],line["download_link"],line["url"], line["model"],line["version"],line["Vendor"],line["osname"],line["hash_id"],line["timestamp"] )
        print(tmp_str)
        cur.execute(tmp_str)

    conn.commit()
    conn.close()
    print('finished')



def update_db():
    server = SSHTunnelForwarder(
        MONGO_HOST,
        ssh_username="huang",
        ssh_password="cuc@huang",
        #ssh_pkey ="/home/cuc/.ssh/id_rsa",
        #ssh_private_key_pasisword=None,
        remote_bind_address=('127.0.0.1', 27017)
    )

    server.start()

    client = pymongo.MongoClient('127.0.0.1', server.local_bind_port) # server.local_bind_port is assigned local port
    db = client[MONGO_DB]
    #print("打印数据表")
    #pprint.pprint(db.collection_names())

    spider_list = get_spider(db)  # 获取爬虫名
    print("打印爬虫名称")
    for i in spider_list:
        print(i['spider_name'])
    get_tasks(db,spider_list)     # 获取爬虫对应数据
    data = get_data(db,spider_list)  # 获取固件信息
    new_data = alter_data(data)      # 数据清洗

    # update_database(sqlite_db,new_data)   # 更新到指定数据库
    server.stop()
    update_database_mysql(new_data)


update_db()

