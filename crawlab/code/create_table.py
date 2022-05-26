import pymysql


def create_tables():
    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='cuc123',
        db='FuzzPlatform',
        port=3306
    )
    cur = conn.cursor() # 获取操作游标
    cur.execute("""CREATE TABLE spiders_data(
    id_  INTEGER   PRIMARY KEY AUTO_INCREMENT,
    spider_name VARCHAR(50),
    spider_id   VARCHAR(100),
    spider_col  VARCHAR(30),
    task_id   VARCHAR(50),
    task_start_time  DATETIME,
    task_end_time   DATETIME,
    published_date   VARCHAR(100),
    firmware_name   VARCHAR(100),
    download_link   VARCHAR(300),
    url   VARCHAR(300),
    model   VARCHAR(100),
    version   VARCHAR(100),
    Vendor   VARCHAR(100),
    osname   VARCHAR(100),
    hash_id   VARCHAR(75),
    timestamp   DATETIME);""")
    conn.close()


create_tables()

