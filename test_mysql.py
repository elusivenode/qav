import pymysql
import paramiko
import pandas as pd
from sshtunnel import SSHTunnelForwarder

mypkey = paramiko.RSAKey.from_private_key_file(r'/Users/hamishmacdonald/.ssh/qav_ssh')

sql_hostname = 'localhost'
sql_username = 'root'
sql_password = 'Jackhm01!'
sql_main_database = 'qav'
sql_port = 3306
ssh_host = '35.197.184.238'
ssh_user = 'hamish.macdonald@servian.com'
ssh_port = 22

with SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_username=ssh_user,
        ssh_pkey=mypkey,
        remote_bind_address=(sql_hostname, sql_port)) as tunnel:
    conn = pymysql.connect(host=sql_hostname, user=sql_username,
            passwd=sql_password, db=sql_main_database,
            port=tunnel.local_bind_port)
    query = '''SELECT * from STOCK_PRICES;'''
    data = pd.read_sql_query(query, conn)
    conn.close()

test = 1