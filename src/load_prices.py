import pandas as pd
import gcsfs
import pymysql
import paramiko
from sshtunnel import SSHTunnelForwarder
from datetime import datetime
import sys
import os

sys.path.append(".")
from get_stock_prices import YahooFinanceHistory

mypkey = paramiko.RSAKey.from_private_key_file(r'/Users/hamishmacdonald/.ssh/qav_ssh')

sql_hostname = 'localhost'
sql_username = 'root'
sql_password = 'Jackhm01!'
sql_main_database = 'qav'
sql_port = 3306
ssh_host = '35.197.184.238'
ssh_user = 'hamish.macdonald@servian.com'
ssh_port = 22

project = 'servian-labs-gcp-brisbane'
token = r'/Users/hamishmacdonald/.config/gcloud/legacy_credentials/hamish.macdonald@servian.com/adc.json'
src_bucket = r'gs://hm_qav_to_process/'
fs = gcsfs.GCSFileSystem(project=project, token=token)
df_full = pd.DataFrame(columns=['ticker', 'Date', 'Open', 'High', 'Low', 'Close', 'Adj Close',
       'Volume'])

wanted_tickers = ['FMG.AX', 'SFC.AX', 'PRU.AX', 'STO.AX', 'BFG.AX', 'KOV.AX', 'MRC.AX', 'ECX.AX', 'CAA.AX',
                  'C6C.AX', 'GRR.AX', 'VUK.AX', 'CVL.AX', 'CBA.AX']

for t in wanted_tickers:
    print(f'Querying Yahoo Finance for historic prices for {t}.')
    df = YahooFinanceHistory(t, days_back=200).get_quote()
    from_dt = df.Date.min().strftime('%Y%m%d')
    to_dt = df.Date.max().strftime('%Y%m%d')
    fn_csv = f'{t}_{from_dt}_{to_dt}.csv'
    print(f'Writing {fn_csv} to local fs.')
    df.to_csv(fn_csv, index=False)

    print(f'Writing {fn_csv} to {src_bucket}.')
    with open(fn_csv, 'rb') as local_f:
        local_bytes = local_f.read()

    with fs.open(f'{src_bucket}{fn_csv}', 'wb') as f:
        f.write(local_bytes)

    print(f'Deleting {fn_csv} from local fs.\n')
    os.remove(fn_csv)

files = fs.ls(src_bucket)

for f in files:
    fn_src = 'gs://' + f
    fn_trg = 'gs://' + f.replace('to_process', 'processed')

    with fs.open(fn_src) as f:
        print(f'Processing file {fn_src} ...')
        df = pd.read_csv(f)
        try:
            df.Date = df.Date.apply(lambda x: datetime.strptime(x, '%d/%m/%y').strftime('%Y-%m-%d'))
        except:
            pass
        ticker = fn_src.split('/')[3].split('_')[0]
        df.insert(0,'ticker', ticker)
        df.dropna(subset=['Volume'], inplace=True)
        df_full = df_full.append(df)
        ct = len(df)
        print(f'{ct} records captured ...')
        fs.mv(fn_src, fn_trg)
        print(f'File moved to {fn_trg}.\n')

with SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_username=ssh_user,
        ssh_pkey=mypkey,
        remote_bind_address=(sql_hostname, sql_port)) as tunnel:
    conn = pymysql.connect(host=sql_hostname, user=sql_username,
            passwd=sql_password, db=sql_main_database,
            port=tunnel.local_bind_port)

    cur = conn.cursor()
    records = [tuple(x) for x in df_full.values]
    ct = len(records)
    print(f'Writing {ct} records to QAV.STOCK_PRICES_STAGE ...')
    sql = ("INSERT INTO STOCK_PRICES_STAGE (ticker, price_date, open, high, low, close, adj_close, volume) "
           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")
    cur.executemany(sql, records)
    conn.commit()
    conn.close()

    print('Processing complete')
