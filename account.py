from libraries import *

with open('passwords.json') as jsonFile:
    passwords = json.load(jsonFile)
    jsonFile.close()


# Учётка ClickHouse
clickhouse_client = Client(
    'host'
    , user=passwords['user_clickhouse']
    , password=passwords['password_clickhouse']
    , database='db'
)


# Учётка Vertica БОЕВАЯ
vertica_conn_info = {'DRIVER': 'Vertica',
                     'host': 'host123',
                     'port': 5433, 
                     'database': 'db',
                     'user': passwords['user_vertica'],
                     'password': passwords['password_vertica'],
                     'read_timeout': 3000, 
                     'unicode_error': 'strict', 
                     'ssl': False}
