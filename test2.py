import configparser

config = configparser.ConfigParser()
config.read('./config')


oracle_id    = config['ORACLE_INFO']['oracle_id']
oracle_pass  = config['ORACLE_INFO']['oracle_pass']
oracle_ip    = config['ORACLE_INFO']['oracle_IP']
oracle_info1 = config['ORACLE_INFO']['oracle_info1']
oracle_info2 = config['ORACLE_INFO']['oracle_info2']


### DB 접속하기
### Before(하드코딩)
# conn = cx_Oracle.connect('cpsrndver/cpsrndver123@222.122.47.39:1521/orcl', encoding='UTF-8')


### After(config에 저장된 변수 사용)
# conn = cx_Oracle.connect(oracle_id +'/'+ oracle_pass +'@'+ oracle_ip +':'+ oracle_info1 +'/'+ oracle_info2, encoding='UTF-8')
print(oracle_id +'/'+ oracle_pass +'@'+ oracle_ip +':'+ oracle_info1 +'/'+ oracle_info2)












# import config
#
# def connect_db(dbname):
#     if dbname != config.DATABASE_CONFIG['dbname']:
#         raise ValueError("Could not find DB with given name")
#     conn = pymysql.connect(host=config.DATABASE_CONFIG['host'],
#                            user=config.DATABASE_CONFIG['user'],
#                            password=config.DATABASE_CONFIG['password'],
#                            db=config.DATABASE_CONFIG['dbname'])
#     return conn
#
# connect_db('company')
