import pypyodbc
import time

print(pypyodbc.dataSources())

print("正在连接数据库...")
conn=pypyodbc.connect("DRIVER={NetezzaSQL};SERVER='hqdellnz03';PORT='5480';DATABASE='smb_prod>';UID='jachen';PWD='Nowornever@357';")

print('conn')




